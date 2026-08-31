from __future__ import annotations

import math
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Callable
from uuid import UUID

from euv_acquisition.models import SnapshotCloseReason
from euv_acquisition.session import (
    AcceptedPulse,
    CaptureEngine,
    CapturePurpose,
    CaptureUpdate,
    SnapshotBatch,
)
from euv_acquisition.snapshot import SnapshotManifest


@dataclass(frozen=True)
class PipelineConfig:
    capture_queue_capacity: int = 32
    persistence_queue_capacity: int = 8
    capture_poll_seconds: float = 0.001
    drain_timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        for name in ("capture_queue_capacity", "persistence_queue_capacity"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer.")
        for name in ("capture_poll_seconds", "drain_timeout_seconds"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value <= 0
            ):
                raise ValueError(f"{name} must be a finite positive number.")


@dataclass(frozen=True)
class _AcceptedWork:
    accepted: AcceptedPulse
    enqueued_at_monotonic_ns: int


@dataclass
class _FenceCompletion:
    event: threading.Event = field(default_factory=threading.Event)
    manifest: SnapshotManifest | None = None
    error: Exception | None = None


@dataclass(frozen=True)
class _FlushFence:
    completion: _FenceCompletion


@dataclass(frozen=True)
class _TerminalFence:
    reason: str
    terminal_error: str | None


@dataclass(frozen=True)
class _PersistenceBatch:
    batch: SnapshotBatch
    enqueued_at_monotonic_ns: int
    completion: _FenceCompletion | None = None


@dataclass(frozen=True)
class _PersistenceFence:
    completion: _FenceCompletion


@dataclass(frozen=True)
class _PersistenceTerminal:
    reason: str
    terminal_error: str | None


_RawWork = _AcceptedWork | _FlushFence | _TerminalFence
_PersistenceWork = _PersistenceBatch | _PersistenceFence | _PersistenceTerminal


class CapturePipeline:
    def __init__(
        self,
        engine: CaptureEngine,
        config: PipelineConfig = PipelineConfig(),
        *,
        emit_update: Callable[[CaptureUpdate], None] | None = None,
        monotonic_time_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        self.engine = engine
        self.config = config
        self._emit_update = emit_update or (lambda _update: None)
        self._monotonic_time_ns = monotonic_time_ns
        self._state_lock = threading.RLock()
        self._stop_requested = threading.Event()
        self._source_ready = threading.Event()
        self._completed = threading.Event()
        self._flush_requests: queue.SimpleQueue[_FlushFence] = queue.SimpleQueue()
        self._capture_queue: queue.Queue[_RawWork] = queue.Queue(maxsize=config.capture_queue_capacity)
        self._persistence_queue: queue.Queue[_PersistenceWork] = queue.Queue(
            maxsize=config.persistence_queue_capacity
        )
        self._threads: tuple[threading.Thread, ...] = ()
        self._terminal_reason: str | None = None
        self._terminal_error: str | None = None
        self._source_open_error: Exception | None = None
        self._shutdown = False

    @property
    def active(self) -> bool:
        return self.engine.active

    @property
    def stopping(self) -> bool:
        return self._stop_requested.is_set() and self.engine.active

    def start_capture(
        self,
        session_id: UUID | None = None,
        purpose: CapturePurpose = CapturePurpose.EXPERIMENT,
    ) -> UUID:
        with self._state_lock:
            if self._shutdown:
                raise RuntimeError("Capture pipeline is shut down.")
            if self.engine.active:
                raise RuntimeError("Capture is already active.")
            if any(thread.is_alive() for thread in self._threads):
                raise RuntimeError("Previous capture pipeline workers are still running.")
            selected_id = self.engine.begin_session(session_id, purpose)
            self._reset_session_state()
            self.engine.metrics.observe_queue(
                "capture",
                depth=0,
                capacity=self.config.capture_queue_capacity,
            )
            self.engine.metrics.observe_queue(
                "persistence",
                depth=0,
                capacity=self.config.persistence_queue_capacity,
            )
            self._threads = (
                threading.Thread(target=self._run_persistence, name="euv-persistence", daemon=True),
                threading.Thread(target=self._run_analysis, name="euv-analysis", daemon=True),
                threading.Thread(target=self._run_producer, name="euv-capture-producer", daemon=True),
            )
            for thread in self._threads:
                thread.start()

        if not self._source_ready.wait(self.config.drain_timeout_seconds):
            reason = "Pulse source did not open before the pipeline start deadline."
            self.request_abort(reason)
            self._wait_for_completion()
            raise TimeoutError(reason)
        if self._source_open_error is not None:
            self._wait_for_completion()
            error = self._source_open_error
            raise RuntimeError(f"Pulse source failed to open: {type(error).__name__}: {error}") from error
        return selected_id

    def flush(self) -> SnapshotManifest | None:
        with self._state_lock:
            if not self.engine.active:
                raise RuntimeError("Capture is not active.")
            if self._stop_requested.is_set():
                raise RuntimeError("Capture is already stopping.")
            completion = _FenceCompletion()
            self._flush_requests.put(_FlushFence(completion))
        self._wait_for_fence(completion)
        return completion.manifest

    def stop_capture(self, reason: str = "Capture stop requested.") -> None:
        if not reason.strip():
            raise ValueError("Capture stop reason cannot be empty.")
        with self._state_lock:
            if not self.engine.active:
                raise RuntimeError("Capture is not active.")
        self._request_terminal(reason, terminal_error=None)
        self._wait_for_completion()

    def abort(self, reason: str) -> None:
        if not reason.strip():
            raise ValueError("Capture abort reason cannot be empty.")
        with self._state_lock:
            if not self.engine.active:
                return
        self._request_terminal(reason, terminal_error=reason)
        self._wait_for_completion()

    def request_abort(self, reason: str) -> bool:
        if not reason.strip():
            raise ValueError("Capture abort reason cannot be empty.")
        with self._state_lock:
            if not self.engine.active:
                return False
        self._request_terminal(reason, terminal_error=reason)
        return True

    def shutdown(self, reason: str = "Acquisition server is shutting down.") -> None:
        with self._state_lock:
            self._shutdown = True
            active = self.engine.active
        if active:
            self.abort(reason)
        self._join_workers()

    def _reset_session_state(self) -> None:
        self._stop_requested.clear()
        self._source_ready.clear()
        self._completed.clear()
        self._flush_requests = queue.SimpleQueue()
        self._capture_queue = queue.Queue(maxsize=self.config.capture_queue_capacity)
        self._persistence_queue = queue.Queue(maxsize=self.config.persistence_queue_capacity)
        self._terminal_reason = None
        self._terminal_error = None
        self._source_open_error = None

    def _run_producer(self) -> None:
        source_open = False
        try:
            try:
                self.engine.source.open()
                source_open = True
                self.engine.refresh_capture_mode()
            except Exception as exc:
                self._source_open_error = exc
                self._request_terminal(
                    f"Pulse source failed to open: {type(exc).__name__}: {exc}",
                    terminal_error=f"Pulse source failed to open: {type(exc).__name__}: {exc}",
                )
            finally:
                self._source_ready.set()

            while source_open and not self._stop_requested.is_set():
                self._publish_flush_requests(source_open=True)
                if self._stop_requested.is_set():
                    break
                if self._capture_queue.qsize() >= self.config.capture_queue_capacity:
                    reason = (
                        "Capture queue reached its capacity of "
                        f"{self.config.capture_queue_capacity} accepted pulse(s)."
                    )
                    self.engine.metrics.increment("capture_queue_overflow")
                    self._request_terminal(reason, terminal_error=reason)
                    break
                try:
                    pulse = self.engine.source.capture()
                except Exception as exc:
                    reason = f"Pulse source failure: {type(exc).__name__}: {exc}"
                    self._request_terminal(reason, terminal_error=reason)
                    break
                if pulse is None:
                    self._stop_requested.wait(self.config.capture_poll_seconds)
                    continue
                if not self._accept_captured_pulse(pulse, block=False):
                    break
        finally:
            self._publish_flush_requests(source_open=source_open)
            if source_open:
                try:
                    self.engine.source.close()
                except Exception as exc:
                    reason = f"Pulse source close failure: {type(exc).__name__}: {exc}"
                    self._request_terminal(reason, terminal_error=reason)
                drain_captured = getattr(self.engine.source, "drain_captured", None)
                if drain_captured is not None:
                    try:
                        for pulse in drain_captured():
                            if not self._accept_captured_pulse(pulse, block=True):
                                break
                    except Exception as exc:
                        reason = f"Pulse source drain failure: {type(exc).__name__}: {exc}"
                        self._request_terminal(reason, terminal_error=reason)
            reason, terminal_error = self._terminal_values()
            self._capture_queue.put(_TerminalFence(reason, terminal_error))
            self._observe_capture_queue()

    def _run_analysis(self) -> None:
        analysis_error: Exception | None = None
        while True:
            try:
                work = self._capture_queue.get(timeout=self.config.capture_poll_seconds)
            except queue.Empty:
                if analysis_error is None:
                    try:
                        batch = self.engine.take_due_batch()
                        if batch is not None:
                            self._queue_persistence_batch(batch)
                    except Exception as exc:
                        analysis_error = exc
                        self.engine.metrics.increment("analysis_failures")
                        reason = f"Analysis stage failure: {type(exc).__name__}: {exc}"
                        self._request_terminal(reason, terminal_error=reason)
                continue

            self._observe_capture_queue()
            try:
                if isinstance(work, _AcceptedWork):
                    if analysis_error is not None:
                        self.engine.metrics.increment("unprocessed_after_analysis_failure")
                        continue
                    self.engine.metrics.record_duration(
                        "capture_queue_wait",
                        self._monotonic_time_ns() - work.enqueued_at_monotonic_ns,
                    )
                    try:
                        processed = self.engine.process_accepted(work.accepted)
                        self._emit_update(CaptureUpdate(report=processed.report))
                        for batch in processed.closed_batches:
                            self._queue_persistence_batch(batch)
                        if processed.stop_reason is not None:
                            self._request_terminal(
                                processed.stop_reason,
                                terminal_error=processed.stop_reason,
                            )
                    except Exception as exc:
                        analysis_error = exc
                        self.engine.metrics.increment("analysis_failures")
                        self.engine.metrics.increment("unprocessed_after_analysis_failure")
                        reason = f"Analysis stage failure: {type(exc).__name__}: {exc}"
                        self._request_terminal(reason, terminal_error=reason)
                elif isinstance(work, _FlushFence):
                    if analysis_error is None:
                        try:
                            batch = self.engine.take_pending_batch(SnapshotCloseReason.EXPLICIT_FLUSH)
                            if batch is not None:
                                self._queue_persistence_batch(batch, completion=work.completion)
                            else:
                                self._queue_persistence_fence(work.completion)
                        except Exception as exc:
                            work.completion.error = exc
                            self._queue_persistence_fence(work.completion)
                    else:
                        work.completion.error = analysis_error
                        self._queue_persistence_fence(work.completion)
                else:
                    terminal_error = work.terminal_error
                    reason = work.reason
                    if analysis_error is not None:
                        reason = f"Analysis stage failure: {type(analysis_error).__name__}: {analysis_error}"
                        terminal_error = reason
                    close_reason = (
                        SnapshotCloseReason.CAPTURE_STOP
                        if terminal_error is None and analysis_error is None
                        else SnapshotCloseReason.ACQUISITION_ERROR
                    )
                    batch = self.engine.take_pending_batch(close_reason)
                    if batch is not None:
                        self._queue_persistence_batch(batch)
                    self._queue_persistence_terminal(reason, terminal_error)
                    return
            finally:
                self._capture_queue.task_done()

    def _run_persistence(self) -> None:
        persistence_error: Exception | None = None
        while True:
            work = self._persistence_queue.get()
            self._observe_persistence_queue()
            try:
                if isinstance(work, _PersistenceBatch):
                    if persistence_error is not None:
                        self.engine.metrics.increment("unpersisted_after_storage_failure", len(work.batch.records))
                        if work.completion is not None:
                            work.completion.error = persistence_error
                            work.completion.event.set()
                        continue
                    self.engine.metrics.record_duration(
                        "persistence_queue_wait",
                        self._monotonic_time_ns() - work.enqueued_at_monotonic_ns,
                    )
                    try:
                        manifest = self.engine.persist_batch(work.batch)
                        self._emit_update(CaptureUpdate(closed_snapshots=(manifest,)))
                        if work.completion is not None:
                            work.completion.manifest = manifest
                            work.completion.event.set()
                    except Exception as exc:
                        persistence_error = exc
                        self.engine.metrics.increment("persistence_failures")
                        self.engine.metrics.increment(
                            "unpersisted_after_storage_failure",
                            len(work.batch.records),
                        )
                        if work.completion is not None:
                            work.completion.error = exc
                            work.completion.event.set()
                        reason = f"Persistence stage failure: {type(exc).__name__}: {exc}"
                        self._request_terminal(reason, terminal_error=reason)
                elif isinstance(work, _PersistenceFence):
                    if work.completion.error is None and persistence_error is not None:
                        work.completion.error = persistence_error
                    work.completion.event.set()
                else:
                    reason = work.reason
                    terminal_error = work.terminal_error
                    if persistence_error is not None:
                        reason = f"Persistence stage failure: {type(persistence_error).__name__}: {persistence_error}"
                        terminal_error = reason
                    try:
                        self.engine.finalize_session(reason, terminal_error=terminal_error)
                        self._emit_update(CaptureUpdate(stop_reason=reason))
                    finally:
                        self._completed.set()
                    return
            finally:
                self._persistence_queue.task_done()

    def _publish_flush_requests(self, *, source_open: bool) -> None:
        while True:
            try:
                request = self._flush_requests.get_nowait()
            except queue.Empty:
                return
            capture_fence = getattr(self.engine.source, "capture_fence", None)
            if source_open and capture_fence is not None:
                try:
                    for pulse in capture_fence():
                        if not self._accept_captured_pulse(pulse, block=True):
                            break
                except Exception as exc:
                    request.completion.error = exc
                    drain_captured = getattr(self.engine.source, "drain_captured", None)
                    if drain_captured is not None:
                        try:
                            for pulse in drain_captured():
                                if not self._accept_captured_pulse(pulse, block=True):
                                    break
                        except Exception as drain_exc:
                            request.completion.error = drain_exc
                    reason = f"Pulse source flush failure: {type(exc).__name__}: {exc}"
                    self._request_terminal(reason, terminal_error=reason)
            self._capture_queue.put(request)
            self._observe_capture_queue()

    def _accept_captured_pulse(self, pulse, *, block: bool) -> bool:
        try:
            accepted = self.engine.accept_pulse(pulse)
        except Exception as exc:
            reason = f"Pulse acceptance failure: {type(exc).__name__}: {exc}"
            self._request_terminal(reason, terminal_error=reason)
            return False
        work = _AcceptedWork(accepted, self._monotonic_time_ns())
        if block:
            self._capture_queue.put(work)
        else:
            self._capture_queue.put_nowait(work)
        self._observe_capture_queue()
        return True

    def _queue_persistence_batch(
        self,
        batch: SnapshotBatch,
        *,
        completion: _FenceCompletion | None = None,
    ) -> None:
        if self._persistence_queue.qsize() >= self.config.persistence_queue_capacity:
            reason = (
                "Persistence queue reached its capacity of "
                f"{self.config.persistence_queue_capacity} snapshot batch(es)."
            )
            self.engine.metrics.increment("persistence_queue_overflow")
            self._request_terminal(reason, terminal_error=reason)
        self._persistence_queue.put(
            _PersistenceBatch(batch, self._monotonic_time_ns(), completion)
        )
        self._observe_persistence_queue()

    def _queue_persistence_fence(self, completion: _FenceCompletion) -> None:
        self._persistence_queue.put(_PersistenceFence(completion))
        self._observe_persistence_queue()

    def _queue_persistence_terminal(self, reason: str, terminal_error: str | None) -> None:
        self._persistence_queue.put(_PersistenceTerminal(reason, terminal_error))
        self._observe_persistence_queue()

    def _request_terminal(self, reason: str, *, terminal_error: str | None) -> None:
        with self._state_lock:
            if self._terminal_reason is None or (
                self._terminal_error is None and terminal_error is not None
            ):
                self._terminal_reason = reason
                self._terminal_error = terminal_error
            self._stop_requested.set()

    def _terminal_values(self) -> tuple[str, str | None]:
        with self._state_lock:
            return (
                self._terminal_reason or "Capture pipeline stopped.",
                self._terminal_error,
            )

    def _wait_for_fence(self, completion: _FenceCompletion) -> None:
        if not completion.event.wait(self.config.drain_timeout_seconds):
            reason = "Capture pipeline flush exceeded its drain deadline."
            self.request_abort(reason)
            raise TimeoutError(reason)
        if completion.error is not None:
            raise RuntimeError(
                f"Capture pipeline flush failed: {type(completion.error).__name__}: {completion.error}"
            ) from completion.error

    def _wait_for_completion(self) -> None:
        if not self._completed.wait(self.config.drain_timeout_seconds):
            raise TimeoutError(
                f"Capture pipeline did not drain within {self.config.drain_timeout_seconds:g} second(s)."
            )
        self._join_workers()

    def _join_workers(self) -> None:
        current = threading.current_thread()
        for thread in self._threads:
            if thread is not current:
                thread.join(timeout=0.5)

    def _observe_capture_queue(self) -> None:
        self.engine.metrics.observe_queue(
            "capture",
            depth=self._capture_queue.qsize(),
            capacity=self.config.capture_queue_capacity,
        )

    def _observe_persistence_queue(self) -> None:
        self.engine.metrics.observe_queue(
            "persistence",
            depth=self._persistence_queue.qsize(),
            capacity=self.config.persistence_queue_capacity,
        )
