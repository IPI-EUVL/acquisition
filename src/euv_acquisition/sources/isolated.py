from __future__ import annotations

import math
import multiprocessing
import os
import queue
import signal
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import numpy as np

from euv_acquisition.models import (
    CaptureConfig,
    CapturedPulse,
    NativePulseAnalysis,
    SourceBatchEnvelope,
    SourceCaptureBatch,
)
from euv_acquisition.pipeline_metrics import PipelineMetrics
from euv_acquisition.sources.base import PulseSource


@dataclass(frozen=True)
class CaptureProcessConfig:
    cpu: int | None = 1
    realtime_priority: int | None = 20
    poll_seconds: float = 0.001
    queue_capacity: int = 32
    queue_timeout_seconds: float = 0.25
    startup_timeout_seconds: float = 10.0
    shutdown_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        if self.cpu is not None and (
            isinstance(self.cpu, bool) or not isinstance(self.cpu, int) or self.cpu < 0
        ):
            raise ValueError("Capture-process CPU must be a non-negative integer or null.")
        if self.realtime_priority is not None and (
            isinstance(self.realtime_priority, bool)
            or not isinstance(self.realtime_priority, int)
            or not 1 <= self.realtime_priority <= 99
        ):
            raise ValueError("Capture-process realtime priority must be between 1 and 99 or null.")
        if (
            isinstance(self.queue_capacity, bool)
            or not isinstance(self.queue_capacity, int)
            or self.queue_capacity <= 0
        ):
            raise ValueError("Capture-process queue capacity must be a positive integer.")
        for name in (
            "poll_seconds",
            "queue_timeout_seconds",
            "startup_timeout_seconds",
            "shutdown_timeout_seconds",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value <= 0
            ):
                raise ValueError(f"Capture-process {name} must be a finite positive number.")


@dataclass(frozen=True)
class _WorkerReady:
    pid: int
    cpu: int | None
    scheduler: str
    realtime_priority: int
    requested_mode: str
    effective_mode: str
    fallback_reason: str | None


@dataclass(frozen=True)
class _WorkerPulse:
    samples: bytes
    captured_at_unix_ns: int
    captured_at_monotonic_ns: int
    durations: tuple[tuple[str, int], ...]
    native_analysis: NativePulseAnalysis | None = None


@dataclass(frozen=True)
class _WorkerSourceBatch:
    pulses: tuple[_WorkerPulse, ...]
    envelope: SourceBatchEnvelope
    durations: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class _WorkerFence:
    token: int


@dataclass(frozen=True)
class _WorkerFailure:
    phase: str
    error_type: str
    message: str
    hardware_release_confirmed: bool = True


@dataclass(frozen=True)
class _WorkerStopped:
    close_error_type: str | None = None
    close_error_message: str | None = None
    hardware_release_confirmed: bool = True


class _CaptureWorkerTermination(BaseException):
    pass


def _terminate_capture_worker(_signum, _frame) -> None:
    raise _CaptureWorkerTermination("Capture worker received SIGTERM.")


class _WorkerMetrics:
    def __init__(self) -> None:
        self._durations: list[tuple[str, int]] = []

    def record_duration(self, stage: str, duration_ns: int) -> None:
        self._durations.append((stage, duration_ns))

    def set_capture_mode(self, **_values: Any) -> None:
        return

    def take_durations(self) -> tuple[tuple[str, int], ...]:
        durations = tuple(self._durations)
        self._durations.clear()
        return durations


def _configure_worker_scheduling(config: CaptureProcessConfig) -> tuple[int | None, str, int]:
    if config.cpu is not None:
        if not hasattr(os, "sched_setaffinity"):
            raise RuntimeError("This platform does not support CPU affinity.")
        os.sched_setaffinity(0, {config.cpu})
        affinity = os.sched_getaffinity(0)
        if affinity != {config.cpu}:
            raise RuntimeError(f"Capture worker affinity is {sorted(affinity)}, expected [{config.cpu}].")

    if config.realtime_priority is not None:
        if not all(hasattr(os, name) for name in ("SCHED_FIFO", "sched_param", "sched_setscheduler")):
            raise RuntimeError("This platform does not support realtime FIFO scheduling.")
        os.sched_setscheduler(
            0,
            os.SCHED_FIFO,
            os.sched_param(config.realtime_priority),
        )

    if hasattr(os, "sched_getscheduler") and hasattr(os, "sched_getparam"):
        policy = os.sched_getscheduler(0)
        priority = os.sched_getparam(0).sched_priority
        scheduler = "fifo" if policy == getattr(os, "SCHED_FIFO", object()) else str(policy)
    else:
        priority = 0
        scheduler = "default"
    return config.cpu, scheduler, priority


def _capture_worker(
    source_factory: Callable[[], PulseSource],
    outbox,
    status_connection,
    command_connection,
    stop_event,
    config: CaptureProcessConfig,
) -> None:
    source: PulseSource | None = None
    phase = "scheduling"
    worker_failure: _WorkerFailure | None = None
    try:
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, _terminate_capture_worker)
        cpu, scheduler, priority = _configure_worker_scheduling(config)
        phase = "source construction"
        source = source_factory()
        metrics = _WorkerMetrics()
        set_metrics = getattr(source, "set_metrics", None)
        if set_metrics is not None:
            set_metrics(metrics)
        set_stop_requested = getattr(source, "set_stop_requested", None)
        if set_stop_requested is not None:
            set_stop_requested(stop_event.is_set)
        phase = "source open"
        source.open()
        status_connection.send(
            _WorkerReady(
                pid=os.getpid(),
                cpu=cpu,
                scheduler=scheduler,
                realtime_priority=priority,
                requested_mode=str(
                    getattr(source, "requested_capture_mode", getattr(source, "capture_mode", "unknown"))
                ),
                effective_mode=str(getattr(source, "effective_capture_mode", getattr(source, "capture_mode", "unknown"))),
                fallback_reason=getattr(source, "capture_fallback_reason", None),
            )
        )
        phase = "capture"
        while not stop_event.is_set():
            while command_connection.poll():
                token = command_connection.recv()
                outbox.put(_WorkerFence(token), timeout=config.queue_timeout_seconds)
            captured = source.capture()
            if captured is None:
                stop_event.wait(config.poll_seconds)
                continue
            durations = metrics.take_durations()
            if isinstance(captured, SourceCaptureBatch):
                message = _WorkerSourceBatch(
                    pulses=tuple(
                        _WorkerPulse(
                            samples=pulse.samples_v.tobytes(order="C"),
                            captured_at_unix_ns=pulse.captured_at_unix_ns,
                            captured_at_monotonic_ns=pulse.captured_at_monotonic_ns,
                            durations=(),
                            native_analysis=pulse.native_analysis,
                        )
                        for pulse in captured.pulses
                    ),
                    envelope=captured.envelope,
                    durations=durations,
                )
            else:
                message = _WorkerPulse(
                    samples=captured.samples_v.tobytes(order="C"),
                    captured_at_unix_ns=captured.captured_at_unix_ns,
                    captured_at_monotonic_ns=captured.captured_at_monotonic_ns,
                    durations=durations,
                    native_analysis=captured.native_analysis,
                )
            try:
                outbox.put(message, timeout=config.queue_timeout_seconds)
            except queue.Full as exc:
                while True:
                    try:
                        outbox.put(message, timeout=0.05)
                        break
                    except queue.Full:
                        continue
                raise RuntimeError(
                    f"Capture IPC queue reached its capacity of {config.queue_capacity} pulse(s)."
                ) from exc
    except BaseException as exc:
        worker_failure = _WorkerFailure(phase, type(exc).__name__, str(exc))
    finally:
        close_error: BaseException | None = None
        if source is not None:
            try:
                source.close()
            except BaseException as exc:
                close_error = exc
        release_confirmed = bool(
            getattr(source, "release_confirmed", close_error is None)
        )
        try:
            if worker_failure is not None:
                message = worker_failure.message
                if close_error is not None:
                    message += f"; source close also failed: {type(close_error).__name__}: {close_error}"
                worker_failure = _WorkerFailure(
                    worker_failure.phase,
                    worker_failure.error_type,
                    message,
                    release_confirmed,
                )
                status_connection.send(worker_failure)
            else:
                status_connection.send(
                    _WorkerStopped(
                        None if close_error is None else type(close_error).__name__,
                        None if close_error is None else str(close_error),
                        release_confirmed,
                    )
                )
        except (BrokenPipeError, EOFError, OSError):
            pass
        command_connection.close()
        outbox.close()
        outbox.join_thread()
        status_connection.close()


class IsolatedPulseSource:
    def __init__(
        self,
        source_factory: Callable[[], PulseSource],
        capture_config: CaptureConfig,
        *,
        requested_capture_mode: str,
        process_config: CaptureProcessConfig = CaptureProcessConfig(),
        process_context=None,
        metrics: PipelineMetrics | None = None,
    ) -> None:
        if not callable(source_factory):
            raise ValueError("source_factory must be callable.")
        if not requested_capture_mode.strip():
            raise ValueError("requested_capture_mode cannot be empty.")
        self._source_factory = source_factory
        self._capture_config = capture_config
        self._requested_capture_mode = requested_capture_mode
        self._effective_capture_mode = requested_capture_mode
        self._capture_fallback_reason: str | None = None
        self._process_config = process_config
        self._context = process_context or multiprocessing.get_context("spawn")
        self._metrics = metrics or PipelineMetrics()
        self._state = "stopped"
        self._process = None
        self._outbox = None
        self._status_connection = None
        self._command_connection = None
        self._stop_event = None
        self._pending_captures: deque[_WorkerPulse | _WorkerSourceBatch] = deque()
        self._deferred_failure: str | None = None
        self._next_fence_token = 0
        self._ipc_overflow_recorded = False
        self._worker_pid: int | None = None
        self._worker_cpu: int | None = None
        self._worker_scheduler: str | None = None
        self._worker_realtime_priority: int | None = None
        self._unusable = False

    @property
    def capture_config(self) -> CaptureConfig:
        return self._capture_config

    @property
    def state(self) -> str:
        return self._state

    @property
    def capture_mode(self) -> str:
        return self._effective_capture_mode

    @property
    def requested_capture_mode(self) -> str:
        return self._requested_capture_mode

    @property
    def effective_capture_mode(self) -> str:
        return self._effective_capture_mode

    @property
    def capture_fallback_reason(self) -> str | None:
        return self._capture_fallback_reason

    @property
    def worker_pid(self) -> int | None:
        return self._worker_pid

    @property
    def worker_cpu(self) -> int | None:
        return self._worker_cpu

    @property
    def worker_scheduler(self) -> str | None:
        return self._worker_scheduler

    @property
    def worker_realtime_priority(self) -> int | None:
        return self._worker_realtime_priority

    @property
    def process_config(self) -> CaptureProcessConfig:
        return self._process_config

    def set_metrics(self, metrics: PipelineMetrics) -> None:
        self._metrics = metrics

    def open(self) -> None:
        if self._unusable:
            raise RuntimeError(
                "Capture worker could not be terminated; restart the acquisition service before reopening hardware."
            )
        if self._state != "stopped":
            raise RuntimeError("Isolated pulse source is already open.")
        self._state = "starting"
        self._pending_captures.clear()
        self._deferred_failure = None
        self._ipc_overflow_recorded = False
        self._stop_event = self._context.Event()
        self._outbox = self._context.Queue(maxsize=self._process_config.queue_capacity)
        parent_status, child_status = self._context.Pipe(duplex=False)
        child_commands, parent_commands = self._context.Pipe(duplex=False)
        self._status_connection = parent_status
        self._command_connection = parent_commands
        self._process = self._context.Process(
            target=_capture_worker,
            args=(
                self._source_factory,
                self._outbox,
                child_status,
                child_commands,
                self._stop_event,
                self._process_config,
            ),
            name="euv-capture-worker",
            daemon=True,
        )
        try:
            self._process.start()
            child_status.close()
            child_commands.close()
            message = self._wait_for_startup_message()
            if isinstance(message, _WorkerFailure):
                raise RuntimeError(self._failure_text(message))
            if not isinstance(message, _WorkerReady):
                raise RuntimeError(f"Capture worker returned unexpected startup message {type(message).__name__}.")
            self._worker_pid = message.pid
            self._worker_cpu = message.cpu
            self._worker_scheduler = message.scheduler
            self._worker_realtime_priority = message.realtime_priority
            self._effective_capture_mode = message.effective_mode
            self._capture_fallback_reason = message.fallback_reason
            self._metrics.set_capture_mode(
                requested_mode=message.requested_mode,
                effective_mode=message.effective_mode,
                fallback_reason=message.fallback_reason,
            )
            self._observe_ipc_queue()
            self._state = "running"
        except BaseException:
            child_status.close()
            child_commands.close()
            self._stop_worker()
            self._state = "failed" if self._unusable else "stopped"
            raise

    def capture(self) -> CapturedPulse | SourceCaptureBatch | None:
        if self._state != "running":
            raise RuntimeError("Isolated pulse source is not open.")
        message = self._take_outbox_message()
        if message is None:
            self._collect_worker_status()
            message = self._take_outbox_message()
        if message is None:
            self._raise_finished_worker_failure()
            return None
        if not isinstance(message, (_WorkerPulse, _WorkerSourceBatch)):
            raise RuntimeError(f"Capture worker returned unexpected pulse message {type(message).__name__}.")
        return self._decode_capture(message)

    def capture_fence(self) -> tuple[CapturedPulse | SourceCaptureBatch, ...]:
        if self._state != "running":
            raise RuntimeError("Isolated pulse source is not open.")
        self._next_fence_token += 1
        token = self._next_fence_token
        self._command_connection.send(token)
        capture_messages: list[_WorkerPulse | _WorkerSourceBatch] = []
        deadline = time.monotonic() + self._process_config.shutdown_timeout_seconds
        try:
            while time.monotonic() < deadline:
                message = self._take_outbox_message(timeout=min(0.05, deadline - time.monotonic()))
                if isinstance(message, (_WorkerPulse, _WorkerSourceBatch)):
                    capture_messages.append(message)
                    continue
                if isinstance(message, _WorkerFence):
                    if message.token != token:
                        raise RuntimeError(
                            f"Capture worker returned fence {message.token}; expected {token}."
                        )
                    return tuple(self._decode_capture(message) for message in capture_messages)
                self._collect_worker_status()
                self._raise_finished_worker_failure()
            raise TimeoutError("Capture worker did not publish a flush fence before its deadline.")
        except BaseException:
            self._pending_captures.extend(capture_messages)
            raise

    def drain_captured(self) -> tuple[CapturedPulse | SourceCaptureBatch, ...]:
        captures = tuple(self._decode_capture(message) for message in self._pending_captures)
        self._pending_captures.clear()
        return captures

    def _decode_capture(
        self,
        message: _WorkerPulse | _WorkerSourceBatch,
    ) -> CapturedPulse | SourceCaptureBatch:
        if isinstance(message, _WorkerPulse):
            return self._decode_pulse(message)
        for stage, duration_ns in message.durations:
            self._metrics.record_duration(stage, duration_ns)
        return SourceCaptureBatch(
            tuple(self._decode_pulse(pulse) for pulse in message.pulses),
            message.envelope,
        )

    def _decode_pulse(self, message: _WorkerPulse) -> CapturedPulse:
        expected_bytes = self._capture_config.window_samples * np.dtype(np.float32).itemsize
        if len(message.samples) != expected_bytes:
            raise RuntimeError(
                f"Capture worker returned {len(message.samples)} sample bytes; expected {expected_bytes}."
            )
        for stage, duration_ns in message.durations:
            self._metrics.record_duration(stage, duration_ns)
        return CapturedPulse(
            samples_v=np.frombuffer(message.samples, dtype=np.float32),
            captured_at_unix_ns=message.captured_at_unix_ns,
            captured_at_monotonic_ns=message.captured_at_monotonic_ns,
            native_analysis=message.native_analysis,
        )

    def close(self) -> None:
        if self._state == "stopped":
            return
        self._state = "stopping"
        failure = self._stop_worker()
        self._state = "failed" if self._unusable else "stopped"
        if failure is not None:
            raise RuntimeError(failure)

    def _wait_for_startup_message(self):
        deadline = time.monotonic() + self._process_config.startup_timeout_seconds
        while time.monotonic() < deadline:
            if self._status_connection.poll(min(0.05, max(0.0, deadline - time.monotonic()))):
                try:
                    return self._status_connection.recv()
                except EOFError:
                    break
            if not self._process.is_alive():
                break
        exit_code = self._process.exitcode
        if exit_code is None:
            raise TimeoutError("Capture worker did not open before its startup deadline.")
        raise RuntimeError(f"Capture worker exited with code {exit_code} before opening the source.")

    def _collect_worker_status(self) -> None:
        while self._status_connection is not None:
            try:
                if not self._status_connection.poll():
                    return
                message = self._status_connection.recv()
            except (BrokenPipeError, EOFError, OSError):
                return
            if isinstance(message, _WorkerFailure):
                self._deferred_failure = self._failure_text(message)
                if not message.hardware_release_confirmed:
                    self._unusable = True
                    self._deferred_failure += "; hardware release was not confirmed"
                if "Capture IPC queue reached its capacity" in message.message and not self._ipc_overflow_recorded:
                    self._metrics.increment("capture_ipc_overflow")
                    self._ipc_overflow_recorded = True
            elif isinstance(message, _WorkerStopped):
                if not message.hardware_release_confirmed:
                    self._unusable = True
                if message.close_error_type is not None:
                    self._deferred_failure = (
                        f"Capture worker source close failed: {message.close_error_type}: "
                        f"{message.close_error_message}"
                    )
                    if not message.hardware_release_confirmed:
                        self._deferred_failure += "; hardware release was not confirmed"
                elif self._state == "running":
                    self._deferred_failure = "Capture worker stopped unexpectedly."
            else:
                self._deferred_failure = (
                    f"Capture worker returned unexpected status message {type(message).__name__}."
                )

    def _raise_finished_worker_failure(self) -> None:
        if self._process is None or self._process.is_alive():
            return
        failure = self._deferred_failure
        self._deferred_failure = None
        if failure is not None:
            raise RuntimeError(failure)
        raise RuntimeError(f"Capture worker exited unexpectedly with code {self._process.exitcode}.")

    def _take_outbox_message(self, *, timeout: float | None = None):
        if self._outbox is None:
            return None
        self._observe_ipc_queue()
        try:
            message = self._outbox.get_nowait() if timeout is None else self._outbox.get(timeout=timeout)
        except (EOFError, OSError, ValueError, queue.Empty):
            return None
        self._observe_ipc_queue()
        return message

    def _drain_outbox(self) -> None:
        while True:
            message = self._take_outbox_message()
            if message is None:
                return
            if isinstance(message, (_WorkerPulse, _WorkerSourceBatch)):
                self._pending_captures.append(message)
            else:
                self._deferred_failure = (
                    f"Capture worker returned unexpected drain message {type(message).__name__}."
                )

    def _wait_for_worker_exit(self, process, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while process.is_alive() and time.monotonic() < deadline:
            self._drain_outbox()
            self._collect_worker_status()
            process.join(timeout=min(0.01, max(0.0, deadline - time.monotonic())))
        self._drain_outbox()
        self._collect_worker_status()
        return not process.is_alive()

    def _stop_worker(self) -> str | None:
        failure: str | None = None
        process = self._process
        if self._stop_event is not None:
            self._stop_event.set()
        if process is not None:
            if not self._wait_for_worker_exit(process, self._process_config.shutdown_timeout_seconds):
                failure = "Capture worker did not stop before its shutdown deadline and required SIGTERM."
                process.terminate()
                if not self._wait_for_worker_exit(process, self._process_config.shutdown_timeout_seconds):
                    failure = "Capture worker ignored SIGTERM and required SIGKILL; hardware release was not confirmed."
                    self._unusable = True
                    process.kill()
                    process.join(self._process_config.shutdown_timeout_seconds)
                    if process.is_alive():
                        self._unusable = True
                        return (
                            "Capture worker remained alive after SIGKILL; hardware ownership is still active and "
                            "the acquisition service must be restarted."
                        )
            self._drain_outbox()
            self._collect_worker_status()
        if failure is None and self._deferred_failure is not None:
            failure = self._deferred_failure
        self._deferred_failure = None
        if self._status_connection is not None:
            self._status_connection.close()
        if self._command_connection is not None:
            self._command_connection.close()
        if self._outbox is not None:
            self._outbox.close()
            self._outbox.cancel_join_thread()
        if process is not None and not process.is_alive():
            process.close()
        self._process = None
        self._outbox = None
        self._status_connection = None
        self._command_connection = None
        self._stop_event = None
        self._worker_pid = None
        return failure

    def _observe_ipc_queue(self) -> None:
        if self._outbox is None:
            return
        try:
            depth = self._outbox.qsize()
        except (NotImplementedError, OSError):
            return
        self._metrics.observe_queue(
            "capture_ipc",
            depth=min(depth, self._process_config.queue_capacity),
            capacity=self._process_config.queue_capacity,
        )

    @staticmethod
    def _failure_text(message: _WorkerFailure) -> str:
        return f"Capture worker failed during {message.phase}: {message.error_type}: {message.message}"