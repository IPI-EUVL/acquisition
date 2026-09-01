from __future__ import annotations

import json
import math
import os
import threading
import time
from collections import deque
from dataclasses import dataclass, replace
from enum import Enum
from pathlib import Path
from typing import Callable
from uuid import UUID, uuid4

from euv_acquisition.analysis import analyze_pulse
from euv_acquisition.models import (
    CapturedPulse,
    PulseQuality,
    PulseRecord,
    PulseReport,
    SnapshotCloseReason,
    SourceBatchEnvelope,
    SourceCaptureBatch,
)
from euv_acquisition.pipeline_metrics import PipelineMetrics
from euv_acquisition.snapshot import SNAPSHOT_SCHEMA_VERSION, SnapshotManifest, SnapshotStore
from euv_acquisition.sources.base import PulseSource


SESSION_SCHEMA_VERSION = 2
SESSION_MANIFEST_FILENAME = "capture_session.json"
MANIFEST_REPLACE_ATTEMPTS = 5
MANIFEST_REPLACE_DELAY_SECONDS = 0.05


class CaptureSessionState(str, Enum):
    ACTIVE = "active"
    STOPPED = "stopped"
    ORPHANED = "orphaned"


class CapturePurpose(str, Enum):
    EXPERIMENT = "experiment"
    DIAGNOSTIC = "diagnostic"


@dataclass(frozen=True)
class StoredSnapshot:
    manifest: SnapshotManifest
    acknowledged: bool = False

    def to_dict(self) -> dict:
        return {"manifest": self.manifest.to_dict(), "acknowledged": self.acknowledged}

    @classmethod
    def from_dict(cls, value: object) -> "StoredSnapshot":
        if not isinstance(value, dict) or set(value) != {"manifest", "acknowledged"}:
            raise ValueError("Stored snapshot contains unknown or missing fields.")
        if not isinstance(value["acknowledged"], bool):
            raise ValueError("Stored snapshot acknowledgement must be boolean.")
        return cls(SnapshotManifest.from_dict(value["manifest"]), value["acknowledged"])


@dataclass(frozen=True)
class CaptureSessionManifest:
    server_boot_id: UUID
    session_id: UUID
    state: CaptureSessionState
    source_kind: str
    source_id: str
    started_at_unix_ns: int
    purpose: CapturePurpose = CapturePurpose.EXPERIMENT
    snapshots: tuple[StoredSnapshot, ...] = ()
    final_sequence: int | None = None
    stop_reason: str | None = None

    def to_dict(self) -> dict:
        return {
            "schema_version": SESSION_SCHEMA_VERSION,
            "snapshot_schema_version": SNAPSHOT_SCHEMA_VERSION,
            "server_boot_id": str(self.server_boot_id),
            "session_id": str(self.session_id),
            "state": self.state.value,
            "source_kind": self.source_kind,
            "source_id": self.source_id,
            "started_at_unix_ns": self.started_at_unix_ns,
            "purpose": self.purpose.value,
            "snapshots": [snapshot.to_dict() for snapshot in self.snapshots],
            "final_sequence": self.final_sequence,
            "stop_reason": self.stop_reason,
        }

    @classmethod
    def from_dict(cls, value: object) -> "CaptureSessionManifest":
        expected = {
            "schema_version",
            "snapshot_schema_version",
            "server_boot_id",
            "session_id",
            "state",
            "source_kind",
            "source_id",
            "started_at_unix_ns",
            "snapshots",
            "final_sequence",
            "stop_reason",
        }
        if not isinstance(value, dict):
            raise ValueError("Capture session manifest contains unknown or missing fields.")
        schema_version = value.get("schema_version")
        if schema_version == SESSION_SCHEMA_VERSION:
            expected.add("purpose")
        elif schema_version != 1:
            raise ValueError("Unsupported capture session schema version.")
        if set(value) != expected:
            raise ValueError("Capture session manifest contains unknown or missing fields.")
        if value["snapshot_schema_version"] != SNAPSHOT_SCHEMA_VERSION:
            raise ValueError("Unsupported capture-session snapshot schema version.")
        if not isinstance(value["snapshots"], list):
            raise ValueError("Capture session snapshots must be a list.")
        final_sequence = value["final_sequence"]
        manifest = cls(
            server_boot_id=UUID(str(value["server_boot_id"])),
            session_id=UUID(str(value["session_id"])),
            state=CaptureSessionState(value["state"]),
            source_kind=str(value["source_kind"]),
            source_id=str(value["source_id"]),
            started_at_unix_ns=int(value["started_at_unix_ns"]),
            purpose=CapturePurpose(value.get("purpose", CapturePurpose.EXPERIMENT.value)),
            snapshots=tuple(StoredSnapshot.from_dict(item) for item in value["snapshots"]),
            final_sequence=None if final_sequence is None else int(final_sequence),
            stop_reason=None if value["stop_reason"] is None else str(value["stop_reason"]),
        )
        if not manifest.source_kind.strip() or not manifest.source_id.strip():
            raise ValueError("Capture session source fields cannot be empty.")
        if manifest.started_at_unix_ns < 0:
            raise ValueError("Capture session start timestamp must be non-negative.")
        snapshot_ids = [item.manifest.snapshot_id for item in manifest.snapshots]
        if len(snapshot_ids) != len(set(snapshot_ids)):
            raise ValueError("Capture session contains duplicate snapshots.")
        if any(item.manifest.session_id != manifest.session_id for item in manifest.snapshots):
            raise ValueError("Capture session contains a snapshot from another session.")
        if manifest.state is CaptureSessionState.ACTIVE:
            if manifest.final_sequence is not None or manifest.stop_reason is not None:
                raise ValueError("Active capture session cannot have terminal fields.")
        elif manifest.stop_reason is None:
            raise ValueError("Terminal capture session must include a stop reason.")
        return manifest


class SpoolRepository:
    def __init__(self, root: str | Path, *, server_boot_id: UUID | None = None) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.server_boot_id = server_boot_id or uuid4()
        self.manifest_path = self.root / SESSION_MANIFEST_FILENAME
        self._lock = threading.RLock()

    def load(self) -> CaptureSessionManifest | None:
        with self._lock:
            try:
                with self.manifest_path.open("r", encoding="utf-8") as source:
                    return CaptureSessionManifest.from_dict(json.load(source))
            except FileNotFoundError:
                return None

    def _write(self, manifest: CaptureSessionManifest) -> CaptureSessionManifest:
        with self._lock:
            temporary = self.root / f".{SESSION_MANIFEST_FILENAME}.{uuid4().hex}.tmp"
            try:
                with temporary.open("x", encoding="utf-8", newline="\n") as output:
                    json.dump(manifest.to_dict(), output, allow_nan=False, separators=(",", ":"))
                    output.flush()
                    os.fsync(output.fileno())
                for attempt in range(1, MANIFEST_REPLACE_ATTEMPTS + 1):
                    try:
                        os.replace(temporary, self.manifest_path)
                        break
                    except PermissionError:
                        if attempt == MANIFEST_REPLACE_ATTEMPTS:
                            raise
                        time.sleep(MANIFEST_REPLACE_DELAY_SECONDS * attempt)
            finally:
                temporary.unlink(missing_ok=True)
            return manifest

    def begin(
        self,
        session_id: UUID,
        source_kind: str,
        source_id: str,
        started_at_unix_ns: int,
        purpose: CapturePurpose = CapturePurpose.EXPERIMENT,
    ) -> CaptureSessionManifest:
        with self._lock:
            if self.load() is not None:
                raise RuntimeError("Cannot start capture while an unreleased session remains in the spool.")
            return self._write(
                CaptureSessionManifest(
                    server_boot_id=self.server_boot_id,
                    session_id=session_id,
                    state=CaptureSessionState.ACTIVE,
                    source_kind=source_kind,
                    source_id=source_id,
                    started_at_unix_ns=started_at_unix_ns,
                    purpose=CapturePurpose(purpose),
                )
            )

    def add_snapshot(self, snapshot: SnapshotManifest) -> CaptureSessionManifest:
        with self._lock:
            current = self._require()
            if current.state is not CaptureSessionState.ACTIVE:
                raise RuntimeError("Cannot append a snapshot to a terminal capture session.")
            if snapshot.session_id != current.session_id:
                raise ValueError("Snapshot belongs to another capture session.")
            if any(item.manifest.snapshot_id == snapshot.snapshot_id for item in current.snapshots):
                return current
            return self._write(replace(current, snapshots=current.snapshots + (StoredSnapshot(snapshot),)))

    def acknowledge(self, snapshot_id: UUID) -> CaptureSessionManifest:
        with self._lock:
            current = self._require()
            found = False
            snapshots = []
            for item in current.snapshots:
                if item.manifest.snapshot_id == snapshot_id:
                    found = True
                    item = replace(item, acknowledged=True)
                snapshots.append(item)
            if not found:
                raise ValueError(f"Snapshot {snapshot_id} is not in the current session.")
            return self._write(replace(current, snapshots=tuple(snapshots)))

    def purge_snapshot(self, snapshot_store: SnapshotStore, snapshot_id: UUID) -> CaptureSessionManifest:
        with self._lock:
            current = self._require()
            stored = next((item for item in current.snapshots if item.manifest.snapshot_id == snapshot_id), None)
            if stored is None:
                raise ValueError(f"Snapshot {snapshot_id} is not in the current session.")
            if not stored.acknowledged:
                raise RuntimeError("Cannot purge an unacknowledged snapshot.")
            snapshot_store.path_for(stored.manifest).unlink(missing_ok=True)
            return current

    def stop(self, final_sequence: int | None, reason: str) -> CaptureSessionManifest:
        with self._lock:
            current = self._require()
            if current.state is not CaptureSessionState.ACTIVE:
                return current
            if not reason.strip():
                raise ValueError("Capture stop reason cannot be empty.")
            return self._write(
                replace(
                    current,
                    state=CaptureSessionState.STOPPED,
                    final_sequence=final_sequence,
                    stop_reason=reason,
                )
            )

    def mark_active_session_orphaned(self) -> CaptureSessionManifest | None:
        with self._lock:
            current = self.load()
            if current is None or current.state is not CaptureSessionState.ACTIVE:
                return current
            final_sequence = current.snapshots[-1].manifest.final_sequence if current.snapshots else None
            return self._write(
                replace(
                    current,
                    state=CaptureSessionState.ORPHANED,
                    final_sequence=final_sequence,
                    stop_reason="Digitizer service restarted during capture.",
                )
            )

    def release(self, snapshot_store: SnapshotStore) -> None:
        with self._lock:
            current = self._require()
            if current.state is CaptureSessionState.ACTIVE:
                raise RuntimeError("Cannot release an active capture session.")
            unacknowledged = [item.manifest.snapshot_id for item in current.snapshots if not item.acknowledged]
            if unacknowledged:
                raise RuntimeError(f"Cannot release session with {len(unacknowledged)} unacknowledged snapshot(s).")
            for item in current.snapshots:
                snapshot_store.path_for(item.manifest).unlink(missing_ok=True)
            self.manifest_path.unlink()

    def discard_diagnostic(self, snapshot_store: SnapshotStore, session_id: UUID) -> None:
        with self._lock:
            current = self._require()
            if current.session_id != session_id:
                raise ValueError("Diagnostic session ID does not match the retained session.")
            if current.purpose is not CapturePurpose.DIAGNOSTIC:
                raise RuntimeError("Cannot discard an experiment capture session.")
            if current.state is CaptureSessionState.ACTIVE:
                raise RuntimeError("Cannot discard an active diagnostic session.")
            for item in current.snapshots:
                snapshot_store.path_for(item.manifest).unlink(missing_ok=True)
            self.manifest_path.unlink()

    def _require(self) -> CaptureSessionManifest:
        manifest = self.load()
        if manifest is None:
            raise RuntimeError("No capture session exists in the spool.")
        return manifest


@dataclass(frozen=True)
class RotationConfig:
    pulse_limit: int = 250
    wall_time_seconds: float = 5.0
    trigger_idle_seconds: float = 0.5
    clipped_pulse_limit: int = 3
    clipped_pulse_window: int = 100

    def __post_init__(self) -> None:
        for name in ("pulse_limit", "clipped_pulse_limit", "clipped_pulse_window"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer.")
        for name in ("wall_time_seconds", "trigger_idle_seconds"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)) or value <= 0:
                raise ValueError(f"{name} must be a finite positive number.")
        if self.clipped_pulse_limit > self.clipped_pulse_window:
            raise ValueError("clipped_pulse_limit cannot exceed its rolling window.")


@dataclass(frozen=True)
class CaptureUpdate:
    report: PulseReport | None = None
    closed_snapshots: tuple[SnapshotManifest, ...] = ()
    stop_reason: str | None = None


@dataclass(frozen=True)
class AcceptedPulse:
    sequence: int
    pulse: CapturedPulse


@dataclass(frozen=True)
class AcceptedSourceBatch:
    pulses: tuple[AcceptedPulse, ...]
    envelope: SourceBatchEnvelope


@dataclass(frozen=True)
class SnapshotBatch:
    records: tuple[PulseRecord, ...]
    close_reason: SnapshotCloseReason
    source_batch: SourceBatchEnvelope | None = None

    def __post_init__(self) -> None:
        if (self.close_reason is SnapshotCloseReason.SOURCE_BATCH) != (self.source_batch is not None):
            raise ValueError("Snapshot source batch envelope does not match its close reason.")


@dataclass(frozen=True)
class PulseProcessingResult:
    report: PulseReport
    closed_batches: tuple[SnapshotBatch, ...] = ()
    stop_reason: str | None = None


@dataclass(frozen=True)
class SourceBatchProcessingResult:
    reports: tuple[PulseReport, ...]
    closed_batch: SnapshotBatch
    stop_reason: str | None = None


class CaptureEngine:
    def __init__(
        self,
        source: PulseSource,
        snapshot_store: SnapshotStore,
        spool: SpoolRepository,
        *,
        source_kind: str,
        source_id: str,
        rotation: RotationConfig = RotationConfig(),
        unix_time_ns: Callable[[], int] = time.time_ns,
        monotonic_time_ns: Callable[[], int] = time.monotonic_ns,
        metrics: PipelineMetrics | None = None,
    ) -> None:
        self.source = source
        self.snapshot_store = snapshot_store
        self.spool = spool
        self.source_kind = source_kind
        self.source_id = source_id
        self.rotation = rotation
        self._unix_time_ns = unix_time_ns
        self._monotonic_time_ns = monotonic_time_ns
        source_metrics = getattr(source, "_metrics", None)
        self.metrics = metrics or source_metrics or PipelineMetrics()
        set_metrics = getattr(source, "set_metrics", None)
        if set_metrics is not None:
            set_metrics(self.metrics)
        self._session_id: UUID | None = None
        self._next_sequence = 0
        self._next_analysis_sequence = 0
        self._pending: list[PulseRecord] = []
        self._clipping_window: deque[bool] = deque(maxlen=rotation.clipped_pulse_window)
        self._terminal_requested = False
        self._active = False
        self._lock = threading.RLock()

    @property
    def active(self) -> bool:
        with self._lock:
            return self._active

    @property
    def session_id(self) -> UUID | None:
        with self._lock:
            return self._session_id

    def start(
        self,
        session_id: UUID | None = None,
        purpose: CapturePurpose = CapturePurpose.EXPERIMENT,
    ) -> UUID:
        with self._lock:
            selected_id = self._begin_session_locked(session_id, purpose)
            try:
                self.source.open()
            except Exception as exc:
                self.spool.stop(None, "Pulse source failed to open.")
                self.metrics.finish(terminal_error=f"Pulse source failed to open: {type(exc).__name__}: {exc}")
                self._active = False
                raise
            self.refresh_capture_mode()
            return selected_id

    def begin_session(
        self,
        session_id: UUID | None = None,
        purpose: CapturePurpose = CapturePurpose.EXPERIMENT,
    ) -> UUID:
        with self._lock:
            return self._begin_session_locked(session_id, purpose)

    def refresh_capture_mode(self) -> None:
        requested = str(
            getattr(
                self.source,
                "requested_capture_mode",
                getattr(self.source, "capture_mode", self.source_kind),
            )
        )
        effective = str(getattr(self.source, "effective_capture_mode", requested))
        fallback_reason = getattr(self.source, "capture_fallback_reason", None)
        self.metrics.set_capture_mode(
            requested_mode=requested,
            effective_mode=effective,
            fallback_reason=fallback_reason,
        )
        worker_pid = getattr(self.source, "worker_pid", None)
        if worker_pid is not None:
            self.metrics.set_capture_worker(
                pid=worker_pid,
                cpu=getattr(self.source, "worker_cpu", None),
                scheduler=str(getattr(self.source, "worker_scheduler", "unknown")),
                realtime_priority=int(getattr(self.source, "worker_realtime_priority", 0)),
            )

    def accept_pulse(self, pulse: CapturedPulse) -> AcceptedPulse:
        with self._lock:
            self._require_active()
            if len(pulse.samples_v) != self.source.capture_config.window_samples:
                raise ValueError("Pulse source returned the wrong sample count.")
            owned_pulse = CapturedPulse(
                samples_v=pulse.samples_v.copy(),
                captured_at_unix_ns=pulse.captured_at_unix_ns,
                captured_at_monotonic_ns=pulse.captured_at_monotonic_ns,
                native_analysis=pulse.native_analysis,
            )
            accepted = AcceptedPulse(self._next_sequence, owned_pulse)
            self._next_sequence += 1
            self.metrics.increment("accepted")
            return accepted

    def accept_source_batch(self, batch: SourceCaptureBatch) -> AcceptedSourceBatch:
        with self._lock:
            self._require_active()
            owned_pulses = []
            for pulse in batch.pulses:
                if len(pulse.samples_v) != self.source.capture_config.window_samples:
                    raise ValueError("Pulse source returned the wrong sample count.")
                owned_pulses.append(
                    CapturedPulse(
                        samples_v=pulse.samples_v.copy(),
                        captured_at_unix_ns=pulse.captured_at_unix_ns,
                        captured_at_monotonic_ns=pulse.captured_at_monotonic_ns,
                        native_analysis=pulse.native_analysis,
                    )
                )
            accepted = AcceptedSourceBatch(
                tuple(
                    AcceptedPulse(self._next_sequence + offset, pulse)
                    for offset, pulse in enumerate(owned_pulses)
                ),
                batch.envelope,
            )
            self._next_sequence += len(accepted.pulses)
            self.metrics.increment("accepted", len(accepted.pulses))
            return accepted

    def process_accepted(self, accepted: AcceptedPulse) -> PulseProcessingResult:
        with self._lock:
            self._require_active()
            if accepted.sequence != self._next_analysis_sequence:
                raise ValueError(
                    f"Expected accepted pulse sequence {self._next_analysis_sequence}, got {accepted.sequence}."
                )
            analysis_started_ns = self._monotonic_time_ns()
            analysis = accepted.pulse.native_analysis
            if analysis is None:
                analysis = analyze_pulse(accepted.pulse.samples_v, self.source.capture_config)
            self.metrics.record_duration("analysis", self._monotonic_time_ns() - analysis_started_ns)
            self.metrics.increment("analyzed")
            record = PulseRecord(self._session_id, accepted.sequence, accepted.pulse, analysis)
            self._next_analysis_sequence += 1
            self._pending.append(record)
            self._clipping_window.append(bool(analysis.quality & PulseQuality.CLIPPED))
            closed: list[SnapshotBatch] = []

            if not self._terminal_requested and len(self._pending) >= self.rotation.pulse_limit:
                closed.append(self._take_batch_locked(SnapshotCloseReason.PULSE_LIMIT))

            stop_reason = None
            if not self._terminal_requested and sum(self._clipping_window) >= self.rotation.clipped_pulse_limit:
                self._terminal_requested = True
                if self._pending:
                    closed.append(self._take_batch_locked(SnapshotCloseReason.ACQUISITION_ERROR))
                stop_reason = (
                    f"Clipping limit reached: {sum(self._clipping_window)} clipped pulse(s) "
                    f"in the last {len(self._clipping_window)} pulse(s)."
                )

            return PulseProcessingResult(PulseReport.from_record(record), tuple(closed), stop_reason)

    def process_accepted_source_batch(
        self,
        accepted: AcceptedSourceBatch,
    ) -> SourceBatchProcessingResult:
        with self._lock:
            self._require_active()
            if self._pending:
                raise RuntimeError("Source capture batch cannot be merged with pending pulses.")
            if not accepted.pulses:
                raise ValueError("Accepted source batch cannot be empty.")
            expected_sequences = range(
                self._next_analysis_sequence,
                self._next_analysis_sequence + len(accepted.pulses),
            )
            if any(item.sequence != expected for item, expected in zip(accepted.pulses, expected_sequences)):
                raise ValueError("Accepted source batch pulse sequences are not contiguous.")

            records = []
            analysis_duration_ns = 0
            for item in accepted.pulses:
                analysis_started_ns = self._monotonic_time_ns()
                analysis = item.pulse.native_analysis
                if analysis is None:
                    analysis = analyze_pulse(item.pulse.samples_v, self.source.capture_config)
                analysis_duration_ns += self._monotonic_time_ns() - analysis_started_ns
                records.append(PulseRecord(self._session_id, item.sequence, item.pulse, analysis))
            if len({record.analysis.algorithm_version for record in records}) != 1:
                raise ValueError("Source capture batch must use one native analysis version.")

            self.metrics.record_duration("analysis", analysis_duration_ns)
            self.metrics.increment("analyzed", len(records))
            self._next_analysis_sequence += len(records)
            self._pending.extend(records)
            self._clipping_window.extend(
                bool(record.analysis.quality & PulseQuality.CLIPPED) for record in records
            )
            closed_batch = self._take_batch_locked(
                SnapshotCloseReason.SOURCE_BATCH,
                accepted.envelope,
            )

            stop_reason = None
            if not self._terminal_requested and sum(self._clipping_window) >= self.rotation.clipped_pulse_limit:
                self._terminal_requested = True
                stop_reason = (
                    f"Clipping limit reached: {sum(self._clipping_window)} clipped pulse(s) "
                    f"in the last {len(self._clipping_window)} pulse(s)."
                )
            return SourceBatchProcessingResult(
                tuple(PulseReport.from_record(record) for record in records),
                closed_batch,
                stop_reason,
            )

    def take_due_batch(self, now_monotonic_ns: int | None = None) -> SnapshotBatch | None:
        with self._lock:
            self._require_active()
            if not self._pending or self._terminal_requested:
                return None
            now = self._monotonic_time_ns() if now_monotonic_ns is None else now_monotonic_ns
            first = self._pending[0].pulse.captured_at_monotonic_ns
            last = self._pending[-1].pulse.captured_at_monotonic_ns
            if now - first >= int(self.rotation.wall_time_seconds * 1e9):
                return self._take_batch_locked(SnapshotCloseReason.WALL_TIME)
            if now - last >= int(self.rotation.trigger_idle_seconds * 1e9):
                return self._take_batch_locked(SnapshotCloseReason.TRIGGER_IDLE)
            return None

    def take_pending_batch(self, close_reason: SnapshotCloseReason) -> SnapshotBatch | None:
        with self._lock:
            self._require_active()
            return self._take_batch_locked(close_reason) if self._pending else None

    def request_terminal(self) -> None:
        with self._lock:
            self._require_active()
            self._terminal_requested = True

    def persist_batch(self, batch: SnapshotBatch) -> SnapshotManifest:
        started_ns = self._monotonic_time_ns()
        manifest = self.snapshot_store.write(
            batch.records,
            self.source.capture_config,
            batch.close_reason,
            source_kind=self.source_kind,
            source_id=self.source_id,
            source_batch=batch.source_batch,
        )
        self.spool.add_snapshot(manifest)
        self.metrics.record_duration("snapshot_write", self._monotonic_time_ns() - started_ns)
        self.metrics.increment("persisted", len(batch.records))
        self.metrics.increment("snapshots_written")
        return manifest

    def finalize_session(self, reason: str, *, terminal_error: str | None = None) -> None:
        with self._lock:
            self._require_active()
            final_sequence = self._next_sequence - 1 if self._next_sequence else None
            self.spool.stop(final_sequence, reason)
            self._active = False
            self.metrics.finish(terminal_error=terminal_error)

    def capture_once(self) -> CaptureUpdate:
        with self._lock:
            self._require_active()
            try:
                pulse = self.source.capture()
            except Exception as exc:
                return self._stop_for_error(f"Pulse source failure: {type(exc).__name__}: {exc}")
            if pulse is None:
                return self.tick()
            try:
                if isinstance(pulse, SourceCaptureBatch):
                    accepted_batch = self.accept_source_batch(pulse)
                    processed_batch = self.process_accepted_source_batch(accepted_batch)
                    manifest = self.persist_batch(processed_batch.closed_batch)
                    if processed_batch.stop_reason is not None:
                        self.source.close()
                        self.finalize_session(
                            processed_batch.stop_reason,
                            terminal_error=processed_batch.stop_reason,
                        )
                    return CaptureUpdate(
                        processed_batch.reports[-1],
                        (manifest,),
                        processed_batch.stop_reason,
                    )
                accepted = self.accept_pulse(pulse)
            except ValueError:
                return self._stop_for_error("Pulse source returned the wrong sample count.")
            processed = self.process_accepted(accepted)
            closed = tuple(self.persist_batch(batch) for batch in processed.closed_batches)
            if processed.stop_reason is not None:
                self.source.close()
                self.finalize_session(processed.stop_reason, terminal_error=processed.stop_reason)
            return CaptureUpdate(processed.report, closed, processed.stop_reason)

    def tick(self, now_monotonic_ns: int | None = None) -> CaptureUpdate:
        with self._lock:
            batch = self.take_due_batch(now_monotonic_ns)
            return CaptureUpdate() if batch is None else CaptureUpdate(closed_snapshots=(self.persist_batch(batch),))

    def flush(self) -> CaptureUpdate:
        with self._lock:
            batch = self.take_pending_batch(SnapshotCloseReason.EXPLICIT_FLUSH)
            return CaptureUpdate() if batch is None else CaptureUpdate(closed_snapshots=(self.persist_batch(batch),))

    def stop(self, reason: str = "Capture stop requested.") -> CaptureUpdate:
        with self._lock:
            self._require_active()
            batch = self.take_pending_batch(SnapshotCloseReason.CAPTURE_STOP)
            closed = () if batch is None else (self.persist_batch(batch),)
            self.source.close()
            self.finalize_session(reason)
            return CaptureUpdate(closed_snapshots=closed, stop_reason=reason)

    def abort(self, reason: str) -> CaptureUpdate:
        with self._lock:
            self._require_active()
            if not reason.strip():
                raise ValueError("Capture abort reason cannot be empty.")
            return self._stop_for_error(reason)

    def abort_if_active(self, reason: str) -> CaptureUpdate | None:
        with self._lock:
            if not self._active:
                return None
            if not reason.strip():
                raise ValueError("Capture abort reason cannot be empty.")
            return self._stop_for_error(reason)

    def _stop_for_error(self, reason: str) -> CaptureUpdate:
        batch = self.take_pending_batch(SnapshotCloseReason.ACQUISITION_ERROR)
        closed = () if batch is None else (self.persist_batch(batch),)
        self.source.close()
        self.finalize_session(reason, terminal_error=reason)
        return CaptureUpdate(closed_snapshots=closed, stop_reason=reason)

    def _take_batch_locked(
        self,
        close_reason: SnapshotCloseReason,
        source_batch: SourceBatchEnvelope | None = None,
    ) -> SnapshotBatch:
        batch = SnapshotBatch(tuple(self._pending), close_reason, source_batch)
        self._pending.clear()
        return batch

    def _begin_session_locked(
        self,
        session_id: UUID | None,
        purpose: CapturePurpose,
    ) -> UUID:
        if self._active:
            raise RuntimeError("Capture is already active.")
        selected_id = session_id or uuid4()
        self.spool.begin(
            selected_id,
            self.source_kind,
            self.source_id,
            self._unix_time_ns(),
            CapturePurpose(purpose),
        )
        requested_mode = str(
            getattr(
                self.source,
                "requested_capture_mode",
                getattr(self.source, "capture_mode", self.source_kind),
            )
        )
        self.metrics.begin_session(
            selected_id,
            requested_mode=requested_mode,
            effective_mode=str(getattr(self.source, "effective_capture_mode", requested_mode)),
            fallback_reason=getattr(self.source, "capture_fallback_reason", None),
        )
        self._session_id = selected_id
        self._next_sequence = 0
        self._next_analysis_sequence = 0
        self._pending.clear()
        self._clipping_window.clear()
        self._terminal_requested = False
        self._active = True
        return selected_id

    def _require_active(self) -> None:
        if not self._active or self._session_id is None:
            raise RuntimeError("Capture is not active.")