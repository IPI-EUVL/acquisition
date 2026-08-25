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
from euv_acquisition.models import PulseQuality, PulseRecord, PulseReport, SnapshotCloseReason
from euv_acquisition.snapshot import SNAPSHOT_SCHEMA_VERSION, SnapshotManifest, SnapshotStore
from euv_acquisition.sources.base import PulseSource


SESSION_SCHEMA_VERSION = 1
SESSION_MANIFEST_FILENAME = "capture_session.json"
MANIFEST_REPLACE_ATTEMPTS = 5
MANIFEST_REPLACE_DELAY_SECONDS = 0.05


class CaptureSessionState(str, Enum):
    ACTIVE = "active"
    STOPPED = "stopped"
    ORPHANED = "orphaned"


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
        if not isinstance(value, dict) or set(value) != expected:
            raise ValueError("Capture session manifest contains unknown or missing fields.")
        if value["schema_version"] != SESSION_SCHEMA_VERSION:
            raise ValueError("Unsupported capture session schema version.")
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

    def begin(self, session_id: UUID, source_kind: str, source_id: str, started_at_unix_ns: int) -> CaptureSessionManifest:
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
    ) -> None:
        self.source = source
        self.snapshot_store = snapshot_store
        self.spool = spool
        self.source_kind = source_kind
        self.source_id = source_id
        self.rotation = rotation
        self._unix_time_ns = unix_time_ns
        self._monotonic_time_ns = monotonic_time_ns
        self._session_id: UUID | None = None
        self._next_sequence = 0
        self._pending: list[PulseRecord] = []
        self._clipping_window: deque[bool] = deque(maxlen=rotation.clipped_pulse_window)
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

    def start(self, session_id: UUID | None = None) -> UUID:
        with self._lock:
            if self._active:
                raise RuntimeError("Capture is already active.")
            selected_id = session_id or uuid4()
            self.spool.begin(selected_id, self.source_kind, self.source_id, self._unix_time_ns())
            try:
                self.source.open()
            except Exception:
                self.spool.stop(None, "Pulse source failed to open.")
                raise
            self._session_id = selected_id
            self._next_sequence = 0
            self._pending.clear()
            self._clipping_window.clear()
            self._active = True
            return selected_id

    def capture_once(self) -> CaptureUpdate:
        with self._lock:
            self._require_active()
            try:
                pulse = self.source.capture()
            except Exception as exc:
                return self._stop_for_error(f"Pulse source failure: {type(exc).__name__}: {exc}")
            if pulse is None:
                return self.tick()
            if len(pulse.samples_v) != self.source.capture_config.window_samples:
                return self._stop_for_error("Pulse source returned the wrong sample count.")

            analysis = analyze_pulse(pulse.samples_v, self.source.capture_config)
            record = PulseRecord(self._session_id, self._next_sequence, pulse, analysis)
            self._next_sequence += 1
            self._pending.append(record)
            self._clipping_window.append(bool(analysis.quality & PulseQuality.CLIPPED))
            report = PulseReport.from_record(record)
            closed = []

            if len(self._pending) >= self.rotation.pulse_limit:
                closed.append(self._flush(SnapshotCloseReason.PULSE_LIMIT))

            if sum(self._clipping_window) >= self.rotation.clipped_pulse_limit:
                if self._pending:
                    closed.append(self._flush(SnapshotCloseReason.ACQUISITION_ERROR))
                reason = (
                    f"Clipping limit reached: {sum(self._clipping_window)} clipped pulse(s) "
                    f"in the last {len(self._clipping_window)} pulse(s)."
                )
                self._finish(reason)
                return CaptureUpdate(report, tuple(closed), reason)

            return CaptureUpdate(report, tuple(closed))

    def tick(self, now_monotonic_ns: int | None = None) -> CaptureUpdate:
        with self._lock:
            self._require_active()
            if not self._pending:
                return CaptureUpdate()
            now = self._monotonic_time_ns() if now_monotonic_ns is None else now_monotonic_ns
            first = self._pending[0].pulse.captured_at_monotonic_ns
            last = self._pending[-1].pulse.captured_at_monotonic_ns
            if now - first >= int(self.rotation.wall_time_seconds * 1e9):
                return CaptureUpdate(closed_snapshots=(self._flush(SnapshotCloseReason.WALL_TIME),))
            if now - last >= int(self.rotation.trigger_idle_seconds * 1e9):
                return CaptureUpdate(closed_snapshots=(self._flush(SnapshotCloseReason.TRIGGER_IDLE),))
            return CaptureUpdate()

    def flush(self) -> CaptureUpdate:
        with self._lock:
            self._require_active()
            if not self._pending:
                return CaptureUpdate()
            return CaptureUpdate(closed_snapshots=(self._flush(SnapshotCloseReason.EXPLICIT_FLUSH),))

    def stop(self, reason: str = "Capture stop requested.") -> CaptureUpdate:
        with self._lock:
            self._require_active()
            closed = (self._flush(SnapshotCloseReason.CAPTURE_STOP),) if self._pending else ()
            self._finish(reason)
            return CaptureUpdate(closed_snapshots=closed, stop_reason=reason)

    def abort(self, reason: str) -> CaptureUpdate:
        with self._lock:
            self._require_active()
            if not reason.strip():
                raise ValueError("Capture abort reason cannot be empty.")
            return self._stop_for_error(reason)

    def _stop_for_error(self, reason: str) -> CaptureUpdate:
        closed = (self._flush(SnapshotCloseReason.ACQUISITION_ERROR),) if self._pending else ()
        self._finish(reason)
        return CaptureUpdate(closed_snapshots=closed, stop_reason=reason)

    def _flush(self, close_reason: SnapshotCloseReason) -> SnapshotManifest:
        manifest = self.snapshot_store.write(
            self._pending,
            self.source.capture_config,
            close_reason,
            source_kind=self.source_kind,
            source_id=self.source_id,
        )
        self.spool.add_snapshot(manifest)
        self._pending.clear()
        return manifest

    def _finish(self, reason: str) -> None:
        try:
            self.source.close()
        finally:
            final_sequence = self._next_sequence - 1 if self._next_sequence else None
            self.spool.stop(final_sequence, reason)
            self._active = False

    def _require_active(self) -> None:
        if not self._active or self._session_id is None:
            raise RuntimeError("Capture is not active.")