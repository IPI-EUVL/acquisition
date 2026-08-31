from __future__ import annotations

import queue
import socket
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID, uuid4

from euv_acquisition.protocol import (
    PROTOCOL_VERSION,
    command_message,
    heartbeat_message,
    is_heartbeat,
    receive_artifact,
    receive_frame,
    response_message,
    send_artifact,
    send_frame,
    validate_command_message,
    validate_response_message,
)
from euv_acquisition.session import CaptureEngine, CapturePurpose, CaptureSessionManifest, CaptureSessionState
from euv_acquisition.snapshot import SnapshotManifest


class ServiceLogger(Protocol):
    def log(self, message: str, **kwargs: Any) -> None: ...


class SimulatorControlProvider(Protocol):
    def set_control(self, name: str, enabled: bool) -> None: ...

    def restore_nominal(self) -> None: ...

    def status_value(self) -> dict[str, Any]: ...


@dataclass(frozen=True)
class ServiceConfig:
    host: str = "127.0.0.1"
    control_port: int = 11760
    artifact_port: int = 11761
    controller_watchdog_seconds: float = 5.0
    capture_poll_seconds: float = 0.001

    def __post_init__(self) -> None:
        for name in ("control_port", "artifact_port"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 65535:
                raise ValueError(f"{name} must be a TCP port number.")
        for name in ("controller_watchdog_seconds", "capture_poll_seconds"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
                raise ValueError(f"{name} must be positive.")


class AcquisitionServer:
    def __init__(
        self,
        engine: CaptureEngine,
        config: ServiceConfig = ServiceConfig(),
        *,
        logger: ServiceLogger | None = None,
        simulator_controls: SimulatorControlProvider | None = None,
    ) -> None:
        self.engine = engine
        self.config = config
        self._logger = logger
        self._simulator_controls = simulator_controls
        self._stop = threading.Event()
        self._control_listener: socket.socket | None = None
        self._artifact_listener: socket.socket | None = None
        self._control_connection: socket.socket | None = None
        self._control_send_lock = threading.Lock()
        self._control_lock = threading.Lock()
        self._last_controller_activity = 0.0
        self._threads: list[threading.Thread] = []

    @property
    def control_address(self) -> tuple[str, int]:
        if self._control_listener is None:
            raise RuntimeError("Acquisition server is not started.")
        host, port = self._control_listener.getsockname()[:2]
        return host, port

    @property
    def artifact_address(self) -> tuple[str, int]:
        if self._artifact_listener is None:
            raise RuntimeError("Acquisition server is not started.")
        host, port = self._artifact_listener.getsockname()[:2]
        return host, port

    def start(self) -> None:
        if self._threads:
            raise RuntimeError("Acquisition server is already started.")
        orphaned = self.engine.spool.mark_active_session_orphaned()
        if orphaned is not None and orphaned.state is CaptureSessionState.ORPHANED:
            self._log(
                f"Marked interrupted capture session {orphaned.session_id} orphaned during server startup.",
                level="WARNING",
                event="capture_session_orphaned",
                session_id=str(orphaned.session_id),
            )
        self._control_listener = self._listener(self.config.control_port)
        self._artifact_listener = self._listener(self.config.artifact_port)
        self._log(
            f"Acquisition server listening on {self.control_address} for control and {self.artifact_address} for artifacts.",
            event="acquisition_server_started",
            control_address=self.control_address,
            artifact_address=self.artifact_address,
            source_kind=self.engine.source_kind,
            source_id=self.engine.source_id,
        )
        self._threads = [
            threading.Thread(target=self._accept_control, name="euv-control-accept", daemon=True),
            threading.Thread(target=self._accept_artifact, name="euv-artifact-accept", daemon=True),
            threading.Thread(target=self._capture_loop, name="euv-capture", daemon=True),
        ]
        for thread in self._threads:
            thread.start()

    def close(self) -> None:
        self._log("Acquisition server shutdown requested.", event="acquisition_server_stopping")
        self._stop.set()
        if self.engine.active:
            update = self.engine.abort("Acquisition server is shutting down.")
            self._log("Aborted active capture because the acquisition server is shutting down.", level="WARNING", event="capture_aborted")
            self._emit_update(update)
        for listener in (self._control_listener, self._artifact_listener):
            if listener is not None:
                listener.close()
        with self._control_lock:
            connection = self._control_connection
            self._control_connection = None
        if connection is not None:
            connection.close()
        for thread in self._threads:
            thread.join(timeout=2.0)
        self._threads.clear()
        self._log("Acquisition server stopped.", event="acquisition_server_stopped")

    def _listener(self, port: int) -> socket.socket:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((self.config.host, port))
        listener.listen()
        listener.settimeout(0.1)
        return listener

    def _accept_control(self) -> None:
        assert self._control_listener is not None
        while not self._stop.is_set():
            try:
                connection, _address = self._control_listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            with self._control_lock:
                active = self._control_connection
                if active is None:
                    self._control_connection = connection
                    self._last_controller_activity = time.monotonic()
                    accepted = True
                else:
                    accepted = False
            if not accepted:
                self._log("Rejected an additional control connection because another controller is already connected.", level="WARNING", event="control_connection_rejected")
                try:
                    send_frame(connection, {"protocol_version": PROTOCOL_VERSION, "type": "error", "error": "A controller is already connected."})
                finally:
                    connection.close()
                continue
            self._log(f"Accepted control connection from {_address}.", event="control_connection_accepted", peer=str(_address))
            thread = threading.Thread(target=self._serve_control, args=(connection,), name="euv-control-client", daemon=True)
            thread.start()

    def _serve_control(self, connection: socket.socket) -> None:
        try:
            while not self._stop.is_set():
                message = receive_frame(connection)
                with self._control_lock:
                    if self._control_connection is connection:
                        self._last_controller_activity = time.monotonic()
                if is_heartbeat(message):
                    self._send_control(heartbeat_message())
                    continue
                request_id, command, payload = validate_command_message(message)
                self._log(
                    f"Received control command {command} ({request_id}).",
                    event="control_command_received",
                    command=command,
                    request_id=str(request_id),
                    payload=payload,
                )
                try:
                    result = self._handle_command(command, payload)
                except Exception as exc:
                    error = f"{type(exc).__name__}: {exc}"
                    self._log(
                        f"Control command {command} ({request_id}) failed: {error}",
                        level="ERROR",
                        event="control_command_failed",
                        command=command,
                        request_id=str(request_id),
                        error=error,
                    )
                    self._send_control(response_message(request_id, ok=False, error=error))
                else:
                    self._log(
                        f"Control command {command} ({request_id}) completed.",
                        event="control_command_completed",
                        command=command,
                        request_id=str(request_id),
                        result=result,
                    )
                    self._send_control(response_message(request_id, ok=True, result=result))
        except (ConnectionError, OSError, ValueError) as exc:
            self._log(
                f"Control connection ended: {type(exc).__name__}: {exc}",
                level="WARNING",
                event="control_connection_ended",
            )
        finally:
            with self._control_lock:
                if self._control_connection is connection:
                    self._control_connection = None
            connection.close()
            self._log("Control connection closed.", event="control_connection_closed")

    def _accept_artifact(self) -> None:
        assert self._artifact_listener is not None
        while not self._stop.is_set():
            try:
                connection, _address = self._artifact_listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            thread = threading.Thread(target=self._serve_artifact, args=(connection,), name="euv-artifact-client", daemon=True)
            thread.start()

    def _serve_artifact(self, connection: socket.socket) -> None:
        try:
            request = receive_frame(connection)
            expected = {"protocol_version", "type", "snapshot_id"}
            if not isinstance(request, dict) or set(request) != expected:
                raise ValueError("Artifact request contains unknown or missing fields.")
            if request["protocol_version"] != PROTOCOL_VERSION or request["type"] != "artifact_request":
                raise ValueError("Unsupported artifact request type or protocol version.")
            snapshot_id = UUID(str(request["snapshot_id"]))
            manifest = self._find_snapshot(snapshot_id)
            self._log(
                f"Serving artifact {manifest.filename}.",
                event="artifact_transfer_started",
                snapshot_id=str(snapshot_id),
                filename=manifest.filename,
                byte_count=manifest.byte_count,
            )
            send_artifact(connection, manifest, self.engine.snapshot_store.path_for(manifest))
            self._log(
                f"Served artifact {manifest.filename}.",
                event="artifact_transfer_completed",
                snapshot_id=str(snapshot_id),
            )
        except (ConnectionError, OSError, ValueError) as exc:
            self._log(
                f"Artifact transfer failed: {type(exc).__name__}: {exc}",
                level="ERROR",
                event="artifact_transfer_failed",
            )
            try:
                send_frame(connection, {"protocol_version": PROTOCOL_VERSION, "type": "error", "error": f"{type(exc).__name__}: {exc}"})
            except OSError:
                pass
        finally:
            connection.close()

    def _capture_loop(self) -> None:
        while not self._stop.is_set():
            if self.engine.active:
                try:
                    with self._control_lock:
                        inactive_for = time.monotonic() - self._last_controller_activity
                    if inactive_for >= self.config.controller_watchdog_seconds:
                        self._log(
                            f"Aborting capture because the controlling client heartbeat was stale for {inactive_for:.3f}s.",
                            level="ERROR",
                            event="capture_watchdog_timeout",
                            inactive_seconds=inactive_for,
                        )
                        self._emit_update(self.engine.abort("Controlling client heartbeat timed out."))
                    else:
                        self._emit_update(self.engine.capture_once())
                except Exception as exc:
                    self._log(
                        f"Capture loop failed: {type(exc).__name__}: {exc}",
                        level="ERROR",
                        event="capture_loop_failed",
                    )
                    if self.engine.active:
                        try:
                            self._emit_update(self.engine.abort(f"Capture loop failure: {type(exc).__name__}: {exc}"))
                        except Exception as abort_exc:
                            self._log(
                                f"Capture-loop abort also failed: {type(abort_exc).__name__}: {abort_exc}",
                                level="ERROR",
                                event="capture_loop_abort_failed",
                            )
            self._stop.wait(self.config.capture_poll_seconds)

    def _handle_command(self, command: str, payload: dict[str, Any]) -> dict[str, Any]:
        if command == "start_capture":
            if "session_id" not in payload or set(payload) - {"session_id", "purpose"}:
                raise ValueError("start_capture requires session_id and accepts optional purpose.")
            purpose = CapturePurpose(str(payload.get("purpose", CapturePurpose.EXPERIMENT.value)))
            session_id = self.engine.start(UUID(str(payload["session_id"])), purpose)
            self._log(
                f"Started {purpose.value} capture session {session_id}.",
                event="capture_started",
                session_id=str(session_id),
                purpose=purpose.value,
            )
            return {
                "session_id": str(session_id),
                "state": CaptureSessionState.ACTIVE.value,
            } | self._status_value()
        if command == "stop_capture":
            allowed = {"reason"}
            if set(payload) - allowed:
                raise ValueError("stop_capture accepts only an optional reason.")
            reason = str(payload.get("reason", "Capture stop requested."))
            update = self.engine.stop(reason)
            self._log(
                f"Stopped capture session {self.engine.session_id}: {reason}",
                event="capture_stopped",
                reason=reason,
            )
            self._emit_update(update)
            return self._status_value()
        if command == "flush_snapshot":
            if payload:
                raise ValueError("flush_snapshot does not accept payload fields.")
            self._emit_update(self.engine.flush())
            self._log("Flushed the active capture snapshot.", event="capture_snapshot_flushed")
            return self._status_value()
        if command == "status":
            if payload:
                raise ValueError("status does not accept payload fields.")
            return self._status_value()
        if command == "list_snapshots":
            if payload:
                raise ValueError("list_snapshots does not accept payload fields.")
            manifest = self.engine.spool.load()
            return {"snapshots": [] if manifest is None else [item.to_dict() for item in manifest.snapshots]}
        if command == "acknowledge_snapshot":
            if set(payload) != {"snapshot_id"}:
                raise ValueError("acknowledge_snapshot requires only snapshot_id.")
            manifest = self.engine.spool.acknowledge(UUID(str(payload["snapshot_id"])))
            self._log(
                f"Acknowledged snapshot {payload['snapshot_id']} for session {manifest.session_id}.",
                event="snapshot_acknowledged",
                snapshot_id=str(payload["snapshot_id"]),
                session_id=str(manifest.session_id),
            )
            return {"session_id": str(manifest.session_id), "state": manifest.state.value}
        if command == "purge_snapshot":
            if set(payload) != {"snapshot_id"}:
                raise ValueError("purge_snapshot requires only snapshot_id.")
            snapshot_id = UUID(str(payload["snapshot_id"]))
            manifest = self.engine.spool.purge_snapshot(self.engine.snapshot_store, snapshot_id)
            self._log(
                f"Purged acknowledged snapshot {snapshot_id} for session {manifest.session_id}.",
                event="snapshot_purged",
                snapshot_id=str(snapshot_id),
                session_id=str(manifest.session_id),
            )
            return {"session_id": str(manifest.session_id), "purged": True}
        if command == "discard_diagnostic_session":
            if set(payload) != {"session_id"}:
                raise ValueError("discard_diagnostic_session requires only session_id.")
            session_id = UUID(str(payload["session_id"]))
            self.engine.spool.discard_diagnostic(self.engine.snapshot_store, session_id)
            self._log(
                f"Discarded diagnostic capture session {session_id}.",
                event="diagnostic_session_discarded",
                session_id=str(session_id),
            )
            return {"session_id": str(session_id), "discarded": True}
        if command == "release_snapshots":
            if payload:
                raise ValueError("release_snapshots does not accept payload fields.")
            self.engine.spool.release(self.engine.snapshot_store)
            self._log("Released acknowledged capture artifacts from the spool.", event="capture_artifacts_released")
            return {"released": True}
        if command == "set_simulator_control":
            if set(payload) != {"name", "enabled"}:
                raise ValueError("set_simulator_control requires only name and enabled.")
            if self._simulator_controls is None:
                raise RuntimeError("Simulator controls are not available for this acquisition source.")
            enabled = payload["enabled"]
            if not isinstance(enabled, bool):
                raise ValueError("Simulator control enabled value must be boolean.")
            self._simulator_controls.set_control(str(payload["name"]), enabled)
            return {"simulator": self._simulator_controls.status_value()}
        if command == "restore_simulator_controls":
            if payload:
                raise ValueError("restore_simulator_controls does not accept payload fields.")
            if self._simulator_controls is None:
                raise RuntimeError("Simulator controls are not available for this acquisition source.")
            self._simulator_controls.restore_nominal()
            return {"simulator": self._simulator_controls.status_value()}
        raise ValueError(f"Unknown acquisition command {command!r}.")

    def _find_snapshot(self, snapshot_id: UUID) -> SnapshotManifest:
        session = self.engine.spool.load()
        if session is None:
            raise ValueError("No capture session is available.")
        for stored in session.snapshots:
            if stored.manifest.snapshot_id == snapshot_id:
                return stored.manifest
        raise ValueError(f"Snapshot {snapshot_id} is not available.")

    def _status_value(self) -> dict[str, Any]:
        session = self.engine.spool.load()
        return {
            "server_boot_id": str(self.engine.spool.server_boot_id),
            "source_kind": self.engine.source_kind,
            "source_id": self.engine.source_id,
            "capabilities": {
                "capture_purpose": True,
                "purge_snapshot": True,
                "discard_diagnostic_session": True,
                "simulator_controls": self._simulator_controls is not None,
            },
            "simulator": None if self._simulator_controls is None else self._simulator_controls.status_value(),
            "capture_active": self.engine.active,
            "session": None if session is None else session.to_dict(),
        }

    def _emit_update(self, update) -> None:
        for manifest in update.closed_snapshots:
            self._log(
                f"Closed snapshot {manifest.filename} with {manifest.pulse_count} pulse(s).",
                event="snapshot_closed",
                snapshot_id=str(manifest.snapshot_id),
                session_id=str(manifest.session_id),
                pulse_count=manifest.pulse_count,
                close_reason=manifest.close_reason.value,
            )
            self._send_control(
                {
                    "protocol_version": PROTOCOL_VERSION,
                    "type": "snapshot_closed",
                    "manifest": manifest.to_dict(),
                }
            )
        if update.report is not None:
            self._send_control(
                {
                    "protocol_version": PROTOCOL_VERSION,
                    "type": "pulse_report",
                    "report": update.report.to_dict(),
                }
            )
        if update.stop_reason is not None:
            self._log(f"Capture stop notification: {update.stop_reason}", event="capture_stop_notification")
            self._send_control(
                {
                    "protocol_version": PROTOCOL_VERSION,
                    "type": "capture_stopped",
                    "reason": update.stop_reason,
                }
            )

    def _send_control(self, value: dict[str, Any]) -> None:
        with self._control_lock:
            connection = self._control_connection
        if connection is None:
            return
        try:
            with self._control_send_lock:
                send_frame(connection, value)
        except OSError:
            self._log(
                f"Failed to send control message of type {value.get('type')}.",
                level="ERROR",
                event="control_response_send_failed",
                message_type=value.get("type"),
            )
            with self._control_lock:
                if self._control_connection is connection:
                    self._control_connection = None

    def _log(self, message: str, *, level: str = "INFO", event: str | None = None, **data: Any) -> None:
        print(f"[EUV Acquisition Service] {message}", flush=True)
        if self._logger is not None:
            self._logger.log(
                message,
                level=level,
                l_type="ACQ",
                subsystem="EUV Acquisition Service",
                event=event,
                **data,
            )


class AcquisitionClient:
    def __init__(
        self,
        control_address: tuple[str, int],
        artifact_address: tuple[str, int],
        *,
        timeout_seconds: float = 5.0,
    ) -> None:
        self.control_address = control_address
        self.artifact_address = artifact_address
        self.timeout_seconds = timeout_seconds
        self._connection: socket.socket | None = None
        self._send_lock = threading.Lock()
        self._pending: dict[UUID, queue.Queue] = {}
        self._pending_lock = threading.Lock()
        self._reports: queue.Queue[dict[str, Any]] = queue.Queue()
        self._snapshots: queue.Queue[SnapshotManifest] = queue.Queue()
        self._stops: queue.Queue[str] = queue.Queue()
        self._closed = threading.Event()
        self._reader: threading.Thread | None = None
        self._last_heartbeat_monotonic = 0.0

    def connect(self) -> None:
        if self._connection is not None:
            raise RuntimeError("Acquisition client is already connected.")
        connection = socket.create_connection(self.control_address, timeout=self.timeout_seconds)
        connection.settimeout(None)
        self._connection = connection
        self._reader = threading.Thread(target=self._read_loop, name="euv-client-reader", daemon=True)
        self._reader.start()

    def close(self) -> None:
        self._closed.set()
        connection = self._connection
        self._connection = None
        if connection is not None:
            connection.close()
        if self._reader is not None:
            self._reader.join(timeout=2.0)

    def command(self, command: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        if self._connection is None:
            raise RuntimeError("Acquisition client is not connected.")
        request_id = uuid4()
        response_queue: queue.Queue = queue.Queue(maxsize=1)
        with self._pending_lock:
            self._pending[request_id] = response_queue
        try:
            with self._send_lock:
                send_frame(self._connection, command_message(request_id, command, payload))
            try:
                ok, result, error = response_queue.get(timeout=self.timeout_seconds)
            except queue.Empty as exc:
                raise TimeoutError(f"Timed out waiting for {command} response.") from exc
            if not ok:
                raise RuntimeError(error)
            return result
        finally:
            with self._pending_lock:
                self._pending.pop(request_id, None)

    def heartbeat(self) -> None:
        if self._connection is None:
            raise RuntimeError("Acquisition client is not connected.")
        with self._send_lock:
            send_frame(self._connection, heartbeat_message())

    def heartbeat_if_due(self, interval_seconds: float = 1.0) -> bool:
        if interval_seconds <= 0:
            raise ValueError("heartbeat interval must be positive.")
        now = time.monotonic()
        if now - self._last_heartbeat_monotonic < interval_seconds:
            return False
        self.heartbeat()
        self._last_heartbeat_monotonic = now
        return True

    def get_report(self, timeout: float | None = None) -> dict[str, Any]:
        return self._reports.get(timeout=timeout)

    def get_snapshot(self, timeout: float | None = None) -> SnapshotManifest:
        return self._snapshots.get(timeout=timeout)

    def get_stop_reason(self, timeout: float | None = None) -> str:
        return self._stops.get(timeout=timeout)

    def fetch_snapshot(self, snapshot_id: UUID, destination: str | Path) -> SnapshotManifest:
        with socket.create_connection(self.artifact_address, timeout=self.timeout_seconds) as connection:
            send_frame(
                connection,
                {
                    "protocol_version": PROTOCOL_VERSION,
                    "type": "artifact_request",
                    "snapshot_id": str(snapshot_id),
                },
            )
            return receive_artifact(connection, destination)

    def _read_loop(self) -> None:
        connection = self._connection
        if connection is None:
            return
        try:
            while not self._closed.is_set():
                message = receive_frame(connection)
                if is_heartbeat(message):
                    continue
                message_type = message.get("type") if isinstance(message, dict) else None
                if message_type == "response":
                    request_id, ok, result, error = validate_response_message(message)
                    with self._pending_lock:
                        waiting = self._pending.get(request_id)
                    if waiting is not None:
                        waiting.put((ok, result, error))
                elif message_type == "pulse_report":
                    report = message.get("report")
                    if isinstance(report, dict):
                        self._reports.put(report)
                elif message_type == "snapshot_closed":
                    self._snapshots.put(SnapshotManifest.from_dict(message.get("manifest")))
                elif message_type == "capture_stopped":
                    reason = message.get("reason")
                    if isinstance(reason, str):
                        self._stops.put(reason)
        except (ConnectionError, OSError, ValueError):
            pass