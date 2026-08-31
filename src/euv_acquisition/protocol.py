from __future__ import annotations

import hashlib
import json
import os
import socket
from pathlib import Path
from typing import Any
from uuid import UUID

from euv_acquisition.snapshot import SnapshotManifest


PROTOCOL_VERSION = 1
MAX_CONTROL_FRAME_BYTES = 1_048_576
ARTIFACT_CHUNK_BYTES = 1024 * 1024


def _encode_json(value: dict[str, Any]) -> bytes:
    if not isinstance(value, dict):
        raise ValueError("Protocol messages must be JSON objects.")
    try:
        return json.dumps(value, allow_nan=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("Protocol message is not JSON serializable.") from exc


def _decode_json(payload: bytes) -> dict[str, Any]:
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Protocol frame is not valid UTF-8 JSON.") from exc
    if not isinstance(value, dict):
        raise ValueError("Protocol frame must contain a JSON object.")
    return value


def send_frame(connection: socket.socket, value: dict[str, Any]) -> None:
    payload = _encode_json(value)
    if len(payload) > MAX_CONTROL_FRAME_BYTES:
        raise ValueError("Protocol frame exceeds the configured maximum size.")
    connection.sendall(len(payload).to_bytes(4, byteorder="big") + payload)


def receive_exact(connection: socket.socket, byte_count: int) -> bytes:
    if byte_count < 0:
        raise ValueError("Byte count cannot be negative.")
    data = bytearray()
    while len(data) < byte_count:
        chunk = connection.recv(byte_count - len(data))
        if not chunk:
            raise ConnectionError("Peer disconnected before completing the requested payload.")
        data.extend(chunk)
    return bytes(data)


def receive_frame(connection: socket.socket) -> dict[str, Any]:
    payload_length = int.from_bytes(receive_exact(connection, 4), byteorder="big")
    if payload_length > MAX_CONTROL_FRAME_BYTES:
        raise ValueError("Incoming protocol frame exceeds the configured maximum size.")
    return _decode_json(receive_exact(connection, payload_length))


def command_message(request_id: UUID, command: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    if not isinstance(command, str) or not command.strip():
        raise ValueError("Command name cannot be empty.")
    if payload is not None and not isinstance(payload, dict):
        raise ValueError("Command payload must be an object.")
    return {
        "protocol_version": PROTOCOL_VERSION,
        "type": "command",
        "request_id": str(request_id),
        "command": command.strip(),
        "payload": payload or {},
    }


def validate_command_message(value: object) -> tuple[UUID, str, dict[str, Any]]:
    expected = {"protocol_version", "type", "request_id", "command", "payload"}
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError("Command message contains unknown or missing fields.")
    if value["protocol_version"] != PROTOCOL_VERSION or value["type"] != "command":
        raise ValueError("Unsupported command message type or protocol version.")
    request_id = UUID(str(value["request_id"]))
    command = value["command"]
    payload = value["payload"]
    if not isinstance(command, str) or not command.strip() or not isinstance(payload, dict):
        raise ValueError("Command message has invalid fields.")
    return request_id, command.strip(), payload


def response_message(request_id: UUID, *, ok: bool, result: dict[str, Any] | None = None, error: str | None = None) -> dict[str, Any]:
    if not isinstance(ok, bool):
        raise ValueError("Response ok field must be boolean.")
    if result is not None and not isinstance(result, dict):
        raise ValueError("Response result must be an object when present.")
    if error is not None and (not isinstance(error, str) or not error.strip()):
        raise ValueError("Response error must be non-empty text when present.")
    if ok == (error is not None):
        raise ValueError("Successful responses require no error; failed responses require one.")
    return {
        "protocol_version": PROTOCOL_VERSION,
        "type": "response",
        "request_id": str(request_id),
        "ok": ok,
        "result": result or {},
        "error": error,
    }


def validate_response_message(value: object) -> tuple[UUID, bool, dict[str, Any], str | None]:
    expected = {"protocol_version", "type", "request_id", "ok", "result", "error"}
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError("Response message contains unknown or missing fields.")
    if value["protocol_version"] != PROTOCOL_VERSION or value["type"] != "response":
        raise ValueError("Unsupported response message type or protocol version.")
    request_id = UUID(str(value["request_id"]))
    ok = value["ok"]
    result = value["result"]
    error = value["error"]
    if not isinstance(ok, bool) or not isinstance(result, dict):
        raise ValueError("Response message has invalid fields.")
    if ok and error is not None:
        raise ValueError("Successful response cannot include an error.")
    if not ok and (not isinstance(error, str) or not error.strip()):
        raise ValueError("Failed response must include a non-empty error.")
    return request_id, ok, result, error


def report_message(report: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(report, dict):
        raise ValueError("Pulse report must be an object.")
    return {
        "protocol_version": PROTOCOL_VERSION,
        "type": "pulse_report",
        "report": report,
    }


def validate_report_message(value: object) -> dict[str, Any]:
    expected = {"protocol_version", "type", "report"}
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError("Pulse report message contains unknown or missing fields.")
    if value["protocol_version"] != PROTOCOL_VERSION or value["type"] != "pulse_report":
        raise ValueError("Unsupported pulse report message type or protocol version.")
    if not isinstance(value["report"], dict):
        raise ValueError("Pulse report payload must be an object.")
    return value["report"]


def heartbeat_message() -> dict[str, Any]:
    return {"protocol_version": PROTOCOL_VERSION, "type": "heartbeat"}


def is_heartbeat(value: object) -> bool:
    return value == heartbeat_message()


def send_artifact(connection: socket.socket, manifest: SnapshotManifest, source: str | Path) -> None:
    path = Path(source)
    if not path.is_file():
        raise FileNotFoundError(path)
    if path.name != manifest.filename:
        raise ValueError("Artifact filename does not match its manifest.")
    with path.open("rb") as artifact:
        if os.fstat(artifact.fileno()).st_size != manifest.byte_count:
            raise ValueError("Artifact byte count does not match its manifest.")
        header = {
            "protocol_version": PROTOCOL_VERSION,
            "type": "artifact",
            "manifest": manifest.to_dict(),
        }
        send_frame(connection, header)
        sent = connection.sendfile(artifact, count=manifest.byte_count)
        if sent != manifest.byte_count:
            raise ConnectionError(f"Artifact transfer sent {sent} of {manifest.byte_count} bytes.")


def receive_artifact(connection: socket.socket, destination_directory: str | Path) -> SnapshotManifest:
    header = receive_frame(connection)
    expected = {"protocol_version", "type", "manifest"}
    if not isinstance(header, dict) or set(header) != expected:
        raise ValueError("Artifact header contains unknown or missing fields.")
    if header["protocol_version"] != PROTOCOL_VERSION or header["type"] != "artifact":
        raise ValueError("Unsupported artifact header type or protocol version.")
    manifest = SnapshotManifest.from_dict(header["manifest"])
    destination = Path(destination_directory)
    destination.mkdir(parents=True, exist_ok=True)
    output = destination / manifest.filename
    temporary = destination / f".{manifest.filename}.{UUID(str(manifest.snapshot_id)).hex}.tmp"
    digest = hashlib.sha256()
    remaining = manifest.byte_count
    try:
        with temporary.open("xb") as artifact:
            while remaining:
                chunk = receive_exact(connection, min(ARTIFACT_CHUNK_BYTES, remaining))
                artifact.write(chunk)
                digest.update(chunk)
                remaining -= len(chunk)
            artifact.flush()
            os.fsync(artifact.fileno())
        if digest.hexdigest() != manifest.sha256:
            raise ValueError("Transferred artifact SHA-256 does not match its manifest.")
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    return manifest