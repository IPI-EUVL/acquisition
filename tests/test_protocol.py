import socket
from pathlib import Path
from uuid import uuid4

import pytest

from euv_acquisition.analysis import analyze_pulse
from euv_acquisition.models import CaptureConfig, CapturedPulse, PulseRecord, SnapshotCloseReason
from euv_acquisition.protocol import (
    MAX_CONTROL_FRAME_BYTES,
    command_message,
    receive_artifact,
    receive_frame,
    response_message,
    send_artifact,
    send_frame,
    validate_command_message,
    validate_response_message,
)
from euv_acquisition.snapshot import SnapshotStore


def _snapshot(tmp_path: Path):
    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=4e-6, pretrigger_seconds=1e-6)
    samples = __import__("numpy").array([0.0, 0.2, 0.2, 0.0], dtype="float32")
    pulse = CapturedPulse(samples, 1, 1)
    record = PulseRecord(uuid4(), 0, pulse, analyze_pulse(samples, config))
    store = SnapshotStore(tmp_path / "source")
    manifest = store.write([record], config, SnapshotCloseReason.CAPTURE_STOP, source_kind="simulated", source_id="test")
    return store, manifest


def test_command_and_response_messages_round_trip_over_length_prefixed_socket() -> None:
    sender, receiver = socket.socketpair()
    request_id = uuid4()
    try:
        send_frame(sender, command_message(request_id, "start_capture", {"session_id": str(uuid4())}))
        sent_request_id, command, payload = validate_command_message(receive_frame(receiver))
        assert sent_request_id == request_id
        assert command == "start_capture"
        assert "session_id" in payload

        send_frame(receiver, response_message(request_id, ok=True, result={"accepted": True}))
        received_request_id, ok, result, error = validate_response_message(receive_frame(sender))
        assert received_request_id == request_id
        assert ok is True
        assert result == {"accepted": True}
        assert error is None
    finally:
        sender.close()
        receiver.close()


def test_receive_frame_rejects_oversized_input() -> None:
    sender, receiver = socket.socketpair()
    try:
        sender.sendall((MAX_CONTROL_FRAME_BYTES + 1).to_bytes(4, "big"))
        with pytest.raises(ValueError, match="maximum"):
            receive_frame(receiver)
    finally:
        sender.close()
        receiver.close()


def test_artifact_transfer_streams_and_verifies_hash(tmp_path) -> None:
    store, manifest = _snapshot(tmp_path)
    sender, receiver = socket.socketpair()
    try:
        send_artifact(sender, manifest, store.path_for(manifest))
        received = receive_artifact(receiver, tmp_path / "received")
    finally:
        sender.close()
        receiver.close()

    assert received == manifest
    assert (tmp_path / "received" / manifest.filename).read_bytes() == store.path_for(manifest).read_bytes()


def test_artifact_receiver_rejects_manifest_hash_mismatch(tmp_path) -> None:
    store, manifest = _snapshot(tmp_path)
    corrupted = manifest.to_dict()
    corrupted["sha256"] = "0" * 64
    bad_manifest = type(manifest).from_dict(corrupted)
    sender, receiver = socket.socketpair()
    try:
        send_artifact(sender, bad_manifest, store.path_for(manifest))
        with pytest.raises(ValueError, match="SHA-256"):
            receive_artifact(receiver, tmp_path / "received-corrupt")
    finally:
        sender.close()
        receiver.close()

    assert list((tmp_path / "received-corrupt").iterdir()) == []