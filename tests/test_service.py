import time
import threading
from collections import deque
from uuid import uuid4

import numpy as np

import euv_acquisition.service as service_module
from euv_acquisition.models import CaptureConfig, CapturedPulse
from euv_acquisition.service import AcquisitionClient, AcquisitionServer, ServiceConfig
from euv_acquisition.session import CaptureEngine, CaptureSessionState, RotationConfig, SpoolRepository
from euv_acquisition.simulator_controls import SimulatorFaultControls
from euv_acquisition.snapshot import SnapshotStore


class _Logger:
    def __init__(self) -> None:
        self.records = []

    def log(self, message, **kwargs) -> None:
        self.records.append((message, kwargs))


class QueuePulseSource:
    def __init__(self, timestamps):
        self.capture_config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=4e-6, pretrigger_seconds=1e-6)
        self._timestamps = deque(timestamps)
        self._open = False
        self.captured_count = 0

    def open(self):
        self._open = True

    def capture(self):
        if not self._open or not self._timestamps:
            return None
        timestamp = self._timestamps.popleft()
        self.captured_count += 1
        return CapturedPulse(np.array([0.0, 0.2, 0.2, 0.0], dtype=np.float32), timestamp, timestamp)

    def close(self):
        self._open = False


def _server(
    tmp_path,
    *,
    timestamps=(1, 2),
    watchdog=5.0,
    logger=None,
    simulator_controls=None,
    control_queue_capacity=512,
    start=True,
):
    source = QueuePulseSource(timestamps)
    store = SnapshotStore(tmp_path / "spool")
    spool = SpoolRepository(tmp_path / "spool", server_boot_id=uuid4())
    engine = CaptureEngine(
        source,
        store,
        spool,
        source_kind="simulated",
        source_id="test",
        rotation=RotationConfig(pulse_limit=2, trigger_idle_seconds=100.0),
    )
    server = AcquisitionServer(
        engine,
        ServiceConfig(
            control_port=0,
            artifact_port=0,
            capture_poll_seconds=0.001,
            controller_watchdog_seconds=watchdog,
            control_queue_capacity=control_queue_capacity,
        ),
        logger=logger,
        simulator_controls=simulator_controls,
    )
    if start:
        server.start()
    return server


def test_service_streams_reports_transfers_artifact_and_releases_spool(tmp_path) -> None:
    server = _server(tmp_path)
    client = AcquisitionClient(server.control_address, server.artifact_address)
    client.connect()
    try:
        session_id = uuid4()
        started = client.command("start_capture", {"session_id": str(session_id)})
        first = client.get_report(timeout=1.0)
        second = client.get_report(timeout=1.0)
        snapshot = client.get_snapshot(timeout=1.0)
        fetched = client.fetch_snapshot(snapshot.snapshot_id, tmp_path / "imported")
        client.command("acknowledge_snapshot", {"snapshot_id": str(snapshot.snapshot_id)})
        stopped = client.command("stop_capture", {"reason": "test complete"})
        client.command("release_snapshots")

        assert started["session_id"] == str(session_id)
        assert started["session"]["source_kind"] == "simulated"
        assert started["session"]["source_id"] == "test"
        assert [first["sequence"], second["sequence"]] == [0, 1]
        assert fetched == snapshot
        assert stopped["session"]["final_sequence"] == 1
        assert server.engine.spool.load() is None
    finally:
        client.close()
        server.close()


def test_slow_report_send_does_not_stall_capture(tmp_path, monkeypatch) -> None:
    server = _server(tmp_path, timestamps=())
    client = AcquisitionClient(server.control_address, server.artifact_address)
    client.connect()
    send_started = threading.Event()
    release_send = threading.Event()
    real_send_frame = service_module.send_frame

    def slow_send_frame(connection, value):
        if value.get("type") == "pulse_report" and not send_started.is_set():
            send_started.set()
            release_send.wait(timeout=1.0)
        real_send_frame(connection, value)

    monkeypatch.setattr(service_module, "send_frame", slow_send_frame)
    try:
        client.command("start_capture", {"session_id": str(uuid4())})
        source = server.engine.source
        source._timestamps.extend(range(1, 9))

        assert send_started.wait(timeout=1.0)
        deadline = time.monotonic() + 0.5
        while source.captured_count < 8 and time.monotonic() < deadline:
            time.sleep(0.005)

        assert source.captured_count == 8
        release_send.set()
        reports = [client.get_report(timeout=1.0) for _ in range(8)]
        assert [report["sequence"] for report in reports] == list(range(8))
        client.command("stop_capture")
    finally:
        release_send.set()
        client.close()
        server.close()


def test_control_queue_overflow_aborts_intake_and_drains_accepted_pulses(
    tmp_path,
    monkeypatch,
) -> None:
    logger = _Logger()
    server = _server(
        tmp_path,
        timestamps=(),
        logger=logger,
        control_queue_capacity=1,
    )
    client = AcquisitionClient(server.control_address, server.artifact_address)
    client.connect()
    send_started = threading.Event()
    release_send = threading.Event()
    real_send_frame = service_module.send_frame

    def blocking_send_frame(connection, value):
        if (
            threading.current_thread().name == "euv-control-writer"
            and service_module.is_heartbeat(value)
            and not send_started.is_set()
        ):
            send_started.set()
            release_send.wait(timeout=1.0)
        real_send_frame(connection, value)

    try:
        client.command("start_capture", {"session_id": str(uuid4())})
        monkeypatch.setattr(service_module, "send_frame", blocking_send_frame)
        client.heartbeat()
        assert send_started.wait(timeout=1.0)
        server.engine.source._timestamps.extend(range(1, 9))
        deadline = time.monotonic() + 2.0
        while server.pipeline.active and time.monotonic() < deadline:
            time.sleep(0.005)

        assert server.pipeline.active is False
        metrics = server.engine.metrics.snapshot()
        accepted = metrics["counters"]["accepted"]
        assert metrics["counters"]["control_queue_overflow"] == 1
        assert metrics["counters"]["persisted"] == accepted
        assert metrics["terminal_error"].startswith("Control transport failed: RuntimeError:")
        session = server.engine.spool.load()
        assert session.state is CaptureSessionState.STOPPED
        assert session.final_sequence == accepted - 1
        failures = [record for record in logger.records if record[1].get("event") == "control_response_send_failed"]
        assert len(failures) == 1
    finally:
        release_send.set()
        client.close()
        server.close()


def test_control_send_failure_aborts_intake_and_drains_accepted_pulses(
    tmp_path,
    monkeypatch,
) -> None:
    logger = _Logger()
    server = _server(tmp_path, timestamps=(), logger=logger)
    client = AcquisitionClient(server.control_address, server.artifact_address)
    client.connect()
    real_send_frame = service_module.send_frame

    def failing_send_frame(connection, value):
        if (
            threading.current_thread().name == "euv-control-writer"
            and value.get("type") == "pulse_report"
        ):
            raise OSError("send fixture failed")
        real_send_frame(connection, value)

    try:
        client.command("start_capture", {"session_id": str(uuid4())})
        monkeypatch.setattr(service_module, "send_frame", failing_send_frame)
        server.engine.source._timestamps.extend(range(1, 9))

        deadline = time.monotonic() + 2.0
        while server.pipeline.active and time.monotonic() < deadline:
            time.sleep(0.005)

        assert server.pipeline.active is False
        metrics = server.engine.metrics.snapshot()
        accepted = metrics["counters"]["accepted"]
        assert metrics["counters"]["persisted"] == accepted
        assert metrics["terminal_error"] == "Control transport failed: OSError: send fixture failed"
        session = server.engine.spool.load()
        assert session.state is CaptureSessionState.STOPPED
        assert session.final_sequence == accepted - 1
        failures = [record for record in logger.records if record[1].get("event") == "control_response_send_failed"]
        assert len(failures) == 1
    finally:
        client.close()
        server.close()


def test_control_writer_discards_frames_from_stale_connection_generations(
    tmp_path,
    monkeypatch,
) -> None:
    server = _server(tmp_path, timestamps=(), start=False)
    stale_connection = object()
    current_connection = object()
    server._control_connection = current_connection
    server._control_generation = 2
    server._control_outbox.put_nowait(
        service_module._QueuedControlFrame(
            stale_connection,
            1,
            {"type": "pulse_report", "report": {}},
            time.monotonic_ns(),
        )
    )
    sent = []
    monkeypatch.setattr(service_module, "send_frame", lambda connection, value: sent.append((connection, value)))

    server._stop.set()
    server._write_control()

    assert sent == []
    assert server._control_connection is current_connection
    assert server._control_generation == 2
    assert server._control_outbox.empty()


def test_service_watchdog_stops_capture_without_heartbeats(tmp_path) -> None:
    server = _server(tmp_path, timestamps=(1,), watchdog=0.05)
    client = AcquisitionClient(server.control_address, server.artifact_address)
    client.connect()
    try:
        client.command("start_capture", {"session_id": str(uuid4())})
        client.get_report(timeout=1.0)
        assert client.get_stop_reason(timeout=1.0) == "Controlling client heartbeat timed out."
        status = client.command("status")
        assert status["capture_active"] is False
        assert status["session"]["state"] == "stopped"
    finally:
        client.close()
        server.close()


def test_service_heartbeat_keeps_capture_alive(tmp_path) -> None:
    server = _server(tmp_path, timestamps=(), watchdog=0.05)
    client = AcquisitionClient(server.control_address, server.artifact_address)
    client.connect()
    try:
        client.command("start_capture", {"session_id": str(uuid4())})
        for _ in range(3):
            time.sleep(0.02)
            client.heartbeat()
        assert client.command("status")["capture_active"] is True
        client.command("stop_capture")
    finally:
        client.close()
        server.close()


def test_client_rate_limited_heartbeat_keeps_capture_alive(tmp_path) -> None:
    server = _server(tmp_path, timestamps=(), watchdog=0.05)
    client = AcquisitionClient(server.control_address, server.artifact_address)
    client.connect()
    try:
        client.command("start_capture", {"session_id": str(uuid4())})
        for _ in range(3):
            time.sleep(0.02)
            client.heartbeat_if_due(0.01)
        assert client.command("status")["capture_active"] is True
        client.command("stop_capture")
    finally:
        client.close()
        server.close()


def test_service_logs_control_command_lifecycle(tmp_path) -> None:
    logger = _Logger()
    server = _server(tmp_path, timestamps=(), logger=logger)
    client = AcquisitionClient(server.control_address, server.artifact_address)
    client.connect()
    try:
        client.command("start_capture", {"session_id": str(uuid4())})
        client.command("stop_capture", {"reason": "logging fixture"})
    finally:
        client.close()
        server.close()

    events = [record[1].get("event") for record in logger.records]
    assert "control_command_received" in events
    assert "capture_started" in events
    assert "capture_stopped" in events
    assert "control_command_completed" in events


def test_service_reports_capabilities_and_defaults_capture_to_experiment(tmp_path) -> None:
    server = _server(tmp_path, timestamps=())
    client = AcquisitionClient(server.control_address, server.artifact_address)
    client.connect()
    try:
        status = client.command("status")
        started = client.command("start_capture", {"session_id": str(uuid4())})

        assert status["source_kind"] == "simulated"
        assert status["source_id"] == "test"
        assert status["capabilities"] == {
            "capture_purpose": True,
            "purge_snapshot": True,
            "discard_diagnostic_session": True,
            "simulator_controls": False,
            "asynchronous_control_writer": True,
            "pipeline_metrics": True,
        }
        assert status["simulator"] is None
        assert status["pipeline_metrics"]["schema_version"] == 1
        assert status["pipeline_metrics"]["state"] == "idle"
        assert started["session"]["purpose"] == "experiment"
        client.command("stop_capture")
    finally:
        client.close()
        server.close()


def test_service_purges_and_discards_diagnostic_artifacts(tmp_path) -> None:
    server = _server(tmp_path, timestamps=(1,))
    client = AcquisitionClient(server.control_address, server.artifact_address)
    client.connect()
    try:
        session_id = uuid4()
        client.command("start_capture", {"session_id": str(session_id), "purpose": "diagnostic"})
        client.get_report(timeout=1.0)
        client.command("flush_snapshot")
        snapshot = client.get_snapshot(timeout=1.0)
        client.fetch_snapshot(snapshot.snapshot_id, tmp_path / "received")
        client.command("acknowledge_snapshot", {"snapshot_id": str(snapshot.snapshot_id)})
        client.command("purge_snapshot", {"snapshot_id": str(snapshot.snapshot_id)})
        client.command("stop_capture", {"reason": "diagnostic complete"})
        discarded = client.command("discard_diagnostic_session", {"session_id": str(session_id)})

        assert discarded["discarded"] is True
        assert server.engine.spool.load() is None
        assert not server.engine.snapshot_store.path_for(snapshot).exists()
    finally:
        client.close()
        server.close()


def test_service_routes_controls_only_to_simulator_provider(tmp_path) -> None:
    controls = SimulatorFaultControls()
    server = _server(tmp_path / "simulator", timestamps=(), simulator_controls=controls)
    client = AcquisitionClient(server.control_address, server.artifact_address)
    client.connect()
    try:
        status = client.command("status")
        changed = client.command("set_simulator_control", {"name": "pll_locked", "enabled": False})
        restored = client.command("restore_simulator_controls")

        assert status["capabilities"]["simulator_controls"] is True
        assert changed["simulator"]["pll_locked"] is False
        assert changed["simulator"]["effective_euv_transmitting"] is False
        assert restored["simulator"]["pll_locked"] is True
        with __import__("pytest").raises(RuntimeError, match="Unknown simulator control"):
            client.command("set_simulator_control", {"name": "physical_laser", "enabled": True})
        with __import__("pytest").raises(RuntimeError, match="boolean"):
            client.command("set_simulator_control", {"name": "pll_locked", "enabled": 1})
    finally:
        client.close()
        server.close()

    hardware = _server(tmp_path / "hardware", timestamps=())
    hardware_client = AcquisitionClient(hardware.control_address, hardware.artifact_address)
    hardware_client.connect()
    try:
        with __import__("pytest").raises(RuntimeError, match="not available"):
            hardware_client.command("set_simulator_control", {"name": "pll_locked", "enabled": False})
    finally:
        hardware_client.close()
        hardware.close()