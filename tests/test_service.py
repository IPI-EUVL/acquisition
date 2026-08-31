import time
from collections import deque
from uuid import uuid4

import numpy as np

from euv_acquisition.models import CaptureConfig, CapturedPulse
from euv_acquisition.service import AcquisitionClient, AcquisitionServer, ServiceConfig
from euv_acquisition.session import CaptureEngine, RotationConfig, SpoolRepository
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

    def open(self):
        self._open = True

    def capture(self):
        if not self._open or not self._timestamps:
            return None
        timestamp = self._timestamps.popleft()
        return CapturedPulse(np.array([0.0, 0.2, 0.2, 0.0], dtype=np.float32), timestamp, timestamp)

    def close(self):
        self._open = False


def _server(tmp_path, *, timestamps=(1, 2), watchdog=5.0, logger=None, simulator_controls=None):
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
        ServiceConfig(control_port=0, artifact_port=0, capture_poll_seconds=0.001, controller_watchdog_seconds=watchdog),
        logger=logger,
        simulator_controls=simulator_controls,
    )
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
        }
        assert status["simulator"] is None
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