from uuid import uuid4

from euv_acquisition.service import AcquisitionClient
from euv_acquisition.siglent_simulator_service import _build_server, _parse_args
from euv_acquisition.snapshot import read_snapshot
from euv_acquisition.sources.isolated import IsolatedPulseSource


def _args(tmp_path, *extra: str):
    return _parse_args(["--spool", str(tmp_path), *extra])


def test_siglent_simulator_service_uses_physical_geometry_and_ports_by_default(tmp_path) -> None:
    server = _build_server(_args(tmp_path))

    assert isinstance(server.engine.source, IsolatedPulseSource)
    assert server.engine.source.capture_config.sample_rate_hz == 100_000_000.0
    assert server.engine.source.capture_config.window_samples == 1000
    assert server.engine.source.capture_config.pretrigger_samples == 25
    assert server.engine.source.requested_capture_mode == "siglent-sequence"
    assert server.engine.source_kind == "siglent"
    assert server.engine.source_id == "siglent-simulator"
    assert server.config.control_port == 11762
    assert server.config.artifact_port == 11763


def test_siglent_simulator_service_transfers_one_atomic_sequence_snapshot(tmp_path) -> None:
    server = _build_server(
        _args(
            tmp_path / "spool",
            "--sample-rate-hz",
            "1000000",
            "--points-per-frame",
            "30",
            "--sequence-count",
            "2",
            "--trigger-rate-hz",
            "20",
            "--control-port",
            "0",
            "--artifact-port",
            "0",
            "--no-ecs-log",
        )
    )
    server.start()
    client = AcquisitionClient(server.control_address, server.artifact_address)
    client.connect()
    try:
        session_id = uuid4()
        started = client.command("start_capture", {"session_id": str(session_id)})
        reports = [client.get_report(timeout=5.0), client.get_report(timeout=5.0)]
        snapshot = client.get_snapshot(timeout=5.0)
        fetched = client.fetch_snapshot(snapshot.snapshot_id, tmp_path / "fetched")
        contents = read_snapshot(tmp_path / "fetched" / fetched.filename)
        client.command("acknowledge_snapshot", {"snapshot_id": str(snapshot.snapshot_id)})
        client.command("stop_capture", {"reason": "simulator test complete"})

        assert started["session"]["source_kind"] == "siglent"
        assert started["session"]["source_id"] == "siglent-simulator"
        assert [report["sequence"] for report in reports] == [0, 1]
        assert contents.source_batch is not None
        assert contents.source_batch.batch_kind == "siglent_sequence"
        assert fetched.pulse_count == 2
    finally:
        client.close()
        server.close()