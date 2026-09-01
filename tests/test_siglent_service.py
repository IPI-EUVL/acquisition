from euv_acquisition.siglent_service import _build_server, _parse_args
from euv_acquisition.sources.isolated import IsolatedPulseSource


def test_siglent_service_builds_isolated_windows_server_without_opening_visa(tmp_path) -> None:
    logger = object()
    args = _parse_args(
        [
            "--spool",
            str(tmp_path),
            "--visa-resource",
            "TCPIP0::10.11.13.220::5025::SOCKET",
            "--source-id",
            "siglent-test",
            "--sample-rate-hz",
            "200000000",
            "--points-per-frame",
            "2000",
            "--capture-queue-capacity",
            "3",
        ]
    )

    server = _build_server(args, logger=logger)

    assert isinstance(server.engine.source, IsolatedPulseSource)
    assert server.engine.source.state == "stopped"
    assert server.engine.source.capture_config.sample_rate_hz == 200_000_000.0
    assert server.engine.source.capture_config.window_samples == 2000
    assert server.engine.source.capture_config.pretrigger_samples == 25
    assert server.engine.source.process_config.cpu is None
    assert server.engine.source.process_config.realtime_priority is None
    assert server.engine.source.requested_capture_mode == "siglent-sequence"
    assert server.engine.source_kind == "siglent"
    assert server.engine.source_id == "siglent-test"
    assert server.engine.snapshot_store.root == tmp_path
    assert server.config.control_port == 11762
    assert server.config.artifact_port == 11763
    assert server.config.capture_queue_capacity == 3
    assert server._logger is logger