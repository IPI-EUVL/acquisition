from euv_acquisition.red_pitaya_service import _build_server, _parse_args
from euv_acquisition.sources.red_pitaya import RedPitayaPulseSource


def test_red_pitaya_service_builds_hardware_server_without_opening_source(tmp_path) -> None:
    logger = object()
    args = _parse_args(
        [
            "--spool",
            str(tmp_path),
            "--host",
            "0.0.0.0",
            "--source-id",
            "rp-test",
            "--snapshot-pulse-limit",
            "100",
        ]
    )

    server = _build_server(args, logger=logger)

    assert isinstance(server.engine.source, RedPitayaPulseSource)
    assert server.engine.source.state == "stopped"
    assert server.engine.source.capture_config.window_samples == 1250
    assert server.engine.source.capture_config.pretrigger_samples == 125
    assert server.engine.snapshot_store.root == tmp_path
    assert server.engine.spool.root == tmp_path
    assert server.engine.source_kind == "red_pitaya"
    assert server.engine.source_id == "rp-test"
    assert server.engine.rotation.pulse_limit == 100
    assert server.config.host == "0.0.0.0"
    assert server.config.control_port == 11760
    assert server.config.artifact_port == 11761
    assert server._logger is logger
    assert args.log_host == "10.11.13.1"
    assert args.log_port == 11751