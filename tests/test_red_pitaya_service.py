from pathlib import Path

from euv_acquisition.red_pitaya_service import _build_server, _parse_args
from euv_acquisition.sources.isolated import IsolatedPulseSource


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
            "--capture-mode",
            "single-shot",
            "--capture-queue-capacity",
            "48",
            "--persistence-queue-capacity",
            "6",
        ]
    )

    server = _build_server(args, logger=logger)

    assert isinstance(server.engine.source, IsolatedPulseSource)
    assert server.engine.source.state == "stopped"
    assert server.engine.source.capture_config.window_samples == 1250
    assert server.engine.source.capture_config.pretrigger_samples == 125
    assert server.engine.source.requested_capture_mode == "single-shot"
    assert server.engine.source.process_config.cpu == 1
    assert server.engine.source.process_config.realtime_priority == 20
    assert server.engine.source.process_config.startup_timeout_seconds == 5.0
    assert server.engine.source.process_config.shutdown_timeout_seconds == 2.0
    assert server.engine.snapshot_store.root == tmp_path
    assert server.engine.spool.root == tmp_path
    assert server.engine.source_kind == "red_pitaya"
    assert server.engine.source_id == "rp-test"
    assert server.engine.rotation.pulse_limit == 100
    assert server.config.host == "0.0.0.0"
    assert server.config.control_port == 11760
    assert server.config.artifact_port == 11761
    assert server.config.capture_queue_capacity == 48
    assert server.config.persistence_queue_capacity == 6
    assert server._logger is logger
    assert args.log_host == "10.11.13.1"
    assert args.log_port == 11751


def test_production_unit_pins_legacy_mode_and_pipeline_capacity_budgets() -> None:
    unit = (Path(__file__).parents[1] / "deploy" / "euv-acquisition.service").read_text()

    assert 'Environment="EUV_CAPTURE_MODE=legacy-single-shot"' in unit
    assert "--capture-queue-capacity 32" in unit
    assert "--capture-cpu 1 --capture-realtime-priority 20" in unit
    assert "--capture-process-startup-timeout-seconds 5" in unit
    assert "--capture-process-shutdown-timeout-seconds 2" in unit
    assert "--persistence-queue-capacity 8" in unit
    assert "--control-queue-capacity 512" in unit
    assert "--pipeline-drain-timeout-seconds 10" in unit
    assert "CPUAffinity=0" in unit
    assert "LimitRTPRIO=20" in unit
    assert "RestrictRealtime=no" in unit
    assert "KillMode=mixed" in unit
    assert "TimeoutStopSec=30s" in unit


def test_deployment_rollback_restores_previous_unit_before_restart() -> None:
    script = (Path(__file__).parents[1] / "scripts" / "deploy_red_pitaya.ps1").read_text()

    restore_unit = script.index('install -m 0644 "$previous_unit"')
    restore_release = script.index('ln -s "$previous_release" "$rollback_link"')
    restart_service = script.index('systemctl restart "$unit_name"', restore_release)

    assert restore_unit < restore_release < restart_service