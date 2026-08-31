from __future__ import annotations

import argparse
import os
import signal
import socket
import threading
import uuid
from collections.abc import Sequence
from functools import partial

from euv_acquisition.ecs_logging import open_ecs_logger
from euv_acquisition.models import CaptureConfig
from euv_acquisition.service import AcquisitionServer, ServiceConfig
from euv_acquisition.session import CaptureEngine, RotationConfig, SpoolRepository
from euv_acquisition.snapshot import SnapshotStore
from euv_acquisition.sources.isolated import CaptureProcessConfig, IsolatedPulseSource
from euv_acquisition.sources.red_pitaya import CaptureMode, RedPitayaPulseSource


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the EUV acquisition service on Red Pitaya hardware.")
    parser.add_argument("--spool", required=True, help="Local directory for retained HDF5 snapshots and session metadata.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--control-port", type=int, default=11760)
    parser.add_argument("--artifact-port", type=int, default=11761)
    parser.add_argument("--controller-watchdog-seconds", type=float, default=5.0)
    parser.add_argument("--capture-poll-seconds", type=float, default=0.001)
    parser.add_argument("--capture-queue-capacity", type=int, default=32)
    parser.add_argument("--capture-cpu", type=int, default=1)
    parser.add_argument("--capture-realtime-priority", type=int, default=20)
    parser.add_argument("--capture-process-startup-timeout-seconds", type=float, default=5.0)
    parser.add_argument("--capture-process-shutdown-timeout-seconds", type=float, default=2.0)
    parser.add_argument("--persistence-queue-capacity", type=int, default=8)
    parser.add_argument("--control-queue-capacity", type=int, default=512)
    parser.add_argument("--pipeline-drain-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--source-id", default=socket.gethostname())
    parser.add_argument("--sample-rate-hz", type=float, default=125_000_000.0)
    parser.add_argument("--window-microseconds", type=float, default=10.0)
    parser.add_argument("--pretrigger-microseconds", type=float, default=1.0)
    parser.add_argument("--input-full-scale-volts", type=float, default=1.0)
    parser.add_argument("--clipping-fraction", type=float, default=0.99)
    parser.add_argument("--prefill-seconds", type=float, default=0.001)
    parser.add_argument("--debounce-microseconds", type=float, default=1.0)
    parser.add_argument(
        "--capture-mode",
        choices=[mode.value for mode in CaptureMode],
        default=os.environ.get("EUV_CAPTURE_MODE", CaptureMode.LEGACY_SINGLE_SHOT.value),
    )
    parser.add_argument("--axi-minimum-buffer-seconds", type=float, default=0.05)
    parser.add_argument("--snapshot-pulse-limit", type=int, default=250)
    parser.add_argument("--snapshot-wall-seconds", type=float, default=5.0)
    parser.add_argument("--trigger-idle-seconds", type=float, default=0.5)
    parser.add_argument("--log-host", default=os.environ.get("ECS_LOG_HOST", "10.11.13.1"))
    parser.add_argument("--log-port", type=int, default=int(os.environ.get("ECS_LOG_PORT", "11751")))
    parser.add_argument("--no-ecs-log", action="store_true")
    return parser.parse_args(argv)


def _build_server(args: argparse.Namespace, *, logger=None) -> AcquisitionServer:
    capture_config = CaptureConfig(
        sample_rate_hz=args.sample_rate_hz,
        window_seconds=args.window_microseconds * 1e-6,
        pretrigger_seconds=args.pretrigger_microseconds * 1e-6,
        input_full_scale_volts=args.input_full_scale_volts,
        clipping_fraction=args.clipping_fraction,
    )
    source = IsolatedPulseSource(
        partial(
            RedPitayaPulseSource,
            capture_config,
            prefill_seconds=args.prefill_seconds,
            debounce_microseconds=args.debounce_microseconds,
            capture_mode=args.capture_mode,
            axi_minimum_buffer_seconds=args.axi_minimum_buffer_seconds,
        ),
        capture_config,
        requested_capture_mode=args.capture_mode,
        process_config=CaptureProcessConfig(
            cpu=args.capture_cpu,
            realtime_priority=args.capture_realtime_priority,
            poll_seconds=args.capture_poll_seconds,
            queue_capacity=args.capture_queue_capacity,
            startup_timeout_seconds=args.capture_process_startup_timeout_seconds,
            shutdown_timeout_seconds=args.capture_process_shutdown_timeout_seconds,
        ),
    )
    snapshot_store = SnapshotStore(args.spool)
    spool = SpoolRepository(args.spool)
    engine = CaptureEngine(
        source,
        snapshot_store,
        spool,
        source_kind="red_pitaya",
        source_id=args.source_id,
        rotation=RotationConfig(
            pulse_limit=args.snapshot_pulse_limit,
            wall_time_seconds=args.snapshot_wall_seconds,
            trigger_idle_seconds=args.trigger_idle_seconds,
        ),
    )
    return AcquisitionServer(
        engine,
        ServiceConfig(
            host=args.host,
            control_port=args.control_port,
            artifact_port=args.artifact_port,
            controller_watchdog_seconds=args.controller_watchdog_seconds,
            capture_poll_seconds=args.capture_poll_seconds,
            capture_queue_capacity=args.capture_queue_capacity,
            persistence_queue_capacity=args.persistence_queue_capacity,
            control_queue_capacity=args.control_queue_capacity,
            pipeline_drain_timeout_seconds=args.pipeline_drain_timeout_seconds,
        ),
        logger=logger,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    logger = None
    logger_transport = None
    if not args.no_ecs_log:
        origin_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, f"ipi-euv-acquisition:{args.source_id}")
        logger, logger_transport = open_ecs_logger(args.log_host, args.log_port, origin_uuid=origin_uuid)
    server = _build_server(args, logger=logger)
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda _signum, _frame: stop_event.set())
    signal.signal(signal.SIGTERM, lambda _signum, _frame: stop_event.set())
    server.start()
    try:
        while not stop_event.wait(0.5):
            pass
    finally:
        server.close()
        if logger_transport is not None:
            logger_transport.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())