from __future__ import annotations

import argparse
import os
import signal
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
from euv_acquisition.sources.siglent import SIGLENT_CAPTURE_MODE, SiglentPulseSource


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the EUV acquisition service for a Siglent oscilloscope.")
    parser.add_argument("--spool", required=True, help="Dedicated local Siglent HDF5 spool directory.")
    parser.add_argument("--visa-resource", required=True, help="PyVISA resource for the Siglent oscilloscope.")
    parser.add_argument("--source-id", required=True, help="Stable identity for this physical oscilloscope.")
    parser.add_argument("--sample-rate-hz", required=True, type=float, help="Expected effective exported sample rate.")
    parser.add_argument("--points-per-frame", required=True, type=int, help="Expected exported points per frame.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--control-port", type=int, default=11762)
    parser.add_argument("--artifact-port", type=int, default=11763)
    parser.add_argument("--controller-watchdog-seconds", type=float, default=5.0)
    parser.add_argument("--capture-poll-seconds", type=float, default=0.001)
    parser.add_argument("--capture-queue-capacity", type=int, default=4)
    parser.add_argument("--persistence-queue-capacity", type=int, default=4)
    parser.add_argument("--control-queue-capacity", type=int, default=512)
    parser.add_argument("--pipeline-drain-timeout-seconds", type=float, default=20.0)
    parser.add_argument("--capture-process-startup-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--capture-process-shutdown-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--sequence-count", type=int, default=250)
    parser.add_argument("--waveform-interval", type=int, default=10)
    parser.add_argument("--trigger-poll-seconds", type=float, default=0.02)
    parser.add_argument("--visa-timeout-milliseconds", type=int, default=10_000)
    parser.add_argument("--input-full-scale-volts", type=float, default=1.0)
    parser.add_argument("--clipping-fraction", type=float, default=0.99)
    parser.add_argument("--log-host", default=os.environ.get("ECS_LOG_HOST", "10.11.13.1"))
    parser.add_argument("--log-port", type=int, default=int(os.environ.get("ECS_LOG_PORT", "11751")))
    parser.add_argument("--no-ecs-log", action="store_true")
    return parser.parse_args(argv)


def _build_server(args: argparse.Namespace, *, logger=None) -> AcquisitionServer:
    if args.points_per_frame <= 25:
        raise ValueError("Siglent points per frame must exceed the 25-sample baseline window.")
    capture_config = CaptureConfig(
        sample_rate_hz=args.sample_rate_hz,
        window_seconds=args.points_per_frame / args.sample_rate_hz,
        pretrigger_seconds=25 / args.sample_rate_hz,
        input_full_scale_volts=args.input_full_scale_volts,
        clipping_fraction=args.clipping_fraction,
    )
    source = IsolatedPulseSource(
        partial(
            SiglentPulseSource,
            capture_config,
            resource_name=args.visa_resource,
            sequence_count=args.sequence_count,
            waveform_interval=args.waveform_interval,
            trigger_poll_seconds=args.trigger_poll_seconds,
            timeout_milliseconds=args.visa_timeout_milliseconds,
        ),
        capture_config,
        requested_capture_mode=SIGLENT_CAPTURE_MODE,
        process_config=CaptureProcessConfig(
            cpu=None,
            realtime_priority=None,
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
        source_kind="siglent",
        source_id=args.source_id,
        rotation=RotationConfig(pulse_limit=args.sequence_count),
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
        origin_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, f"ipi-euv-acquisition:siglent:{args.source_id}")
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