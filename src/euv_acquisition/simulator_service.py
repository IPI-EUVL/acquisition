from __future__ import annotations

import argparse
import os
import signal
import threading
import time
import uuid

from euv_acquisition.ecs_logging import open_ecs_logger
from euv_acquisition.service import AcquisitionServer, ServiceConfig
from euv_acquisition.session import CaptureEngine, RotationConfig, SpoolRepository
from euv_acquisition.snapshot import SnapshotStore
from euv_acquisition.simulator_controls import SimulatorFaultControls
from euv_acquisition.sources.simulated import SimulatedPulseConfig, SimulatedPulseSource


DEFAULT_LASER_SUBSYSTEM_UUID = uuid.uuid3(uuid.NAMESPACE_OID, "Laser Sync Controller")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the standalone simulated EUV digitizer service.")
    parser.add_argument("--spool", required=True, help="Local directory for retained HDF5 snapshots and session metadata.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--control-port", type=int, default=11760)
    parser.add_argument("--artifact-port", type=int, default=11761)
    parser.add_argument("--trigger-rate-hz", type=float, default=96.0)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--dds-host", default=os.environ.get("ECS_HOST", "127.0.0.1"))
    parser.add_argument("--laser-subsystem-uuid", default=str(DEFAULT_LASER_SUBSYSTEM_UUID))
    parser.add_argument(
        "--standalone-timing",
        action="store_true",
        help="Ignore chamber DDS timing and continuously simulate a nominal transmitting laser.",
    )
    parser.add_argument("--log-host", default="127.0.0.1")
    parser.add_argument("--log-port", type=int, default=11751)
    parser.add_argument("--no-ecs-log", action="store_true")
    parser.add_argument("--control-gui", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logger = None
    logger_socket = None
    if not args.no_ecs_log:
        logger, logger_socket = open_ecs_logger(args.log_host, args.log_port)
    if logger is not None:
        logger.log(
            "Starting simulated EUV acquisition service.",
            level="INFO",
            l_type="ACQ",
            subsystem="EUV Acquisition Service",
            event="simulator_starting",
            spool=args.spool,
            host=args.host,
            control_port=args.control_port,
            artifact_port=args.artifact_port,
            trigger_rate_hz=args.trigger_rate_hz,
            seed=args.seed,
            timing_mode="standalone" if args.standalone_timing else "dds",
            dds_host=None if args.standalone_timing else args.dds_host,
            laser_subsystem_uuid=None if args.standalone_timing else args.laser_subsystem_uuid,
        )
    adapter = None
    upstream_options = {
        "upstream_trigger_rate_hz": lambda: args.trigger_rate_hz,
    }
    timing_mode = "standalone"
    if not args.standalone_timing:
        from uuid import UUID

        from euv_acquisition.simulator_dds import DdsLaserTimingAdapter

        adapter = DdsLaserTimingAdapter(UUID(args.laser_subsystem_uuid), host=args.dds_host)
        adapter.start()
        timing_mode = "dds"
        upstream_options = {
            "upstream_triggers_enabled": adapter.trigger_enabled,
            "upstream_euv_transmitting": adapter.euv_transmitting,
            "upstream_trigger_rate_hz": adapter.trigger_rate_hz,
        }
    controls = SimulatorFaultControls(**upstream_options)
    source = SimulatedPulseSource(
        pulse_config=SimulatedPulseConfig(seed=args.seed, trigger_rate_hz=args.trigger_rate_hz),
        trigger_enabled=controls.trigger_enabled,
        euv_transmitting=controls.euv_transmitting,
        trigger_rate_hz=controls.trigger_rate_hz,
    )
    snapshot_store = SnapshotStore(args.spool)
    spool = SpoolRepository(args.spool)
    engine = CaptureEngine(
        source,
        snapshot_store,
        spool,
        source_kind="simulated",
        source_id=f"{timing_mode}-timing-analytic-seed-{args.seed}",
        rotation=RotationConfig(),
    )
    server = AcquisitionServer(
        engine,
        ServiceConfig(host=args.host, control_port=args.control_port, artifact_port=args.artifact_port),
        logger=logger,
    )
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda _signum, _frame: stop_event.set())
    signal.signal(signal.SIGTERM, lambda _signum, _frame: stop_event.set())
    server.start()
    try:
        if args.control_gui:
            from euv_acquisition.simulator_gui import SimulatorControlWindow

            SimulatorControlWindow(controls, stop_event).run()
        else:
            while not stop_event.wait(0.5):
                pass
    finally:
        server.close()
        if adapter is not None:
            adapter.close()
        if logger is not None:
            logger.log(
                "Stopped simulated EUV acquisition service.",
                level="INFO",
                l_type="ACQ",
                subsystem="EUV Acquisition Service",
                event="simulator_stopped",
            )
        if logger_socket is not None:
            logger_socket.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())