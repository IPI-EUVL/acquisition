from __future__ import annotations

import sys
import uuid

from euv_acquisition.simulator_service import DEFAULT_LASER_SUBSYSTEM_UUID, _parse_args


def test_simulator_defaults_to_the_chamber_laser_timing_adapter(monkeypatch) -> None:
    monkeypatch.setenv("ECS_HOST", "dds.fixture")
    monkeypatch.setattr(sys, "argv", ["euv-acquisition-sim", "--spool", "C:/spool"])

    args = _parse_args()

    assert args.standalone_timing is False
    assert args.dds_host == "dds.fixture"
    assert uuid.UUID(args.laser_subsystem_uuid) == DEFAULT_LASER_SUBSYSTEM_UUID


def test_standalone_timing_requires_explicit_opt_out(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["euv-acquisition-sim", "--spool", "C:/spool", "--standalone-timing"],
    )

    assert _parse_args().standalone_timing is True