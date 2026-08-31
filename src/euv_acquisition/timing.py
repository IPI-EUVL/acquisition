from __future__ import annotations

import json
import math
from dataclasses import dataclass


TIMING_STATUS_SCHEMA_VERSION = 2


@dataclass(frozen=True)
class LaserTimingState:
    laser_on: bool
    laser_warming_up: bool
    chopper_on: bool
    chopper_starting_up: bool
    current_phase: float
    preinit_phase: float
    configured_target_phase: float
    chopper_frequency_hz: float | None
    sampled_at_unix_ns: int | None = None
    sampled_at_monotonic_ns: int | None = None

    def __post_init__(self) -> None:
        for name in ("laser_on", "laser_warming_up", "chopper_on", "chopper_starting_up"):
            if not isinstance(getattr(self, name), bool):
                raise ValueError(f"{name} must be boolean.")
        for name in ("current_phase", "preinit_phase", "configured_target_phase"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
                raise ValueError(f"{name} must be finite.")
        if self.chopper_frequency_hz is not None:
            value = self.chopper_frequency_hz
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)) or value < 0:
                raise ValueError("chopper_frequency_hz must be non-negative and finite when present.")
        for name in ("sampled_at_unix_ns", "sampled_at_monotonic_ns"):
            value = getattr(self, name)
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
                raise ValueError(f"{name} must be a non-negative integer when present.")

    @property
    def triggers_enabled(self) -> bool:
        return (
            self.laser_on
            and self.chopper_on
            and not self.laser_warming_up
            and not self.chopper_starting_up
            and self.chopper_frequency_hz is not None
            and self.chopper_frequency_hz > 0
        )

    def euv_transmitting(self, phase_epsilon: float = 1e-2) -> bool:
        return (
            self.triggers_enabled
            and abs(self.configured_target_phase - self.preinit_phase) > phase_epsilon
            and abs(self.current_phase - self.configured_target_phase) <= phase_epsilon
        )

    @property
    def trigger_rate_hz(self) -> float | None:
        if not self.triggers_enabled:
            return None
        return self.chopper_frequency_hz / 2.0

    def to_dict(self) -> dict:
        return {
            "schema_version": TIMING_STATUS_SCHEMA_VERSION,
            "laser_on": self.laser_on,
            "laser_warming_up": self.laser_warming_up,
            "chopper_on": self.chopper_on,
            "chopper_starting_up": self.chopper_starting_up,
            "current_phase": self.current_phase,
            "preinit_phase": self.preinit_phase,
            "configured_target_phase": self.configured_target_phase,
            "chopper_frequency_hz": self.chopper_frequency_hz,
            "sampled_at_unix_ns": self.sampled_at_unix_ns,
            "sampled_at_monotonic_ns": self.sampled_at_monotonic_ns,
        }

    def encode(self) -> bytes:
        return json.dumps(self.to_dict(), allow_nan=False, separators=(",", ":")).encode("utf-8")

    @classmethod
    def decode(cls, payload: bytes) -> "LaserTimingState":
        try:
            value = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("Laser timing status must be UTF-8 JSON.") from exc
        expected_v1 = {
            "schema_version",
            "laser_on",
            "laser_warming_up",
            "chopper_on",
            "chopper_starting_up",
            "current_phase",
            "preinit_phase",
            "configured_target_phase",
            "chopper_frequency_hz",
        }
        expected_v2 = expected_v1 | {"sampled_at_unix_ns", "sampled_at_monotonic_ns"}
        if not isinstance(value, dict) or value.get("schema_version") not in (1, TIMING_STATUS_SCHEMA_VERSION):
            raise ValueError("Unsupported laser timing status schema.")
        if value["schema_version"] == 1 and set(value) != expected_v1:
            raise ValueError("Unsupported laser timing status schema.")
        if value["schema_version"] == TIMING_STATUS_SCHEMA_VERSION and set(value) != expected_v2:
            raise ValueError("Unsupported laser timing status schema.")
        return cls(
            laser_on=value["laser_on"],
            laser_warming_up=value["laser_warming_up"],
            chopper_on=value["chopper_on"],
            chopper_starting_up=value["chopper_starting_up"],
            current_phase=float(value["current_phase"]),
            preinit_phase=float(value["preinit_phase"]),
            configured_target_phase=float(value["configured_target_phase"]),
            chopper_frequency_hz=None if value["chopper_frequency_hz"] is None else float(value["chopper_frequency_hz"]),
            sampled_at_unix_ns=value.get("sampled_at_unix_ns"),
            sampled_at_monotonic_ns=value.get("sampled_at_monotonic_ns"),
        )