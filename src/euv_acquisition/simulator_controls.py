from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class SimulatedLaserStatus:
    upstream_triggers_enabled: bool
    upstream_euv_transmitting: bool
    laser_enabled: bool
    chopper_enabled: bool
    pll_locked: bool
    effective_triggers_enabled: bool
    effective_euv_transmitting: bool
    trigger_rate_hz: float | None


class SimulatorFaultControls:
    """Combine optional DDS timing with local controls for simulator fault injection."""

    def __init__(
        self,
        *,
        upstream_triggers_enabled: Callable[[], bool] = lambda: True,
        upstream_euv_transmitting: Callable[[], bool] = lambda: True,
        upstream_trigger_rate_hz: Callable[[], float | None] = lambda: 96.0,
    ) -> None:
        self._upstream_triggers_enabled = upstream_triggers_enabled
        self._upstream_euv_transmitting = upstream_euv_transmitting
        self._upstream_trigger_rate_hz = upstream_trigger_rate_hz
        self._lock = threading.Lock()
        self._laser_enabled = True
        self._chopper_enabled = True
        self._pll_locked = True

    def set_laser_enabled(self, enabled: bool) -> None:
        with self._lock:
            self._laser_enabled = bool(enabled)

    def set_chopper_enabled(self, enabled: bool) -> None:
        with self._lock:
            self._chopper_enabled = bool(enabled)

    def set_pll_locked(self, locked: bool) -> None:
        with self._lock:
            self._pll_locked = bool(locked)

    def restore_nominal(self) -> None:
        with self._lock:
            self._laser_enabled = True
            self._chopper_enabled = True
            self._pll_locked = True

    def trigger_enabled(self) -> bool:
        status = self.status()
        return status.effective_triggers_enabled

    def euv_transmitting(self) -> bool:
        status = self.status()
        return status.effective_euv_transmitting

    def trigger_rate_hz(self) -> float | None:
        status = self.status()
        return status.trigger_rate_hz if status.effective_triggers_enabled else None

    def status(self) -> SimulatedLaserStatus:
        upstream_triggers_enabled = bool(self._upstream_triggers_enabled())
        upstream_euv_transmitting = bool(self._upstream_euv_transmitting())
        upstream_rate = self._upstream_trigger_rate_hz()
        trigger_rate_hz = None if upstream_rate is None else float(upstream_rate)
        with self._lock:
            laser_enabled = self._laser_enabled
            chopper_enabled = self._chopper_enabled
            pll_locked = self._pll_locked
        effective_triggers_enabled = upstream_triggers_enabled and laser_enabled and chopper_enabled
        return SimulatedLaserStatus(
            upstream_triggers_enabled=upstream_triggers_enabled,
            upstream_euv_transmitting=upstream_euv_transmitting,
            laser_enabled=laser_enabled,
            chopper_enabled=chopper_enabled,
            pll_locked=pll_locked,
            effective_triggers_enabled=effective_triggers_enabled,
            effective_euv_transmitting=(
                effective_triggers_enabled and pll_locked and upstream_euv_transmitting
            ),
            trigger_rate_hz=trigger_rate_hz,
        )