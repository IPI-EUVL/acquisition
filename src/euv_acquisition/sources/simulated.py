from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Callable

import numpy as np

from euv_acquisition.models import CaptureConfig, CapturedPulse


@dataclass(frozen=True)
class SimulatedPulseConfig:
    seed: int = 1
    baseline_volts: float = 0.02
    noise_stddev_volts: float = 0.002
    amplitude_volts: float = 0.35
    center_seconds: float = 1.5e-6
    width_seconds: float = 0.35e-6
    trigger_rate_hz: float = 96.0

    def __post_init__(self) -> None:
        if isinstance(self.seed, bool) or not isinstance(self.seed, int):
            raise ValueError("seed must be an integer.")
        for name in (
            "baseline_volts",
            "noise_stddev_volts",
            "amplitude_volts",
            "center_seconds",
            "width_seconds",
            "trigger_rate_hz",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
                raise ValueError(f"{name} must be a finite number.")
        if self.noise_stddev_volts < 0:
            raise ValueError("noise_stddev_volts must be non-negative.")
        if self.width_seconds <= 0:
            raise ValueError("width_seconds must be positive.")
        if self.trigger_rate_hz <= 0:
            raise ValueError("trigger_rate_hz must be positive.")


class SimulatedPulseSource:
    def __init__(
        self,
        capture_config: CaptureConfig = CaptureConfig(),
        pulse_config: SimulatedPulseConfig = SimulatedPulseConfig(),
        *,
        trigger_enabled: Callable[[], bool] = lambda: True,
        euv_transmitting: Callable[[], bool] = lambda: True,
        trigger_rate_hz: Callable[[], float | None] | None = None,
        unix_time_ns: Callable[[], int] = time.time_ns,
        monotonic_time_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        self._capture_config = capture_config
        self._pulse_config = pulse_config
        self._trigger_enabled = trigger_enabled
        self._euv_transmitting = euv_transmitting
        self._trigger_rate_hz = trigger_rate_hz or (lambda: pulse_config.trigger_rate_hz)
        self._unix_time_ns = unix_time_ns
        self._monotonic_time_ns = monotonic_time_ns
        self._random = np.random.default_rng(pulse_config.seed)
        self._open = False
        self._next_trigger_monotonic_ns: int | None = None

    @property
    def capture_config(self) -> CaptureConfig:
        return self._capture_config

    def open(self) -> None:
        if self._open:
            raise RuntimeError("Simulated pulse source is already open.")
        self._open = True
        self._next_trigger_monotonic_ns = self._monotonic_time_ns()

    def capture(self) -> CapturedPulse | None:
        if not self._open:
            raise RuntimeError("Simulated pulse source is not open.")
        if not self._trigger_enabled():
            self._next_trigger_monotonic_ns = self._monotonic_time_ns()
            return None

        trigger_rate_hz = self._trigger_rate_hz()
        if trigger_rate_hz is None or trigger_rate_hz <= 0:
            self._next_trigger_monotonic_ns = self._monotonic_time_ns()
            return None

        now = self._monotonic_time_ns()
        if self._next_trigger_monotonic_ns is None:
            self._next_trigger_monotonic_ns = now
        if now < self._next_trigger_monotonic_ns:
            return None
        interval_ns = int(round(125e6 / trigger_rate_hz))
        self._next_trigger_monotonic_ns += interval_ns
        while self._next_trigger_monotonic_ns <= now:
            self._next_trigger_monotonic_ns += interval_ns

        config = self._capture_config
        pulse = self._pulse_config
        times = np.arange(config.window_samples, dtype=np.float64) * config.sample_interval_seconds
        times -= config.pretrigger_seconds
        samples = np.full(config.window_samples, pulse.baseline_volts, dtype=np.float64)
        if self._euv_transmitting():
            samples += pulse.amplitude_volts * np.exp(
                -0.5 * ((times - pulse.center_seconds) / pulse.width_seconds) ** 2
            )
        if pulse.noise_stddev_volts:
            samples += self._random.normal(0.0, pulse.noise_stddev_volts, config.window_samples)

        return CapturedPulse(
            samples_v=samples.astype(np.float32),
            captured_at_unix_ns=self._unix_time_ns(),
            captured_at_monotonic_ns=now,
        )

    def close(self) -> None:
        self._open = False
        self._next_trigger_monotonic_ns = None
