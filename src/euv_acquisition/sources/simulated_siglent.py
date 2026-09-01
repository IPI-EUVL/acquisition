from __future__ import annotations

import time
from collections.abc import Callable
from uuid import UUID, uuid4

import numpy as np

from euv_acquisition.models import (
    CaptureConfig,
    CapturedPulse,
    SourceBatchEnvelope,
    SourceCaptureBatch,
)
from euv_acquisition.pipeline_metrics import PipelineMetrics
from euv_acquisition.sources.siglent import (
    SIGLENT_BATCH_KIND,
    SIGLENT_CAPTURE_MODE,
    analyze_siglent_waveform,
)
from euv_acquisition.sources.simulated import SimulatedPulseConfig


class SimulatedSiglentPulseSource:
    def __init__(
        self,
        capture_config: CaptureConfig,
        pulse_config: SimulatedPulseConfig = SimulatedPulseConfig(),
        *,
        sequence_count: int = 250,
        unix_time_ns: Callable[[], int] = time.time_ns,
        monotonic_time_ns: Callable[[], int] = time.monotonic_ns,
        sleep: Callable[[float], None] = time.sleep,
        batch_id_factory: Callable[[], UUID] = uuid4,
        metrics: PipelineMetrics | None = None,
    ) -> None:
        if isinstance(sequence_count, bool) or not isinstance(sequence_count, int) or sequence_count <= 0:
            raise ValueError("Simulated Siglent sequence count must be a positive integer.")
        if capture_config.pretrigger_samples != 25:
            raise ValueError("Simulated Siglent capture configuration must contain exactly 25 pre-trigger samples.")
        self._capture_config = capture_config
        self._pulse_config = pulse_config
        self._sequence_count = sequence_count
        self._unix_time_ns = unix_time_ns
        self._monotonic_time_ns = monotonic_time_ns
        self._sleep = sleep
        self._batch_id_factory = batch_id_factory
        self._metrics = metrics or PipelineMetrics()
        self._random = np.random.default_rng(pulse_config.seed)
        self._state = "stopped"

    @property
    def capture_config(self) -> CaptureConfig:
        return self._capture_config

    @property
    def capture_mode(self) -> str:
        return SIGLENT_CAPTURE_MODE

    @property
    def requested_capture_mode(self) -> str:
        return SIGLENT_CAPTURE_MODE

    @property
    def effective_capture_mode(self) -> str:
        return SIGLENT_CAPTURE_MODE

    @property
    def capture_fallback_reason(self) -> None:
        return None

    @property
    def state(self) -> str:
        return self._state

    @property
    def release_confirmed(self) -> bool:
        return self._state == "stopped"

    def set_metrics(self, metrics: PipelineMetrics) -> None:
        self._metrics = metrics

    def open(self) -> None:
        if self._state != "stopped":
            raise RuntimeError("Simulated Siglent pulse source is already open.")
        self._state = "open"

    def capture(self) -> SourceCaptureBatch:
        if self._state != "open":
            raise RuntimeError("Simulated Siglent pulse source is not open.")

        capture_started_unix_ns = self._unix_time_ns()
        capture_started_monotonic_ns = self._monotonic_time_ns()
        frame_interval_ns = int(round(1e9 / self._pulse_config.trigger_rate_hz))
        self._sleep(self._sequence_count / self._pulse_config.trigger_rate_hz)
        capture_completed_unix_ns = self._unix_time_ns()
        capture_completed_monotonic_ns = self._monotonic_time_ns()
        self._metrics.record_duration(
            "trigger_wait",
            capture_completed_monotonic_ns - capture_started_monotonic_ns,
        )

        config = self._capture_config
        pulse = self._pulse_config
        time_axis = np.arange(config.window_samples, dtype=np.float64) * config.sample_interval_seconds
        relative_time = time_axis - config.pretrigger_seconds
        waveform = np.full(config.window_samples, pulse.baseline_volts, dtype=np.float64)
        waveform += pulse.amplitude_volts * np.exp(
            -0.5 * ((relative_time - pulse.center_seconds) / pulse.width_seconds) ** 2
        )
        waveforms = np.broadcast_to(waveform, (self._sequence_count, config.window_samples)).copy()
        if pulse.noise_stddev_volts:
            waveforms += self._random.normal(0.0, pulse.noise_stddev_volts, waveforms.shape)

        pulses = tuple(
            CapturedPulse(
                samples_v=frame,
                captured_at_unix_ns=capture_started_unix_ns + index * frame_interval_ns,
                captured_at_monotonic_ns=capture_started_monotonic_ns + index * frame_interval_ns,
                native_analysis=analyze_siglent_waveform(frame, time_axis, config),
            )
            for index, frame in enumerate(waveforms)
        )
        return SourceCaptureBatch(
            pulses,
            SourceBatchEnvelope(
                batch_id=self._batch_id_factory(),
                batch_kind=SIGLENT_BATCH_KIND,
                capture_started_unix_ns=capture_started_unix_ns,
                capture_completed_unix_ns=capture_completed_unix_ns,
            ),
        )

    def close(self) -> None:
        self._state = "stopped"