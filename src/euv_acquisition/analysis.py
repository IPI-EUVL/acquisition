from __future__ import annotations

import numpy as np

from euv_acquisition.models import CaptureConfig, NativePulseAnalysis, PulseQuality


NATIVE_ANALYSIS_VERSION = "native-v1-pretrigger-mean-trapezoid-full-window"


def analyze_pulse(samples_v: np.ndarray, config: CaptureConfig) -> NativePulseAnalysis:
    samples = np.asarray(samples_v)
    if samples.ndim != 1 or len(samples) != config.window_samples:
        raise ValueError(f"Pulse must contain exactly {config.window_samples} samples.")
    if not np.issubdtype(samples.dtype, np.floating) or not np.isfinite(samples).all():
        raise ValueError("Pulse samples must be finite floating-point calibrated volts.")

    values = samples.astype(np.float64, copy=False)
    baseline = float(np.mean(values[: config.pretrigger_samples]))
    corrected = values - baseline
    integral = float(np.trapezoid(corrected, dx=config.sample_interval_seconds))
    minimum = float(np.min(values))
    maximum = float(np.max(values))
    peak_absolute = max(abs(minimum), abs(maximum))
    clip_threshold = config.input_full_scale_volts * config.clipping_fraction
    quality = PulseQuality.CLIPPED if peak_absolute >= clip_threshold else PulseQuality.OK

    return NativePulseAnalysis(
        baseline_volts=baseline,
        integral_volt_seconds=integral,
        minimum_volts=minimum,
        maximum_volts=maximum,
        peak_absolute_volts=peak_absolute,
        quality=quality,
        algorithm_version=NATIVE_ANALYSIS_VERSION,
    )
