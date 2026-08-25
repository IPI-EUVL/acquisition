import numpy as np
import pytest

from euv_acquisition.analysis import NATIVE_ANALYSIS_VERSION, analyze_pulse
from euv_acquisition.models import CaptureConfig, CapturedPulse, PulseQuality, PulseRecord, PulseReport


def test_native_analysis_uses_full_pretrigger_mean_and_full_window_trapezoid() -> None:
    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=10e-6, pretrigger_seconds=2e-6)
    samples = np.array([0.2, 0.2, 1.2, 1.2, 1.2, 1.2, 0.2, 0.2, 0.2, 0.2], dtype=np.float32)

    result = analyze_pulse(samples, config)

    expected = np.trapezoid(samples.astype(np.float64) - 0.2, dx=1e-6)
    assert result.baseline_volts == pytest.approx(0.2)
    assert result.integral_volt_seconds == pytest.approx(expected)
    assert result.algorithm_version == NATIVE_ANALYSIS_VERSION
    assert result.quality is PulseQuality.CLIPPED


def test_native_analysis_preserves_signed_integrals() -> None:
    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=4e-6, pretrigger_seconds=1e-6)
    samples = np.array([0.1, 0.0, 0.0, 0.0], dtype=np.float32)

    result = analyze_pulse(samples, config)

    assert result.integral_volt_seconds < 0
    assert result.quality is PulseQuality.OK


def test_native_analysis_rejects_wrong_sample_count() -> None:
    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=4e-6, pretrigger_seconds=1e-6)

    with pytest.raises(ValueError, match="exactly 4"):
        analyze_pulse(np.zeros(3, dtype=np.float32), config)


def test_pulse_report_round_trips_with_strict_analysis_schema() -> None:
    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=4e-6, pretrigger_seconds=1e-6)
    samples = np.zeros(4, dtype=np.float32)
    record = PulseRecord(__import__("uuid").uuid4(), 2, CapturedPulse(samples, 10, 20), analyze_pulse(samples, config))

    report = PulseReport.from_dict(PulseReport.from_record(record).to_dict())

    assert report.session_id == record.session_id
    assert report.sequence == 2
