import numpy as np

from euv_acquisition.analysis import analyze_pulse
from euv_acquisition.models import CaptureConfig
from euv_acquisition.simulator_controls import SimulatorFaultControls
from euv_acquisition.sources.simulated import SimulatedPulseConfig, SimulatedPulseSource


def _source(*, trigger_enabled=lambda: True, transmitting=lambda: True):
    return SimulatedPulseSource(
        CaptureConfig(sample_rate_hz=10_000_000.0, window_seconds=10e-6, pretrigger_seconds=1e-6),
        SimulatedPulseConfig(seed=42, noise_stddev_volts=0.0),
        trigger_enabled=trigger_enabled,
        euv_transmitting=transmitting,
        unix_time_ns=lambda: 100,
        monotonic_time_ns=lambda: 200,
    )


def test_simulator_emits_no_capture_without_trigger() -> None:
    source = _source(trigger_enabled=lambda: False)
    source.open()
    try:
        assert source.capture() is None
    finally:
        source.close()


def test_simulator_emits_a_flat_capture_when_euv_is_blocked() -> None:
    blocked = _source(transmitting=lambda: False)
    transmitting = _source(transmitting=lambda: True)
    blocked.open()
    transmitting.open()
    try:
        blocked_pulse = blocked.capture()
        transmitting_pulse = transmitting.capture()
    finally:
        blocked.close()
        transmitting.close()

    assert blocked_pulse.captured_at_unix_ns == 100
    assert blocked_pulse.captured_at_monotonic_ns == 200
    assert blocked_pulse.samples_v.dtype == np.float32
    assert np.ptp(blocked_pulse.samples_v) == 0.0
    assert transmitting_pulse.captured_at_unix_ns == 100
    assert transmitting_pulse.captured_at_monotonic_ns == 200
    assert transmitting_pulse.samples_v.dtype == np.float32
    assert np.ptp(transmitting_pulse.samples_v) > 0.3
    assert analyze_pulse(transmitting_pulse.samples_v, transmitting.capture_config).integral_volt_seconds > 0


def test_simulator_is_deterministic_for_a_recorded_seed() -> None:
    first = SimulatedPulseSource(pulse_config=SimulatedPulseConfig(seed=7))
    second = SimulatedPulseSource(pulse_config=SimulatedPulseConfig(seed=7))
    first.open()
    second.open()
    try:
        assert np.array_equal(first.capture().samples_v, second.capture().samples_v)
    finally:
        first.close()
        second.close()


def test_simulator_does_not_emit_faster_than_its_configured_trigger_rate() -> None:
    clock = [0]
    source = SimulatedPulseSource(
        pulse_config=SimulatedPulseConfig(noise_stddev_volts=0.0, trigger_rate_hz=100.0),
        monotonic_time_ns=lambda: clock[0],
        unix_time_ns=lambda: clock[0],
    )
    source.open()
    try:
        assert source.capture() is not None
        assert source.capture() is None
        clock[0] = 9_999_999
        assert source.capture() is None
        clock[0] = 10_000_000
        assert source.capture() is not None
    finally:
        source.close()


def test_manual_fault_controls_preserve_triggering_during_a_pll_desync() -> None:
    controls = SimulatorFaultControls(upstream_trigger_rate_hz=lambda: 96.0)

    controls.set_pll_locked(False)
    assert controls.trigger_enabled() is True
    assert controls.euv_transmitting() is False
    assert controls.trigger_rate_hz() == 96.0

    controls.set_chopper_enabled(False)
    assert controls.trigger_enabled() is False
    assert controls.trigger_rate_hz() is None