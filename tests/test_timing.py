from euv_acquisition.sources.simulated import SimulatedPulseConfig, SimulatedPulseSource
from euv_acquisition.timing import LaserTimingState
import json


def _state(*, frequency=192.0, phase=10.0):
    return LaserTimingState(True, False, True, False, phase, 0.0, 10.0, frequency)


def test_timing_state_round_trips_and_separates_triggering_from_transmission() -> None:
    shut = _state(phase=0.0)
    open_state = _state(phase=10.0)

    assert LaserTimingState.decode(open_state.encode()) == open_state
    assert shut.triggers_enabled is True
    assert shut.euv_transmitting() is False
    assert shut.trigger_rate_hz == 96.0
    assert open_state.euv_transmitting() is True


def test_timing_state_round_trips_producer_timestamps_and_decodes_v1() -> None:
    timestamped = LaserTimingState(True, False, True, False, 10.0, 0.0, 10.0, 192.0, 12, 34)
    legacy_payload = {
        "schema_version": 1,
        "laser_on": True,
        "laser_warming_up": False,
        "chopper_on": True,
        "chopper_starting_up": False,
        "current_phase": 10.0,
        "preinit_phase": 0.0,
        "configured_target_phase": 10.0,
        "chopper_frequency_hz": 192.0,
    }

    assert LaserTimingState.decode(timestamped.encode()) == timestamped
    decoded_legacy = LaserTimingState.decode(json.dumps(legacy_payload).encode("utf-8"))
    assert decoded_legacy.sampled_at_unix_ns is None
    assert decoded_legacy.sampled_at_monotonic_ns is None


def test_missing_frequency_fails_closed_at_the_simulated_source() -> None:
    clock = [0]
    state = [_state(frequency=None)]
    source = SimulatedPulseSource(
        pulse_config=SimulatedPulseConfig(noise_stddev_volts=0.0),
        trigger_enabled=lambda: state[0].triggers_enabled,
        euv_transmitting=lambda: state[0].euv_transmitting(),
        trigger_rate_hz=lambda: state[0].trigger_rate_hz,
        monotonic_time_ns=lambda: clock[0],
        unix_time_ns=lambda: clock[0],
    )
    source.open()
    try:
        assert source.capture() is None
        state[0] = _state()
        assert source.capture() is not None
    finally:
        source.close()