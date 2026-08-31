import numpy as np

from euv_acquisition.models import CaptureConfig
from euv_acquisition.sources.red_pitaya import RedPitayaPulseSource


class FakeRp:
    RP_OK = 0
    RP_CH_1 = 1
    RP_DEC_1 = 1
    RP_TRIG_SRC_EXT_PE = 2
    RP_TRIG_STATE_WAITING = 0
    RP_TRIG_STATE_TRIGGERED = 1

    def __init__(self):
        self.triggered = False
        self.filled = False
        self.trigger_index = 8
        self.calls = []

    def fBuffer(self, length):
        return [0.0] * length

    def rp_Init(self):
        self.calls.append("init")
        return self.RP_OK

    def rp_Release(self):
        self.calls.append("release")
        return self.RP_OK

    def rp_AcqReset(self):
        self.calls.append("reset")
        return self.RP_OK

    def rp_AcqSetDecimation(self, _value):
        return self.RP_OK

    def rp_AcqSetTriggerDelay(self, _value):
        return self.RP_OK

    def rp_AcqSetExtTriggerDebouncerUs(self, _value):
        return self.RP_OK

    def rp_AcqStart(self):
        self.calls.append("start")
        return self.RP_OK

    def rp_AcqSetTriggerSrc(self, _value):
        self.calls.append("trigger")
        return self.RP_OK

    def rp_AcqGetTriggerState(self):
        return self.RP_OK, self.RP_TRIG_STATE_TRIGGERED if self.triggered else self.RP_TRIG_STATE_WAITING

    def rp_AcqGetBufferFillState(self):
        return self.RP_OK, self.filled

    def rp_AcqGetWritePointerAtTrig(self):
        self.calls.append("trigger_pointer")
        return self.RP_OK, self.trigger_index

    def rp_AcqGetDataV(self, _channel, _start, count, buffer):
        for index in range(count):
            buffer[index] = float(index)
        return self.RP_OK

    def rp_AcqStop(self):
        self.calls.append("stop")
        return self.RP_OK


def test_red_pitaya_source_arms_and_reads_a_triggered_window_without_blocking() -> None:
    clock = [0]
    fake = FakeRp()
    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=4e-6, pretrigger_seconds=1e-6)
    source = RedPitayaPulseSource(
        config,
        rp_api=fake,
        full_buffer_samples=16,
        prefill_seconds=1e-6,
        unix_time_ns=lambda: 1_000,
        monotonic_time_ns=lambda: clock[0],
    )
    source.open()
    try:
        assert source.state == "prefill"
        assert source.capture() is None
        clock[0] = 1_000
        assert source.capture() is None
        assert source.state == "waiting_trigger"
        assert source.capture() is None
        fake.triggered = True
        assert source.capture() is None
        assert source.state == "waiting_buffer"
        fake.filled = True
        pulse = source.capture()

        assert pulse.samples_v.dtype == np.float32
        assert pulse.samples_v.tolist() == [7.0, 8.0, 9.0, 10.0]
        assert pulse.captured_at_unix_ns == 1_000
        assert source.state == "prefill"
    finally:
        source.close()

    assert fake.calls.count("start") == 2
    assert "trigger_pointer" in fake.calls
    assert fake.calls[-1] == "release"


def test_red_pitaya_source_extracts_window_across_ring_buffer_wraparound() -> None:
    fake = FakeRp()
    fake.trigger_index = 1
    fake.triggered = True
    fake.filled = True
    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=5e-6, pretrigger_seconds=3e-6)
    source = RedPitayaPulseSource(
        config,
        rp_api=fake,
        full_buffer_samples=16,
        prefill_seconds=0.0,
    )

    source.open()
    try:
        assert source.capture() is None
        assert source.capture() is None
        pulse = source.capture()

        assert pulse is not None
        assert pulse.samples_v.tolist() == [14.0, 15.0, 0.0, 1.0, 2.0]
    finally:
        source.close()