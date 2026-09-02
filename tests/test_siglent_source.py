import struct
from datetime import datetime
from itertools import count
from uuid import UUID

import numpy as np
import pytest

from euv_acquisition.analysis import analyze_pulse
from euv_acquisition.models import CaptureConfig, SourceCaptureBatch
from euv_acquisition.sources.siglent import (
    SIGLENT_BATCH_KIND,
    SIGLENT_NATIVE_ANALYSIS_VERSION,
    SiglentPulseSource,
)


_BATCH_ID = UUID("11111111-2222-3333-4444-555555555555")


def _timestamp_ns(seconds: float) -> int:
    base = datetime(2025, 1, 2, 3, 4, 0) - datetime(1970, 1, 1)
    whole_seconds = int(seconds)
    fractional_ns = int(round((seconds - whole_seconds) * 1_000_000_000))
    return (base.days * 86_400 + base.seconds + whole_seconds) * 1_000_000_000 + fractional_ns


def _sequence_fixture(codes: np.ndarray, *, sample_rate_hz: float = 1_000_000.0):
    frame_count, point_count = codes.shape
    waveform_interval = 10
    descriptor = bytearray(0x200 + 16 * frame_count)
    struct.pack_into("<H", descriptor, 0x20, 1)
    struct.pack_into("<H", descriptor, 0x22, 0)
    struct.pack_into("<I", descriptor, 0x74, point_count * waveform_interval)
    struct.pack_into("<I", descriptor, 0x90, frame_count)
    struct.pack_into("<I", descriptor, 0x94, frame_count)
    struct.pack_into("<f", descriptor, 0x9C, 0.1)
    struct.pack_into("<f", descriptor, 0xA0, 0.0)
    struct.pack_into("<f", descriptor, 0xA4, 7680.0)
    struct.pack_into("<H", descriptor, 0xAC, 16)
    struct.pack_into("<f", descriptor, 0xB0, 1.0 / sample_rate_hz / waveform_interval)
    struct.pack_into("<d", descriptor, 0xB4, 0.0)
    struct.pack_into("<H", descriptor, 0x144, 0)
    struct.pack_into("<f", descriptor, 0x148, 1.0)

    seconds = tuple(1.0 + index * 0.01 for index in range(frame_count))
    timestamp_start = len(descriptor) - 16 * frame_count
    for index, value in enumerate(seconds):
        offset = timestamp_start + 16 * index
        struct.pack_into("<d", descriptor, offset, value)
        descriptor[offset + 8] = 4
        descriptor[offset + 9] = 3
        descriptor[offset + 10] = 2
        descriptor[offset + 11] = 1
        struct.pack_into("<h", descriptor, offset + 12, 2025)

    encoded = np.where(codes < 0, codes + (1 << 12), codes).astype("<u2") << 4
    return bytes(descriptor), encoded.astype("<u2", copy=False).tobytes(), tuple(
        _timestamp_ns(value) for value in seconds
    )


def _hash_block(payload: bytes) -> bytes:
    size = str(len(payload)).encode("ascii")
    return b"C1:WF DAT2,#" + str(len(size)).encode("ascii") + size + payload + b"\n"


class _FakeScope:
    def __init__(self, descriptor: bytes, data: bytes, *, trigger_states=("STOP",)) -> None:
        self.descriptor = descriptor
        self.data = data
        self.trigger_states = iter(trigger_states)
        self.commands: list[str] = []
        self.buffer = bytearray()
        self.timeout = None
        self.write_termination = None
        self.read_termination = None
        self.closed = False

    def write(self, command: str) -> None:
        self.commands.append(command)
        if command == ":ACQ:SRAT?":
            self.buffer.extend(b"2.0E9\n")
        elif command == ":TRIG:STAT?":
            self.buffer.extend(f"{next(self.trigger_states, 'STOP')}\n".encode("ascii"))
        elif command == ":WAVeform:PREamble?":
            self.buffer.extend(_hash_block(self.descriptor))
        elif command == ":WAVeform:DATA?":
            self.buffer.extend(_hash_block(self.data))

    def read_bytes(self, size: int) -> bytes:
        if len(self.buffer) < size:
            raise TimeoutError("fake VISA read timeout")
        value = bytes(self.buffer[:size])
        del self.buffer[:size]
        return value

    def close(self) -> None:
        self.closed = True


class _FakeResourceManager:
    def __init__(self, scope: _FakeScope) -> None:
        self.scope = scope
        self.resource_name = None
        self.closed = False

    def open_resource(self, resource_name: str) -> _FakeScope:
        self.resource_name = resource_name
        return self.scope

    def close(self) -> None:
        self.closed = True


def _capture_config(point_count: int = 30) -> CaptureConfig:
    return CaptureConfig(
        sample_rate_hz=1_000_000.0,
        window_seconds=point_count / 1_000_000.0,
        pretrigger_seconds=25 / 1_000_000.0,
    )


def test_siglent_source_preserves_legacy_float64_integrals_and_transfer_envelope() -> None:
    first = np.asarray(([1, -1] * 12) + [1] + [480, 300, -100, 120, 20], dtype=np.int32)
    second = np.asarray(([2, -2] * 12) + [2] + [240, 150, -50, 60, 10], dtype=np.int32)
    codes = np.vstack((first, second))
    descriptor, data, frame_times = _sequence_fixture(codes)
    scope = _FakeScope(descriptor, data)
    manager = _FakeResourceManager(scope)
    unix_times = iter((frame_times[0] - 1_000, frame_times[-1] + 2_000))
    monotonic_times = count(10_000_000_000, 1_000)
    config = _capture_config()
    source = SiglentPulseSource(
        config,
        resource_name="TCPIP::scope::INSTR",
        sequence_count=2,
        resource_manager_factory=lambda: manager,
        unix_time_ns=lambda: next(unix_times),
        monotonic_time_ns=lambda: next(monotonic_times),
        sleep=lambda _seconds: None,
        batch_id_factory=lambda: _BATCH_ID,
    )

    source.open()
    batch = source.capture()
    source.close()

    assert isinstance(batch, SourceCaptureBatch)
    assert batch.envelope.batch_id == _BATCH_ID
    assert batch.envelope.batch_kind == SIGLENT_BATCH_KIND
    assert batch.envelope.capture_started_unix_ns == frame_times[0] - 1_000
    assert batch.envelope.capture_completed_unix_ns == frame_times[-1] + 2_000
    assert [pulse.captured_at_unix_ns for pulse in batch.pulses] == list(frame_times)
    assert all(pulse.samples_v.dtype == np.float32 for pulse in batch.pulses)

    interval = struct.unpack("<f", descriptor[0xB0:0xB4])[0]
    scale = struct.unpack("<f", descriptor[0x9C:0xA0])[0] / 480.0
    decoded = codes[0].astype(np.float64) * scale
    time_axis = np.linspace(0, interval * len(decoded) * 10.0, len(decoded), endpoint=False)
    baseline = float(np.average(decoded[:25]))
    expected_integral = float(np.trapezoid(decoded - baseline, time_axis))
    analysis = batch.pulses[0].native_analysis
    assert analysis.algorithm_version == SIGLENT_NATIVE_ANALYSIS_VERSION
    assert analysis.integral_volt_seconds.hex() == expected_integral.hex()
    assert analyze_pulse(batch.pulses[0].samples_v, config).integral_volt_seconds.hex() != expected_integral.hex()

    assert manager.resource_name == "TCPIP::scope::INSTR"
    assert ":ACQ:SRAT?" in scope.commands
    assert not any(command.startswith(":ACQ:SRAT ") for command in scope.commands)
    assert ":ACQ:MMAN FSRate" not in scope.commands
    assert source.hardware_sample_rate_hz == 2_000_000_000.0
    assert scope.commands.index(":TRIGger:RUN") < scope.commands.index(":WAVeform:DATA?")
    assert scope.commands[-2:] == [":ACQ:SEQuence OFF", ":STOP"]
    assert scope.closed and manager.closed and source.release_confirmed


def test_siglent_source_stops_an_incomplete_sequence_without_reading_it() -> None:
    descriptor, data, _frame_times = _sequence_fixture(np.zeros((2, 30), dtype=np.int32))
    scope = _FakeScope(descriptor, data, trigger_states=("READY",))
    manager = _FakeResourceManager(scope)
    stop_checks = iter((False, False, True))
    source = SiglentPulseSource(
        _capture_config(),
        resource_name="TCPIP::scope::INSTR",
        sequence_count=2,
        resource_manager_factory=lambda: manager,
        sleep=lambda _seconds: None,
    )
    source.set_stop_requested(lambda: next(stop_checks))

    source.open()
    captured = source.capture()
    source.close()

    assert captured is None
    assert ":TRIGger:MODE STOP" in scope.commands
    assert ":WAVeform:PREamble?" not in scope.commands
    assert ":WAVeform:DATA?" not in scope.commands
    assert scope.closed and manager.closed and source.release_confirmed


def test_siglent_decoder_rejects_unexpected_exported_shape() -> None:
    descriptor, data, _frame_times = _sequence_fixture(np.zeros((2, 30), dtype=np.int32))
    source = SiglentPulseSource(
        _capture_config(31),
        resource_name="TCPIP::scope::INSTR",
        sequence_count=2,
    )

    with pytest.raises(ValueError, match="exported point count"):
        source.decode_sequence_waveforms(descriptor, data)


def test_siglent_decoder_uses_the_actual_preamble_sample_interval() -> None:
    descriptor, data, _frame_times = _sequence_fixture(
        np.zeros((2, 30), dtype=np.int32),
        sample_rate_hz=2_000_000.0,
    )
    source = SiglentPulseSource(
        _capture_config(),
        resource_name="TCPIP::scope::INSTR",
        sequence_count=2,
    )

    time_axis, _waveforms, _timestamps = source.decode_sequence_waveforms(descriptor, data)

    assert time_axis[1] == pytest.approx(0.5e-6)


def test_siglent_source_requires_exactly_25_pretrigger_samples() -> None:
    with pytest.raises(ValueError, match="exactly 25"):
        SiglentPulseSource(
            CaptureConfig(
                sample_rate_hz=1_000_000.0,
                window_seconds=30e-6,
                pretrigger_seconds=24e-6,
            ),
            resource_name="TCPIP::scope::INSTR",
        )