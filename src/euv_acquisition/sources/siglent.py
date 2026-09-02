from __future__ import annotations

import importlib
import math
import struct
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import numpy as np

from euv_acquisition.models import (
    CaptureConfig,
    CapturedPulse,
    NativePulseAnalysis,
    PulseQuality,
    SourceBatchEnvelope,
    SourceCaptureBatch,
)
from euv_acquisition.pipeline_metrics import PipelineMetrics


SIGLENT_NATIVE_ANALYSIS_VERSION = "siglent-native-v1-pre-float32-legacy-trapezoid"
SIGLENT_CAPTURE_MODE = "siglent-sequence"
SIGLENT_BATCH_KIND = "siglent_sequence"
SIGLENT_NOMINAL_EXPORTED_SAMPLE_RATE_HZ = 100_000_000.0


def analyze_siglent_waveform(
    waveform: np.ndarray,
    time_axis: np.ndarray,
    capture_config: CaptureConfig,
) -> NativePulseAnalysis:
    baseline = float(np.average(waveform[: min(25, len(waveform))]))
    corrected = waveform - baseline
    integral = float(np.trapezoid(corrected, time_axis))
    minimum = float(np.min(waveform))
    maximum = float(np.max(waveform))
    peak_absolute = max(abs(minimum), abs(maximum))
    clip_threshold = capture_config.input_full_scale_volts * capture_config.clipping_fraction
    quality = PulseQuality.CLIPPED if peak_absolute >= clip_threshold else PulseQuality.OK
    return NativePulseAnalysis(
        baseline_volts=baseline,
        integral_volt_seconds=integral,
        minimum_volts=minimum,
        maximum_volts=maximum,
        peak_absolute_volts=peak_absolute,
        quality=quality,
        algorithm_version=SIGLENT_NATIVE_ANALYSIS_VERSION,
    )


def _default_resource_manager() -> Any:
    return importlib.import_module("pyvisa").ResourceManager()


class SiglentPulseSource:
    HORI_NUM = 10.0
    TDIV_ENUM = (
        100e-12,
        200e-12,
        500e-12,
        1e-9,
        2e-9,
        5e-9,
        10e-9,
        20e-9,
        50e-9,
        100e-9,
        200e-9,
        500e-9,
        1e-6,
        2e-6,
        5e-6,
        10e-6,
        20e-6,
        50e-6,
        100e-6,
        200e-6,
        500e-6,
        1e-3,
        2e-3,
        5e-3,
        10e-3,
        20e-3,
        50e-3,
        100e-3,
        200e-3,
        500e-3,
        1,
        2,
        5,
        10,
        20,
        50,
        100,
        200,
        500,
        1000,
    )
    _EPOCH0 = datetime(1970, 1, 1)

    def __init__(
        self,
        capture_config: CaptureConfig,
        *,
        resource_name: str,
        sequence_count: int = 250,
        waveform_interval: int = 10,
        trigger_poll_seconds: float = 0.02,
        timeout_milliseconds: int = 10_000,
        resource_manager_factory: Callable[[], Any] = _default_resource_manager,
        unix_time_ns: Callable[[], int] = time.time_ns,
        monotonic_time_ns: Callable[[], int] = time.monotonic_ns,
        sleep: Callable[[float], None] = time.sleep,
        batch_id_factory: Callable[[], UUID] = uuid4,
        metrics: PipelineMetrics | None = None,
    ) -> None:
        if not resource_name.strip():
            raise ValueError("Siglent VISA resource name cannot be empty.")
        for name, value in (
            ("sequence_count", sequence_count),
            ("waveform_interval", waveform_interval),
            ("timeout_milliseconds", timeout_milliseconds),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"Siglent {name} must be a positive integer.")
        if (
            isinstance(trigger_poll_seconds, bool)
            or not isinstance(trigger_poll_seconds, (int, float))
            or not math.isfinite(float(trigger_poll_seconds))
            or trigger_poll_seconds <= 0
        ):
            raise ValueError("Siglent trigger poll interval must be positive and finite.")
        if capture_config.pretrigger_samples != 25:
            raise ValueError("Siglent capture configuration must contain exactly 25 pre-trigger samples.")
        if capture_config.window_samples <= capture_config.pretrigger_samples:
            raise ValueError("Siglent capture window must extend beyond its 25 pre-trigger samples.")
        if not callable(resource_manager_factory) or not callable(batch_id_factory):
            raise ValueError("Siglent resource manager and batch ID factories must be callable.")

        self._capture_config = capture_config
        self._resource_name = resource_name
        self._sequence_count = sequence_count
        self._waveform_interval = waveform_interval
        self._trigger_poll_seconds = float(trigger_poll_seconds)
        self._timeout_milliseconds = timeout_milliseconds
        self._resource_manager_factory = resource_manager_factory
        self._unix_time_ns = unix_time_ns
        self._monotonic_time_ns = monotonic_time_ns
        self._sleep = sleep
        self._batch_id_factory = batch_id_factory
        self._metrics = metrics or PipelineMetrics()
        self._resource_manager = None
        self._scope = None
        self._state = "stopped"
        self._release_confirmed = True
        self._hardware_sample_rate_hz: float | None = None
        self._stop_requested: Callable[[], bool] = lambda: False

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
        return self._release_confirmed

    @property
    def hardware_sample_rate_hz(self) -> float | None:
        return self._hardware_sample_rate_hz

    def set_metrics(self, metrics: PipelineMetrics) -> None:
        self._metrics = metrics

    def set_stop_requested(self, stop_requested: Callable[[], bool]) -> None:
        if not callable(stop_requested):
            raise ValueError("Siglent stop-request predicate must be callable.")
        self._stop_requested = stop_requested

    def open(self) -> None:
        if self._state != "stopped" or not self._release_confirmed:
            raise RuntimeError("Siglent pulse source is already open or its previous release failed.")
        self._release_confirmed = False
        try:
            self._resource_manager = self._resource_manager_factory()
            self._scope = self._resource_manager.open_resource(self._resource_name)
            self._scope.write_termination = "\n"
            self._scope.read_termination = None
            self._scope.timeout = self._timeout_milliseconds
            self._configure()
            self._state = "open"
        except BaseException:
            self._close_transport()
            raise

    def capture(self) -> SourceCaptureBatch | None:
        if self._state != "open" or self._scope is None:
            raise RuntimeError("Siglent pulse source is not open.")

        self._scope.write(":ACQ:SEQuence ON")
        self._scope.write(f":ACQ:SEQuence:COUNt {self._sequence_count}")
        self._scope.write(":TRIGger:MODE SINGle")
        self._scope.write(":TRIGger:RUN")
        capture_started_unix_ns = self._unix_time_ns()
        capture_started_monotonic_ns = self._monotonic_time_ns()

        trigger_wait_started_ns = capture_started_monotonic_ns
        while True:
            if self._stop_requested():
                self._scope.write(":TRIGger:MODE STOP")
                return None
            self._sleep(self._trigger_poll_seconds)
            if self._stop_requested():
                self._scope.write(":TRIGger:MODE STOP")
                return None
            self._scope.write(":TRIG:STAT?")
            trigger_state = self._read_line().upper()
            if "STOP" in trigger_state:
                break
        self._metrics.record_duration(
            "trigger_wait",
            self._monotonic_time_ns() - trigger_wait_started_ns,
        )

        read_started_ns = self._monotonic_time_ns()
        self._scope.write(":WAVeform:SEQuence 0,1")
        self._scope.write(":WAVeform:SOURce C1")
        self._scope.write(":WAVeform:PREamble?")
        descriptor = self._read_hash_block()
        self._scope.write(":WAVeform:DATA?")
        data = self._read_hash_block()
        capture_completed_unix_ns = self._unix_time_ns()
        capture_completed_monotonic_ns = self._monotonic_time_ns()
        self._metrics.record_duration(
            "hardware_read",
            capture_completed_monotonic_ns - read_started_ns,
        )

        time_axis, waveforms, frame_unix_ns = self.decode_sequence_waveforms(descriptor, data)
        frame_monotonic_ns = tuple(
            capture_started_monotonic_ns + timestamp - capture_started_unix_ns
            for timestamp in frame_unix_ns
        )
        if any(timestamp < 0 for timestamp in frame_monotonic_ns):
            raise ValueError("Siglent frame timestamps cannot be mapped to the host monotonic clock.")

        pulses = tuple(
            CapturedPulse(
                samples_v=waveform,
                captured_at_unix_ns=unix_ns,
                captured_at_monotonic_ns=monotonic_ns,
                native_analysis=self._analyze_waveform(waveform, time_axis),
            )
            for waveform, unix_ns, monotonic_ns in zip(
                waveforms,
                frame_unix_ns,
                frame_monotonic_ns,
            )
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
        if self._state == "stopped" and self._scope is None and self._resource_manager is None:
            return
        first_error = None
        if self._scope is not None:
            for command in (":ACQ:SEQuence OFF", ":STOP"):
                try:
                    self._scope.write(command)
                except BaseException as exc:
                    if first_error is None:
                        first_error = exc
        close_error = self._close_transport()
        if first_error is None:
            first_error = close_error
        if first_error is not None:
            raise first_error

    def decode_sequence_waveforms(
        self,
        descriptor: bytes,
        data: bytes,
    ) -> tuple[np.ndarray, np.ndarray, tuple[int, ...]]:
        metadata = self._parse_sequence_preamble(descriptor)
        read_points = metadata["read_points"]
        read_frames = metadata["read_frames"]
        if read_points % self._waveform_interval:
            raise ValueError("Siglent preamble point count is not divisible by the waveform interval.")
        exported_points = int(read_points / self._waveform_interval)
        if exported_points != self._capture_config.window_samples:
            raise ValueError(
                "Siglent preamble exported point count does not match the configured capture window."
            )
        if read_frames != self._sequence_count:
            raise ValueError("Siglent preamble frame count does not match the configured sequence count.")

        effective_interval = metadata["interval"] * self._waveform_interval

        width = metadata["width"]
        expected_bytes = exported_points * read_frames * (1 if width == 0 else 2)
        if len(data) != expected_bytes:
            raise ValueError(
                f"Siglent waveform length is {len(data)} bytes; expected {expected_bytes}."
            )
        if width == 0:
            adc_bits = 8
            raw = np.frombuffer(data, dtype=np.uint8)
            code_per_div = metadata["code_raw"] / (1 << (16 - adc_bits))
            center = (1 << (adc_bits - 1)) - 1
            full = 1 << adc_bits
        else:
            adc_bits = 12
            dtype = ">u2" if metadata["order"] == 1 else "<u2"
            raw = np.frombuffer(data, dtype=dtype) >> (16 - adc_bits)
            code_per_div = metadata["code_raw"] / (1 << (16 - adc_bits))
            center = (1 << (adc_bits - 1)) - 1
            full = 1 << adc_bits
        if not math.isfinite(code_per_div) or code_per_div == 0:
            raise ValueError("Siglent preamble code-per-division scale is invalid.")

        codes = raw.reshape(read_frames, exported_points).astype(np.int32)
        codes[codes > center] -= full
        waveforms = codes.astype(np.float64) * (metadata["vdiv"] / code_per_div) - metadata["voff"]
        if not np.isfinite(waveforms).all():
            raise ValueError("Siglent decoded waveforms contain non-finite values.")

        time_axis = np.arange(exported_points, dtype=np.float64) * effective_interval
        frame_unix_ns = self._epochs_ns_from_preamble(descriptor, read_frames)
        if any(current <= previous for previous, current in zip(frame_unix_ns, frame_unix_ns[1:])):
            raise ValueError("Siglent frame timestamps must increase.")
        return time_axis, waveforms, frame_unix_ns

    def _configure(self) -> None:
        for command in (
            ":STOP",
            ":WAVeform:WIDTh BYTE",
            ":WAV:WIDT WORD; :WAV:FORM WORD",
            f":WAVeform:INTerval {self._waveform_interval}",
            ":ACQ:TYPE NORM",
            ":CHAN1:DISP ON; :WAV:SOUR C1",
            ":HISTory ON",
        ):
            self._scope.write(command)
        self._scope.write(":ACQ:SRAT?")
        sample_rate_hz = float(self._read_line())
        if not math.isfinite(sample_rate_hz) or sample_rate_hz <= 0:
            raise ValueError("Siglent reported an invalid hardware sample rate.")
        self._hardware_sample_rate_hz = sample_rate_hz
        self._scope.write(":RUN")

    def _parse_sequence_preamble(self, descriptor: bytes) -> dict[str, int | float]:
        if len(descriptor) < 0x14C:
            raise ValueError("Siglent waveform preamble is too short.")

        def unsigned_short(offset: int) -> int:
            return struct.unpack("<H", descriptor[offset : offset + 2])[0]

        def unsigned_int(offset: int) -> int:
            return struct.unpack("<I", descriptor[offset : offset + 4])[0]

        def float32(offset: int) -> float:
            return struct.unpack("<f", descriptor[offset : offset + 4])[0]

        width = unsigned_short(0x20)
        order = unsigned_short(0x22)
        if width not in (0, 1) or order not in (0, 1):
            raise ValueError("Siglent waveform preamble has unsupported width or byte order.")
        probe = float32(0x148)
        metadata = {
            "width": width,
            "order": order,
            "read_points": unsigned_int(0x74),
            "read_frames": unsigned_int(0x90),
            "vdiv": float32(0x9C) * probe,
            "voff": float32(0xA0) * probe,
            "code_raw": float32(0xA4),
            "interval": float32(0xB0),
        }
        if any(
            not math.isfinite(float(metadata[name]))
            for name in ("vdiv", "voff", "code_raw", "interval")
        ) or metadata["interval"] <= 0:
            raise ValueError("Siglent waveform preamble contains invalid scaling values.")
        return metadata

    def _epochs_ns_from_preamble(self, descriptor: bytes, frame_count: int) -> tuple[int, ...]:
        timestamp_bytes = 16 * frame_count
        if len(descriptor) < timestamp_bytes:
            raise ValueError("Siglent waveform preamble is missing frame timestamps.")
        timestamp_blob = descriptor[-timestamp_bytes:]
        output = []
        for index in range(frame_count):
            record = timestamp_blob[16 * index : 16 * (index + 1)]
            seconds = struct.unpack("<d", record[:8])[0]
            year = struct.unpack("<h", record[12:14])[0]
            if not math.isfinite(seconds):
                raise ValueError("Siglent frame timestamp seconds must be finite.")
            whole_seconds = int(seconds)
            fractional_ns = int(round((seconds - whole_seconds) * 1_000_000_000))
            if fractional_ns >= 1_000_000_000:
                fractional_ns -= 1_000_000_000
                whole_seconds += 1
            try:
                base = datetime(year, record[11], record[10], record[9], record[8], 0)
            except ValueError as exc:
                raise ValueError("Siglent frame timestamp contains an invalid date.") from exc
            delta = base - self._EPOCH0
            output.append(
                (delta.days * 86_400 + delta.seconds + whole_seconds) * 1_000_000_000
                + fractional_ns
            )
        return tuple(output)

    def _analyze_waveform(
        self,
        waveform: np.ndarray,
        time_axis: np.ndarray,
    ) -> NativePulseAnalysis:
        return analyze_siglent_waveform(waveform, time_axis, self._capture_config)

    def _read_line(self) -> str:
        buffer = bytearray()
        while True:
            value = self._scope.read_bytes(1)
            if value == b"\n":
                return buffer.decode("ascii", "ignore").strip()
            buffer.extend(value)

    def _read_hash_block(self) -> bytes:
        while self._scope.read_bytes(1) != b"#":
            pass
        digit_count = int(self._scope.read_bytes(1).decode("ascii"))
        payload_size = int(self._scope.read_bytes(digit_count).decode("ascii"))
        payload = self._scope.read_bytes(payload_size)
        try:
            self._scope.timeout = 1
            while self._scope.read_bytes(1) in (b"\r", b"\n"):
                pass
        except Exception:
            pass
        finally:
            self._scope.timeout = self._timeout_milliseconds
        return payload

    def _close_transport(self) -> BaseException | None:
        first_error = None
        for resource in (self._scope, self._resource_manager):
            close = getattr(resource, "close", None)
            if close is None:
                continue
            try:
                close()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        self._scope = None
        self._resource_manager = None
        self._state = "stopped"
        self._release_confirmed = first_error is None
        return first_error