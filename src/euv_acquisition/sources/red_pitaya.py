from __future__ import annotations

import importlib
import time
from enum import Enum
from typing import Any, Callable

import numpy as np

from euv_acquisition.models import CaptureConfig, CapturedPulse
from euv_acquisition.pipeline_metrics import PipelineMetrics


class _AcquisitionState(str, Enum):
    STOPPED = "stopped"
    PREFILL = "prefill"
    WAITING_TRIGGER = "waiting_trigger"
    WAITING_BUFFER = "waiting_buffer"


class CaptureMode(str, Enum):
    LEGACY_SINGLE_SHOT = "legacy-single-shot"
    SINGLE_SHOT = "single-shot"
    AXI_CONTINUOUS = "axi-continuous"
    AUTO = "auto"


class RedPitayaPulseSource:
    """Nonblocking hardware pulse source for the STEMlab acquisition API."""

    def __init__(
        self,
        capture_config: CaptureConfig = CaptureConfig(),
        *,
        rp_api: Any | None = None,
        channel: Any | None = None,
        full_buffer_samples: int = 16_384,
        prefill_seconds: float = 0.001,
        debounce_microseconds: float = 1.0,
        capture_mode: CaptureMode | str = CaptureMode.LEGACY_SINGLE_SHOT,
        axi_minimum_buffer_seconds: float = 0.05,
        unix_time_ns: Callable[[], int] = time.time_ns,
        monotonic_time_ns: Callable[[], int] = time.monotonic_ns,
        metrics: PipelineMetrics | None = None,
    ) -> None:
        if isinstance(full_buffer_samples, bool) or not isinstance(full_buffer_samples, int) or full_buffer_samples < 2:
            raise ValueError("full_buffer_samples must be an integer of at least two.")
        if capture_config.window_samples > full_buffer_samples:
            raise ValueError("Capture window must fit inside the Red Pitaya buffer.")
        if prefill_seconds < 0:
            raise ValueError("prefill_seconds must be non-negative.")
        if debounce_microseconds < 0:
            raise ValueError("debounce_microseconds must be non-negative.")
        if axi_minimum_buffer_seconds <= 0:
            raise ValueError("axi_minimum_buffer_seconds must be positive.")

        self._capture_config = capture_config
        self._rp = rp_api
        self._channel = channel
        self._full_buffer_samples = full_buffer_samples
        self._prefill_ns = int(prefill_seconds * 1e9)
        self._debounce_microseconds = debounce_microseconds
        self._requested_capture_mode = CaptureMode(capture_mode)
        self._effective_capture_mode = self._requested_capture_mode
        self._capture_fallback_reason: str | None = None
        self._axi_minimum_buffer_seconds = axi_minimum_buffer_seconds
        self._axi_buffer_samples = 0
        self._axi_enabled = False
        self._unix_time_ns = unix_time_ns
        self._monotonic_time_ns = monotonic_time_ns
        self._metrics = metrics or PipelineMetrics()
        self._buffer = None
        self._state = _AcquisitionState.STOPPED
        self._started_monotonic_ns = 0
        self._state_started_monotonic_ns = 0
        self._triggered_at_unix_ns = 0
        self._triggered_at_monotonic_ns = 0

    @property
    def capture_config(self) -> CaptureConfig:
        return self._capture_config

    @property
    def state(self) -> str:
        return self._state.value

    @property
    def capture_mode(self) -> str:
        return self._effective_capture_mode.value

    @property
    def requested_capture_mode(self) -> str:
        return self._requested_capture_mode.value

    @property
    def effective_capture_mode(self) -> str:
        return self._effective_capture_mode.value

    @property
    def capture_fallback_reason(self) -> str | None:
        return self._capture_fallback_reason

    def set_metrics(self, metrics: PipelineMetrics) -> None:
        self._metrics = metrics

    def open(self) -> None:
        if self._state is not _AcquisitionState.STOPPED:
            raise RuntimeError("Red Pitaya pulse source is already open.")
        if self._rp is None:
            self._rp = importlib.import_module("rp")
        if self._channel is None:
            self._channel = self._rp.RP_CH_1
        self._check("rp_Init", self._rp.rp_Init())
        try:
            self._configure_requested_mode()
        except Exception:
            try:
                self._release_failed_open()
            finally:
                self._state = _AcquisitionState.STOPPED
            raise
        self._metrics.set_capture_mode(
            requested_mode=self.requested_capture_mode,
            effective_mode=self.effective_capture_mode,
            fallback_reason=self.capture_fallback_reason,
        )

    def capture(self) -> CapturedPulse | None:
        if self._state is _AcquisitionState.STOPPED:
            raise RuntimeError("Red Pitaya pulse source is not open.")
        now = self._monotonic_time_ns()

        if self._state is _AcquisitionState.PREFILL:
            if now - self._started_monotonic_ns < self._prefill_ns:
                return None
            self._metrics.record_duration("prefill_wait", now - self._started_monotonic_ns)
            self._check("rp_AcqSetTriggerSrc", self._rp.rp_AcqSetTriggerSrc(self._rp.RP_TRIG_SRC_EXT_PE))
            self._state = _AcquisitionState.WAITING_TRIGGER
            self._state_started_monotonic_ns = self._monotonic_time_ns()
            return None

        if self._state is _AcquisitionState.WAITING_TRIGGER:
            state = self._status_value("rp_AcqGetTriggerState", self._rp.rp_AcqGetTriggerState())
            if state != self._rp.RP_TRIG_STATE_TRIGGERED:
                return None
            self._triggered_at_unix_ns = self._unix_time_ns()
            self._triggered_at_monotonic_ns = self._monotonic_time_ns()
            self._metrics.record_duration(
                "trigger_wait",
                self._triggered_at_monotonic_ns - self._state_started_monotonic_ns,
            )
            self._state = _AcquisitionState.WAITING_BUFFER
            self._state_started_monotonic_ns = self._triggered_at_monotonic_ns
            return None

        if self._state is _AcquisitionState.WAITING_BUFFER:
            if self._effective_capture_mode is CaptureMode.AXI_CONTINUOUS:
                fill_name = "rp_AcqAxiGetBufferFillState"
                fill_result = self._rp.rp_AcqAxiGetBufferFillState(self._channel)
            else:
                fill_name = "rp_AcqGetBufferFillState"
                fill_result = self._rp.rp_AcqGetBufferFillState()
            filled = self._status_value(fill_name, fill_result)
            if not filled:
                return None
            buffer_ready_ns = self._monotonic_time_ns()
            self._metrics.record_duration(
                "buffer_fill_wait",
                buffer_ready_ns - self._state_started_monotonic_ns,
            )
            if self._effective_capture_mode is CaptureMode.AXI_CONTINUOUS:
                pointer_name = "rp_AcqAxiGetWritePointerAtTrig"
                pointer_result = self._rp.rp_AcqAxiGetWritePointerAtTrig(self._channel)
                buffer_samples = self._axi_buffer_samples
            else:
                pointer_name = "rp_AcqGetWritePointerAtTrig"
                pointer_result = self._rp.rp_AcqGetWritePointerAtTrig()
                buffer_samples = self._full_buffer_samples
            trigger_index = self._status_value(pointer_name, pointer_result)
            if isinstance(trigger_index, bool) or not isinstance(trigger_index, int):
                raise RuntimeError(f"{pointer_name} did not return an integer buffer index.")
            if not 0 <= trigger_index < buffer_samples:
                raise RuntimeError(
                    f"{pointer_name} returned out-of-range index {trigger_index}."
                )
            read_started_ns = self._monotonic_time_ns()
            window_start = (trigger_index - self._capture_config.pretrigger_samples) % buffer_samples
            if self._effective_capture_mode is CaptureMode.LEGACY_SINGLE_SHOT:
                self._check(
                    "rp_AcqGetDataV",
                    self._rp.rp_AcqGetDataV(self._channel, 0, self._full_buffer_samples, self._buffer),
                )
            elif self._effective_capture_mode is CaptureMode.SINGLE_SHOT:
                self._check(
                    "rp_AcqGetDataVNP",
                    self._rp.rp_AcqGetDataVNP(self._channel, window_start, self._buffer),
                )
            else:
                self._check(
                    "rp_AcqAxiGetDataVNP",
                    self._rp.rp_AcqAxiGetDataVNP(self._channel, window_start, self._buffer),
                )
            self._metrics.record_duration(
                "hardware_read",
                self._monotonic_time_ns() - read_started_ns,
            )
            copy_started_ns = self._monotonic_time_ns()
            if self._effective_capture_mode is CaptureMode.LEGACY_SINGLE_SHOT:
                values = np.asarray(
                    [
                        self._buffer[(window_start + offset) % self._full_buffer_samples]
                        for offset in range(self._capture_config.window_samples)
                    ],
                    dtype=np.float32,
                )
            else:
                values = self._buffer.copy()
            self._metrics.record_duration(
                "window_copy",
                self._monotonic_time_ns() - copy_started_ns,
            )
            pulse = CapturedPulse(
                samples_v=values,
                captured_at_unix_ns=self._triggered_at_unix_ns,
                captured_at_monotonic_ns=self._triggered_at_monotonic_ns,
            )
            if self._effective_capture_mode is CaptureMode.AXI_CONTINUOUS:
                self._unlock_trigger()
            else:
                self._arm_single_shot()
            return pulse

        raise RuntimeError(f"Unknown Red Pitaya acquisition state {self._state!r}.")

    def close(self) -> None:
        if self._state is _AcquisitionState.STOPPED:
            return
        first_error: Exception | None = None
        try:
            if hasattr(self._rp, "rp_AcqStop"):
                try:
                    self._check("rp_AcqStop", self._rp.rp_AcqStop())
                except Exception as exc:
                    first_error = exc
            if self._axi_enabled:
                for name, call in (
                    ("rp_AcqSetArmKeep", lambda: self._rp.rp_AcqSetArmKeep(False)),
                    ("rp_AcqAxiEnable", lambda: self._rp.rp_AcqAxiEnable(self._channel, False)),
                ):
                    try:
                        self._check(name, call())
                    except Exception as exc:
                        if first_error is None:
                            first_error = exc
        finally:
            try:
                try:
                    self._check("rp_Release", self._rp.rp_Release())
                except Exception as exc:
                    if first_error is None:
                        first_error = exc
            finally:
                self._buffer = None
                self._axi_enabled = False
                self._axi_buffer_samples = 0
                self._state = _AcquisitionState.STOPPED
        if first_error is not None:
            raise first_error

    def _configure_requested_mode(self) -> None:
        requested = self._requested_capture_mode
        self._capture_fallback_reason = None
        if requested is CaptureMode.AUTO:
            try:
                self._effective_capture_mode = CaptureMode.AXI_CONTINUOUS
                self._configure_axi()
                return
            except Exception as exc:
                self._capture_fallback_reason = f"AXI unavailable: {type(exc).__name__}: {exc}"
                self._disable_axi_best_effort()
            if hasattr(self._rp, "rp_AcqGetDataVNP"):
                self._effective_capture_mode = CaptureMode.SINGLE_SHOT
                self._buffer = np.empty(self._capture_config.window_samples, dtype=np.float32)
            else:
                self._capture_fallback_reason += "; rp_AcqGetDataVNP unavailable"
                self._effective_capture_mode = CaptureMode.LEGACY_SINGLE_SHOT
                self._buffer = self._rp.fBuffer(self._full_buffer_samples)
            self._arm_single_shot()
            return
        self._effective_capture_mode = requested
        if requested is CaptureMode.AXI_CONTINUOUS:
            self._configure_axi()
            return
        if requested is CaptureMode.SINGLE_SHOT:
            if not hasattr(self._rp, "rp_AcqGetDataVNP"):
                raise RuntimeError("single-shot mode requires rp_AcqGetDataVNP.")
            self._buffer = np.empty(self._capture_config.window_samples, dtype=np.float32)
        else:
            self._buffer = self._rp.fBuffer(self._full_buffer_samples)
        self._arm_single_shot()

    def _configure_axi(self) -> None:
        required = (
            "rp_AcqAxiGetMemoryRegion",
            "rp_AcqAxiSetTriggerDelay",
            "rp_AcqAxiSetBufferSamples",
            "rp_AcqAxiEnable",
            "rp_AcqAxiGetBufferFillState",
            "rp_AcqAxiGetWritePointerAtTrig",
            "rp_AcqAxiGetDataVNP",
            "rp_AcqSetArmKeep",
            "rp_AcqUnlockTrigger",
        )
        missing = [name for name in required if not hasattr(self._rp, name)]
        if not hasattr(self._rp, "rp_AcqAxiSetDecimationFactor") and not hasattr(
            self._rp, "rp_AcqAxiSetDecimationFactorCh"
        ):
            missing.append("rp_AcqAxiSetDecimationFactor")
        if missing:
            raise RuntimeError(f"Missing Red Pitaya AXI API symbol(s): {', '.join(missing)}")

        region = self._rp.rp_AcqAxiGetMemoryRegion()
        if not isinstance(region, (tuple, list)) or len(region) < 3:
            raise RuntimeError("rp_AcqAxiGetMemoryRegion did not return status, start, and size.")
        self._check("rp_AcqAxiGetMemoryRegion", region)
        start, size_bytes = region[1], region[2]
        if any(isinstance(value, bool) or not isinstance(value, int) for value in (start, size_bytes)):
            raise RuntimeError("rp_AcqAxiGetMemoryRegion returned non-integer bounds.")
        samples = (size_bytes // 2 // 8) * 8
        minimum_samples = max(
            self._capture_config.window_samples,
            int(self._capture_config.sample_rate_hz * self._axi_minimum_buffer_seconds),
        )
        if start < 8 or samples < minimum_samples:
            raise RuntimeError(
                f"Reserved AXI memory provides {samples} samples; at least {minimum_samples} are required."
            )

        self._buffer = np.empty(self._capture_config.window_samples, dtype=np.float32)
        self._axi_buffer_samples = samples
        self._check("rp_AcqReset", self._rp.rp_AcqReset())
        self._check("rp_AcqSetDecimation", self._rp.rp_AcqSetDecimation(self._rp.RP_DEC_1))
        if hasattr(self._rp, "rp_AcqAxiSetDecimationFactorCh"):
            self._check(
                "rp_AcqAxiSetDecimationFactorCh",
                self._rp.rp_AcqAxiSetDecimationFactorCh(self._channel, 1),
            )
        else:
            self._check("rp_AcqAxiSetDecimationFactor", self._rp.rp_AcqAxiSetDecimationFactor(1))
        posttrigger_samples = self._capture_config.window_samples - self._capture_config.pretrigger_samples
        self._check(
            "rp_AcqAxiSetTriggerDelay",
            self._rp.rp_AcqAxiSetTriggerDelay(self._channel, posttrigger_samples),
        )
        self._check(
            "rp_AcqAxiSetBufferSamples",
            self._rp.rp_AcqAxiSetBufferSamples(self._channel, start, samples),
        )
        self._check("rp_AcqAxiEnable", self._rp.rp_AcqAxiEnable(self._channel, True))
        self._axi_enabled = True
        self._check("rp_AcqSetArmKeep", self._rp.rp_AcqSetArmKeep(True))
        if hasattr(self._rp, "rp_AcqSetExtTriggerDebouncerUs"):
            self._check(
                "rp_AcqSetExtTriggerDebouncerUs",
                self._rp.rp_AcqSetExtTriggerDebouncerUs(self._debounce_microseconds),
            )
        started_ns = self._monotonic_time_ns()
        self._check("rp_AcqStart", self._rp.rp_AcqStart())
        self._started_monotonic_ns = self._monotonic_time_ns()
        self._state_started_monotonic_ns = self._started_monotonic_ns
        self._state = _AcquisitionState.PREFILL
        self._metrics.record_duration("rearm", self._started_monotonic_ns - started_ns)

    def _arm_single_shot(self) -> None:
        started_ns = self._monotonic_time_ns()
        self._check("rp_AcqReset", self._rp.rp_AcqReset())
        self._check("rp_AcqSetDecimation", self._rp.rp_AcqSetDecimation(self._rp.RP_DEC_1))
        if (
            self._effective_capture_mode is CaptureMode.SINGLE_SHOT
            and hasattr(self._rp, "rp_AcqSetTriggerDelayDirect")
        ):
            posttrigger_samples = self._capture_config.window_samples - self._capture_config.pretrigger_samples
            self._check(
                "rp_AcqSetTriggerDelayDirect",
                self._rp.rp_AcqSetTriggerDelayDirect(posttrigger_samples),
            )
        else:
            self._check("rp_AcqSetTriggerDelay", self._rp.rp_AcqSetTriggerDelay(0))
        if hasattr(self._rp, "rp_AcqSetExtTriggerDebouncerUs"):
            self._check(
                "rp_AcqSetExtTriggerDebouncerUs",
                self._rp.rp_AcqSetExtTriggerDebouncerUs(self._debounce_microseconds),
            )
        self._check("rp_AcqStart", self._rp.rp_AcqStart())
        self._started_monotonic_ns = self._monotonic_time_ns()
        self._state_started_monotonic_ns = self._started_monotonic_ns
        self._state = _AcquisitionState.PREFILL
        self._metrics.record_duration("rearm", self._started_monotonic_ns - started_ns)

    def _unlock_trigger(self) -> None:
        started_ns = self._monotonic_time_ns()
        self._check("rp_AcqUnlockTrigger", self._rp.rp_AcqUnlockTrigger())
        self._state_started_monotonic_ns = self._monotonic_time_ns()
        self._state = _AcquisitionState.WAITING_TRIGGER
        self._metrics.record_duration("rearm", self._state_started_monotonic_ns - started_ns)

    def _disable_axi_best_effort(self) -> None:
        if hasattr(self._rp, "rp_AcqStop"):
            self._rp.rp_AcqStop()
        if hasattr(self._rp, "rp_AcqSetArmKeep"):
            self._rp.rp_AcqSetArmKeep(False)
        if hasattr(self._rp, "rp_AcqAxiEnable"):
            self._rp.rp_AcqAxiEnable(self._channel, False)
        self._axi_enabled = False
        self._axi_buffer_samples = 0
        self._state = _AcquisitionState.STOPPED

    def _release_failed_open(self) -> None:
        self._disable_axi_best_effort()
        self._rp.rp_Release()
        self._buffer = None

    def _check(self, name: str, result: Any) -> None:
        code = result[0] if isinstance(result, (tuple, list)) else result
        if code != self._rp.RP_OK:
            raise RuntimeError(f"{name} failed with code {code}")

    def _status_value(self, name: str, result: Any) -> Any:
        if not isinstance(result, (tuple, list)) or len(result) < 2:
            raise RuntimeError(f"{name} did not return a status/value pair.")
        self._check(name, result)
        return result[1]