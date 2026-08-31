from __future__ import annotations

import importlib
import time
from enum import Enum
from typing import Any, Callable

import numpy as np

from euv_acquisition.models import CaptureConfig, CapturedPulse


class _AcquisitionState(str, Enum):
    STOPPED = "stopped"
    PREFILL = "prefill"
    WAITING_TRIGGER = "waiting_trigger"
    WAITING_BUFFER = "waiting_buffer"


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
        unix_time_ns: Callable[[], int] = time.time_ns,
        monotonic_time_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        if isinstance(full_buffer_samples, bool) or not isinstance(full_buffer_samples, int) or full_buffer_samples < 2:
            raise ValueError("full_buffer_samples must be an integer of at least two.")
        if capture_config.window_samples > full_buffer_samples:
            raise ValueError("Capture window must fit inside the Red Pitaya buffer.")
        if prefill_seconds < 0:
            raise ValueError("prefill_seconds must be non-negative.")
        if debounce_microseconds < 0:
            raise ValueError("debounce_microseconds must be non-negative.")

        self._capture_config = capture_config
        self._rp = rp_api
        self._channel = channel
        self._full_buffer_samples = full_buffer_samples
        self._prefill_ns = int(prefill_seconds * 1e9)
        self._debounce_microseconds = debounce_microseconds
        self._unix_time_ns = unix_time_ns
        self._monotonic_time_ns = monotonic_time_ns
        self._buffer = None
        self._state = _AcquisitionState.STOPPED
        self._started_monotonic_ns = 0

    @property
    def capture_config(self) -> CaptureConfig:
        return self._capture_config

    @property
    def state(self) -> str:
        return self._state.value

    def open(self) -> None:
        if self._state is not _AcquisitionState.STOPPED:
            raise RuntimeError("Red Pitaya pulse source is already open.")
        if self._rp is None:
            self._rp = importlib.import_module("rp")
        if self._channel is None:
            self._channel = self._rp.RP_CH_1
        self._check("rp_Init", self._rp.rp_Init())
        self._buffer = self._rp.fBuffer(self._full_buffer_samples)
        self._arm()

    def capture(self) -> CapturedPulse | None:
        if self._state is _AcquisitionState.STOPPED:
            raise RuntimeError("Red Pitaya pulse source is not open.")
        now = self._monotonic_time_ns()

        if self._state is _AcquisitionState.PREFILL:
            if now - self._started_monotonic_ns < self._prefill_ns:
                return None
            self._check("rp_AcqSetTriggerSrc", self._rp.rp_AcqSetTriggerSrc(self._rp.RP_TRIG_SRC_EXT_PE))
            self._state = _AcquisitionState.WAITING_TRIGGER
            return None

        if self._state is _AcquisitionState.WAITING_TRIGGER:
            state = self._status_value("rp_AcqGetTriggerState", self._rp.rp_AcqGetTriggerState())
            if state != self._rp.RP_TRIG_STATE_TRIGGERED:
                return None
            self._state = _AcquisitionState.WAITING_BUFFER
            return None

        if self._state is _AcquisitionState.WAITING_BUFFER:
            filled = self._status_value("rp_AcqGetBufferFillState", self._rp.rp_AcqGetBufferFillState())
            if not filled:
                return None
            trigger_index = self._status_value(
                "rp_AcqGetWritePointerAtTrig",
                self._rp.rp_AcqGetWritePointerAtTrig(),
            )
            if isinstance(trigger_index, bool) or not isinstance(trigger_index, int):
                raise RuntimeError("rp_AcqGetWritePointerAtTrig did not return an integer buffer index.")
            if not 0 <= trigger_index < self._full_buffer_samples:
                raise RuntimeError(
                    f"rp_AcqGetWritePointerAtTrig returned out-of-range index {trigger_index}."
                )
            self._check(
                "rp_AcqGetDataV",
                self._rp.rp_AcqGetDataV(self._channel, 0, self._full_buffer_samples, self._buffer),
            )
            window_start = (trigger_index - self._capture_config.pretrigger_samples) % self._full_buffer_samples
            values = np.asarray(
                [
                    self._buffer[(window_start + offset) % self._full_buffer_samples]
                    for offset in range(self._capture_config.window_samples)
                ],
                dtype=np.float32,
            )
            pulse = CapturedPulse(
                samples_v=values,
                captured_at_unix_ns=self._unix_time_ns(),
                captured_at_monotonic_ns=self._monotonic_time_ns(),
            )
            self._arm()
            return pulse

        raise RuntimeError(f"Unknown Red Pitaya acquisition state {self._state!r}.")

    def close(self) -> None:
        if self._state is _AcquisitionState.STOPPED:
            return
        try:
            if hasattr(self._rp, "rp_AcqStop"):
                self._check("rp_AcqStop", self._rp.rp_AcqStop())
        finally:
            try:
                self._check("rp_Release", self._rp.rp_Release())
            finally:
                self._buffer = None
                self._state = _AcquisitionState.STOPPED

    def _arm(self) -> None:
        self._check("rp_AcqReset", self._rp.rp_AcqReset())
        self._check("rp_AcqSetDecimation", self._rp.rp_AcqSetDecimation(self._rp.RP_DEC_1))
        self._check("rp_AcqSetTriggerDelay", self._rp.rp_AcqSetTriggerDelay(0))
        if hasattr(self._rp, "rp_AcqSetExtTriggerDebouncerUs"):
            self._check(
                "rp_AcqSetExtTriggerDebouncerUs",
                self._rp.rp_AcqSetExtTriggerDebouncerUs(self._debounce_microseconds),
            )
        self._check("rp_AcqStart", self._rp.rp_AcqStart())
        self._started_monotonic_ns = self._monotonic_time_ns()
        self._state = _AcquisitionState.PREFILL

    def _check(self, name: str, result: Any) -> None:
        code = result[0] if isinstance(result, (tuple, list)) else result
        if code != self._rp.RP_OK:
            raise RuntimeError(f"{name} failed with code {code}")

    def _status_value(self, name: str, result: Any) -> Any:
        if not isinstance(result, (tuple, list)) or len(result) < 2:
            raise RuntimeError(f"{name} did not return a status/value pair.")
        self._check(name, result)
        return result[1]