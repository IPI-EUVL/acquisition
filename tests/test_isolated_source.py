import multiprocessing
import os
import queue
import threading
import time
from functools import partial
from uuid import uuid4

import numpy as np
import pytest

from euv_acquisition.models import (
    CaptureConfig,
    CapturedPulse,
    NativePulseAnalysis,
    PulseQuality,
    SourceBatchEnvelope,
    SourceCaptureBatch,
)
from euv_acquisition.sources.isolated import (
    CaptureProcessConfig,
    IsolatedPulseSource,
    _WorkerPulse,
)


class _ProcessTestSource:
    def __init__(self, *, fail: bool = False) -> None:
        self.capture_config = CaptureConfig(
            sample_rate_hz=1_000_000.0,
            window_seconds=4e-6,
            pretrigger_seconds=1e-6,
        )
        self.requested_capture_mode = "test-requested"
        self.effective_capture_mode = "test-effective"
        self.capture_fallback_reason = "test fallback"
        self._metrics = None
        self._sequence = 0
        self._fail = fail

    def set_metrics(self, metrics) -> None:
        self._metrics = metrics

    def open(self) -> None:
        return

    def capture(self) -> CapturedPulse | None:
        if self._fail:
            raise RuntimeError("test capture failure")
        if self._sequence >= 2:
            return None
        self._metrics.record_duration("hardware_read", self._sequence + 1)
        sequence = self._sequence
        self._sequence += 1
        return CapturedPulse(
            np.asarray([sequence, 0.1, 0.2, 0.3], dtype=np.float32),
            1_000 + sequence,
            2_000 + sequence,
        )

    def close(self) -> None:
        return


class _TimedProcessTestSource(_ProcessTestSource):
    def __init__(self) -> None:
        super().__init__()
        self._next_pulse = 0.0

    def open(self) -> None:
        self._next_pulse = time.monotonic()

    def capture(self) -> CapturedPulse | None:
        if self._sequence >= 24 or time.monotonic() < self._next_pulse:
            return None
        sequence = self._sequence
        self._sequence += 1
        self._next_pulse += 0.01
        return CapturedPulse(
            np.asarray([sequence, 0.1, 0.2, 0.3], dtype=np.float32),
            1_000 + sequence,
            2_000 + sequence,
        )


class _PulseThenFailureSource(_ProcessTestSource):
    def capture(self) -> CapturedPulse | None:
        if self._sequence >= 3:
            raise RuntimeError("failure after queued pulses")
        sequence = self._sequence
        self._sequence += 1
        return CapturedPulse(
            np.asarray([sequence, 0.1, 0.2, 0.3], dtype=np.float32),
            1_000 + sequence,
            2_000 + sequence,
        )


class _BurstProcessTestSource(_ProcessTestSource):
    def capture(self) -> CapturedPulse:
        sequence = self._sequence
        self._sequence += 1
        return CapturedPulse(
            np.asarray([sequence, 0.1, 0.2, 0.3], dtype=np.float32),
            1_000 + sequence,
            2_000 + sequence,
        )


class _CloseFailureSource(_ProcessTestSource):
    def capture(self) -> None:
        return None

    def close(self) -> None:
        raise RuntimeError("release confirmation fixture")


class _CooperativeStopSource(_ProcessTestSource):
    def __init__(self, capture_started) -> None:
        super().__init__()
        self._capture_started = capture_started
        self._stop_requested = lambda: False

    def set_stop_requested(self, stop_requested) -> None:
        self._stop_requested = stop_requested

    def capture(self) -> None:
        self._capture_started.set()
        while not self._stop_requested():
            time.sleep(0.001)


class _BatchProcessTestSource(_ProcessTestSource):
    def capture(self) -> SourceCaptureBatch | None:
        if self._sequence:
            return None
        self._sequence = 1
        analysis = NativePulseAnalysis(
            baseline_volts=0.0,
            integral_volt_seconds=1.234567890123456e-9,
            minimum_volts=0.0,
            maximum_volts=0.3,
            peak_absolute_volts=0.3,
            quality=PulseQuality.OK,
            algorithm_version="source-batch-test-v1",
        )
        return SourceCaptureBatch(
            tuple(
                CapturedPulse(
                    np.asarray([0.0, 0.1, 0.2, 0.3], dtype=np.float64),
                    timestamp,
                    timestamp + 1_000,
                    native_analysis=analysis,
                )
                for timestamp in (100, 200)
            ),
            SourceBatchEnvelope(uuid4(), "test_sequence", 50, 250),
        )


def _make_process_test_source() -> _ProcessTestSource:
    return _ProcessTestSource()


def _make_failing_process_test_source() -> _ProcessTestSource:
    return _ProcessTestSource(fail=True)


def _make_timed_process_test_source() -> _TimedProcessTestSource:
    return _TimedProcessTestSource()


def _make_pulse_then_failure_source() -> _PulseThenFailureSource:
    return _PulseThenFailureSource()


def _make_burst_process_test_source() -> _BurstProcessTestSource:
    return _BurstProcessTestSource()


def _make_close_failure_source() -> _CloseFailureSource:
    return _CloseFailureSource()


def _make_batch_process_test_source() -> _BatchProcessTestSource:
    return _BatchProcessTestSource()


def _process_config() -> CaptureProcessConfig:
    return CaptureProcessConfig(
        cpu=None,
        realtime_priority=None,
        poll_seconds=0.001,
        startup_timeout_seconds=5.0,
        shutdown_timeout_seconds=2.0,
    )


def test_isolated_source_captures_in_spawned_process_and_forwards_metrics() -> None:
    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=4e-6, pretrigger_seconds=1e-6)
    source = IsolatedPulseSource(
        _make_process_test_source,
        config,
        requested_capture_mode="test-requested",
        process_config=_process_config(),
    )

    source.open()
    worker_pid = source.worker_pid
    pulses = []
    try:
        deadline = time.monotonic() + 5.0
        while len(pulses) < 2 and time.monotonic() < deadline:
            pulse = source.capture()
            if pulse is None:
                time.sleep(0.001)
            else:
                pulses.append(pulse)
    finally:
        source.close()

    assert worker_pid is not None and worker_pid != os.getpid()
    assert source.state == "stopped"
    assert source.effective_capture_mode == "test-effective"
    assert source.capture_fallback_reason == "test fallback"
    assert [pulse.captured_at_unix_ns for pulse in pulses] == [1_000, 1_001]
    np.testing.assert_array_equal(
        pulses[1].samples_v,
        np.asarray([1.0, 0.1, 0.2, 0.3], dtype=np.float32),
    )
    assert source._metrics.snapshot()["stages"]["hardware_read"]["count"] == 2


def test_isolated_source_preserves_atomic_source_batch() -> None:
    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=4e-6, pretrigger_seconds=1e-6)
    source = IsolatedPulseSource(
        _make_batch_process_test_source,
        config,
        requested_capture_mode="test-batch",
        process_config=_process_config(),
    )

    source.open()
    captured = None
    try:
        deadline = time.monotonic() + 5.0
        while captured is None and time.monotonic() < deadline:
            captured = source.capture()
            if captured is None:
                time.sleep(0.001)
    finally:
        source.close()

    assert isinstance(captured, SourceCaptureBatch)
    assert captured.envelope.batch_kind == "test_sequence"
    assert [pulse.captured_at_unix_ns for pulse in captured.pulses] == [100, 200]
    assert captured.pulses[0].native_analysis.integral_volt_seconds == 1.234567890123456e-9


def test_isolated_source_reports_capture_worker_failure() -> None:
    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=4e-6, pretrigger_seconds=1e-6)
    source = IsolatedPulseSource(
        _make_failing_process_test_source,
        config,
        requested_capture_mode="test-requested",
        process_config=_process_config(),
    )
    source.open()
    try:
        deadline = time.monotonic() + 5.0
        while True:
            with pytest.raises(RuntimeError, match="capture.*test capture failure"):
                while time.monotonic() < deadline:
                    source.capture()
                    time.sleep(0.001)
                raise AssertionError("Capture worker failure was not reported.")
            break
    finally:
        source.close()


def test_isolated_source_buffers_snapshot_length_parent_stall() -> None:
    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=4e-6, pretrigger_seconds=1e-6)
    source = IsolatedPulseSource(
        _make_timed_process_test_source,
        config,
        requested_capture_mode="test-requested",
        process_config=_process_config(),
    )
    source.open()
    pulses = []
    try:
        time.sleep(0.28)
        deadline = time.monotonic() + 5.0
        while len(pulses) < 24 and time.monotonic() < deadline:
            pulse = source.capture()
            if pulse is None:
                time.sleep(0.001)
            else:
                pulses.append(pulse)
    finally:
        source.close()

    assert [pulse.captured_at_unix_ns for pulse in pulses] == list(range(1_000, 1_024))


def test_isolated_source_delivers_queued_pulses_before_worker_failure() -> None:
    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=4e-6, pretrigger_seconds=1e-6)
    source = IsolatedPulseSource(
        _make_pulse_then_failure_source,
        config,
        requested_capture_mode="test-requested",
        process_config=_process_config(),
    )
    source.open()
    pulses = []
    try:
        deadline = time.monotonic() + 5.0
        with pytest.raises(RuntimeError, match="failure after queued pulses"):
            while time.monotonic() < deadline:
                pulse = source.capture()
                if pulse is None:
                    time.sleep(0.001)
                else:
                    pulses.append(pulse)
    finally:
        source.close()

    assert [pulse.captured_at_unix_ns for pulse in pulses] == [1_000, 1_001, 1_002]


def test_isolated_source_close_preserves_captured_backlog_for_drain() -> None:
    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=4e-6, pretrigger_seconds=1e-6)
    source = IsolatedPulseSource(
        _make_timed_process_test_source,
        config,
        requested_capture_mode="test-requested",
        process_config=_process_config(),
    )
    source.open()
    time.sleep(0.28)
    source.close()

    pulses = source.drain_captured()

    assert [pulse.captured_at_unix_ns for pulse in pulses] == list(range(1_000, 1_024))


def test_isolated_source_cooperatively_stops_a_blocked_capture() -> None:
    context = multiprocessing.get_context("spawn")
    capture_started = context.Event()
    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=4e-6, pretrigger_seconds=1e-6)
    source = IsolatedPulseSource(
        partial(_CooperativeStopSource, capture_started),
        config,
        requested_capture_mode="test-requested",
        process_config=_process_config(),
        process_context=context,
    )

    source.open()
    assert capture_started.wait(5.0)
    source.close()

    assert source.state == "stopped"


def test_isolated_source_failed_fence_restores_dequeued_pulses() -> None:
    class _AliveProcess:
        @staticmethod
        def is_alive() -> bool:
            return True

    class _CommandConnection:
        @staticmethod
        def send(_token: int) -> None:
            return

    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=4e-6, pretrigger_seconds=1e-6)
    source = IsolatedPulseSource(
        _make_process_test_source,
        config,
        requested_capture_mode="test-requested",
        process_config=CaptureProcessConfig(
            cpu=None,
            realtime_priority=None,
            shutdown_timeout_seconds=0.01,
        ),
    )
    source._state = "running"
    source._process = _AliveProcess()
    source._command_connection = _CommandConnection()
    source._outbox = queue.Queue()
    source._outbox.put(
        _WorkerPulse(
            np.asarray([1.0, 0.1, 0.2, 0.3], dtype=np.float32).tobytes(),
            1_000,
            2_000,
            (),
        )
    )

    with pytest.raises(TimeoutError, match="flush fence"):
        source.capture_fence()

    source._state = "stopped"
    pulses = source.drain_captured()
    assert [pulse.captured_at_unix_ns for pulse in pulses] == [1_000]


def test_isolated_source_cannot_reopen_after_worker_survives_kill() -> None:
    class _UnkillableProcess:
        def is_alive(self) -> bool:
            return True

        def join(self, timeout=None) -> None:
            return

        def terminate(self) -> None:
            return

        def kill(self) -> None:
            return

    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=4e-6, pretrigger_seconds=1e-6)
    source = IsolatedPulseSource(
        _make_process_test_source,
        config,
        requested_capture_mode="test-requested",
        process_config=CaptureProcessConfig(
            cpu=None,
            realtime_priority=None,
            shutdown_timeout_seconds=0.01,
        ),
    )
    source._state = "running"
    source._process = _UnkillableProcess()
    source._stop_event = threading.Event()

    with pytest.raises(RuntimeError, match="remained alive after SIGKILL"):
        source.close()

    assert source.state == "failed"
    with pytest.raises(RuntimeError, match="restart the acquisition service"):
        source.open()


def test_isolated_source_cannot_reopen_after_successful_forced_kill() -> None:
    class _KilledProcess:
        def __init__(self) -> None:
            self.alive = True

        def is_alive(self) -> bool:
            return self.alive

        def join(self, timeout=None) -> None:
            return

        def terminate(self) -> None:
            return

        def kill(self) -> None:
            self.alive = False

        def close(self) -> None:
            return

    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=4e-6, pretrigger_seconds=1e-6)
    source = IsolatedPulseSource(
        _make_process_test_source,
        config,
        requested_capture_mode="test-requested",
        process_config=CaptureProcessConfig(
            cpu=None,
            realtime_priority=None,
            shutdown_timeout_seconds=0.01,
        ),
    )
    source._state = "running"
    source._process = _KilledProcess()
    source._stop_event = threading.Event()

    with pytest.raises(RuntimeError, match="hardware release was not confirmed"):
        source.close()

    assert source.state == "failed"
    with pytest.raises(RuntimeError, match="restart the acquisition service"):
        source.open()


def test_isolated_source_retains_pulse_that_overflows_ipc_queue() -> None:
    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=4e-6, pretrigger_seconds=1e-6)
    source = IsolatedPulseSource(
        _make_burst_process_test_source,
        config,
        requested_capture_mode="test-requested",
        process_config=CaptureProcessConfig(
            cpu=None,
            realtime_priority=None,
            queue_capacity=1,
            queue_timeout_seconds=0.02,
            startup_timeout_seconds=5.0,
            shutdown_timeout_seconds=2.0,
        ),
    )
    source.open()
    pulses = []
    try:
        time.sleep(0.1)
        deadline = time.monotonic() + 5.0
        with pytest.raises(RuntimeError, match="Capture IPC queue reached its capacity"):
            while time.monotonic() < deadline:
                pulse = source.capture()
                if pulse is None:
                    time.sleep(0.001)
                else:
                    pulses.append(pulse)
    finally:
        source.close()

    assert [pulse.captured_at_unix_ns for pulse in pulses] == [1_000, 1_001]
    assert source._metrics.snapshot()["counters"]["capture_ipc_overflow"] == 1


def test_isolated_source_latches_unconfirmed_release() -> None:
    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=4e-6, pretrigger_seconds=1e-6)
    source = IsolatedPulseSource(
        _make_close_failure_source,
        config,
        requested_capture_mode="test-requested",
        process_config=_process_config(),
    )
    source.open()

    with pytest.raises(RuntimeError, match="hardware release was not confirmed"):
        source.close()

    assert source.state == "failed"
    with pytest.raises(RuntimeError, match="restart the acquisition service"):
        source.open()