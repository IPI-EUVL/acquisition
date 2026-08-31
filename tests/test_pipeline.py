import threading
import time
from collections import deque
from uuid import uuid4

import numpy as np
import pytest

import euv_acquisition.session as session_module
from euv_acquisition.models import CaptureConfig, CapturedPulse, SnapshotCloseReason
from euv_acquisition.pipeline import CapturePipeline, PipelineConfig
from euv_acquisition.session import CaptureEngine, CaptureUpdate, RotationConfig, SpoolRepository
from euv_acquisition.snapshot import SnapshotStore


class QueuePulseSource:
    def __init__(self, timestamps) -> None:
        self.capture_config = CaptureConfig(
            sample_rate_hz=1_000_000.0,
            window_seconds=4e-6,
            pretrigger_seconds=1e-6,
        )
        self._timestamps = deque(timestamps)
        self._open = False
        self.captured_count = 0
        self.closed = threading.Event()
        self.owner_threads: list[str] = []

    def open(self) -> None:
        self.owner_threads.append(threading.current_thread().name)
        self._open = True

    def capture(self) -> CapturedPulse | None:
        self.owner_threads.append(threading.current_thread().name)
        if not self._open:
            raise RuntimeError("closed")
        if not self._timestamps:
            return None
        timestamp = self._timestamps.popleft()
        self.captured_count += 1
        return CapturedPulse(
            np.array([0.0, 0.2, 0.2, 0.0], dtype=np.float32),
            timestamp,
            timestamp,
        )

    def close(self) -> None:
        self.owner_threads.append(threading.current_thread().name)
        self._open = False
        self.closed.set()


def _make_pipeline(tmp_path, timestamps, *, rotation, config):
    source = QueuePulseSource(timestamps)
    store = SnapshotStore(tmp_path / "spool")
    spool = SpoolRepository(tmp_path / "spool", server_boot_id=uuid4())
    engine = CaptureEngine(
        source,
        store,
        spool,
        source_kind="simulated",
        source_id="pipeline-test",
        rotation=rotation,
    )
    updates: list[CaptureUpdate] = []
    update_lock = threading.Lock()

    def emit(update: CaptureUpdate) -> None:
        with update_lock:
            updates.append(update)

    pipeline = CapturePipeline(engine, config, emit_update=emit)
    return pipeline, source, store, spool, updates


def _wait_until(predicate, timeout=1.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.005)
    assert predicate()


def test_capture_continues_while_persistence_is_blocked_and_stays_ordered(tmp_path, monkeypatch) -> None:
    pipeline, source, store, spool, updates = _make_pipeline(
        tmp_path,
        range(1, 9),
        rotation=RotationConfig(pulse_limit=1, trigger_idle_seconds=100.0),
        config=PipelineConfig(capture_queue_capacity=16, persistence_queue_capacity=16),
    )
    write_started = threading.Event()
    release_write = threading.Event()
    real_write = store.write

    def blocking_write(*args, **kwargs):
        if not write_started.is_set():
            write_started.set()
            assert release_write.wait(1.0)
        return real_write(*args, **kwargs)

    monkeypatch.setattr(store, "write", blocking_write)
    pipeline.start_capture(uuid4())
    try:
        assert write_started.wait(1.0)
        _wait_until(lambda: source.captured_count == 8)
        assert pipeline.engine.metrics.snapshot()["counters"]["accepted"] == 8

        release_write.set()
        _wait_until(lambda: pipeline.engine.metrics.snapshot()["counters"].get("persisted") == 8)
        pipeline.stop_capture("test complete")

        reports = [update.report for update in updates if update.report is not None]
        manifests = [manifest for update in updates for manifest in update.closed_snapshots]
        assert [report.sequence for report in reports] == list(range(8))
        assert [manifest.final_sequence for manifest in manifests] == list(range(8))
        assert updates[-1].stop_reason == "test complete"
        assert spool.load().final_sequence == 7
        assert set(source.owner_threads) == {"euv-capture-producer"}
    finally:
        release_write.set()
        pipeline.shutdown()


def test_flush_waits_until_the_ordered_batch_is_persisted(tmp_path) -> None:
    now_ns = time.monotonic_ns()
    pipeline, _source, _store, spool, updates = _make_pipeline(
        tmp_path,
        (now_ns, now_ns + 1),
        rotation=RotationConfig(pulse_limit=250, trigger_idle_seconds=100.0),
        config=PipelineConfig(),
    )
    pipeline.start_capture(uuid4())
    try:
        _wait_until(lambda: pipeline.engine.metrics.snapshot()["counters"].get("analyzed") == 2)

        manifest = pipeline.flush()

        assert manifest is not None
        assert manifest.close_reason is SnapshotCloseReason.EXPLICIT_FLUSH
        assert (manifest.first_sequence, manifest.final_sequence) == (0, 1)
        assert spool.load().snapshots[-1].manifest == manifest
        assert any(manifest in update.closed_snapshots for update in updates)
        pipeline.stop_capture()
    finally:
        pipeline.shutdown()


def test_capture_queue_overflow_stops_intake_and_drains_accepted_pulses(tmp_path, monkeypatch) -> None:
    pipeline, source, _store, spool, updates = _make_pipeline(
        tmp_path,
        range(1, 100),
        rotation=RotationConfig(pulse_limit=250, trigger_idle_seconds=100.0),
        config=PipelineConfig(capture_queue_capacity=2, persistence_queue_capacity=2),
    )
    analysis_started = threading.Event()
    release_analysis = threading.Event()
    real_analyze = session_module.analyze_pulse

    def blocking_analyze(*args, **kwargs):
        if not analysis_started.is_set():
            analysis_started.set()
            assert release_analysis.wait(1.0)
        return real_analyze(*args, **kwargs)

    monkeypatch.setattr(session_module, "analyze_pulse", blocking_analyze)
    pipeline.start_capture(uuid4())
    try:
        assert analysis_started.wait(1.0)
        assert source.closed.wait(1.0)
        accepted = pipeline.engine.metrics.snapshot()["counters"]["accepted"]
        assert accepted >= 2

        release_analysis.set()
        pipeline.abort("wait for the existing overflow abort")

        metrics = pipeline.engine.metrics.snapshot()
        assert metrics["counters"]["capture_queue_overflow"] == 1
        assert metrics["counters"]["accepted"] == accepted
        assert metrics["counters"]["analyzed"] == accepted
        assert metrics["counters"]["persisted"] == accepted
        assert metrics["terminal_error"].startswith("Capture queue reached its capacity")
        assert spool.load().final_sequence == accepted - 1
        assert [update.report.sequence for update in updates if update.report is not None] == list(range(accepted))
    finally:
        release_analysis.set()
        pipeline.shutdown()


def test_persistence_queue_overflow_stops_intake_and_drains_accepted_pulses(
    tmp_path,
    monkeypatch,
) -> None:
    pipeline, source, store, spool, _updates = _make_pipeline(
        tmp_path,
        range(1, 9),
        rotation=RotationConfig(pulse_limit=1, trigger_idle_seconds=100.0),
        config=PipelineConfig(capture_queue_capacity=16, persistence_queue_capacity=2),
    )
    write_started = threading.Event()
    release_write = threading.Event()
    real_write = store.write

    def blocking_write(*args, **kwargs):
        if not write_started.is_set():
            write_started.set()
            assert release_write.wait(1.0)
        return real_write(*args, **kwargs)

    monkeypatch.setattr(store, "write", blocking_write)
    pipeline.start_capture(uuid4())
    try:
        assert write_started.wait(1.0)
        assert source.closed.wait(1.0)
        accepted = pipeline.engine.metrics.snapshot()["counters"]["accepted"]

        release_write.set()
        pipeline.abort("wait for the existing persistence queue abort")

        metrics = pipeline.engine.metrics.snapshot()
        assert metrics["counters"]["persistence_queue_overflow"] >= 1
        assert metrics["counters"]["accepted"] == accepted
        assert metrics["counters"]["persisted"] == accepted
        assert metrics["terminal_error"].startswith("Persistence queue reached its capacity")
        assert spool.load().final_sequence == accepted - 1
    finally:
        release_write.set()
        pipeline.shutdown()


def test_analysis_failure_preserves_completed_records_and_accounts_for_unprocessed_pulses(
    tmp_path,
    monkeypatch,
) -> None:
    pipeline, source, _store, spool, updates = _make_pipeline(
        tmp_path,
        range(1, 9),
        rotation=RotationConfig(pulse_limit=250, trigger_idle_seconds=100.0),
        config=PipelineConfig(capture_queue_capacity=16, persistence_queue_capacity=16),
    )
    real_analyze = session_module.analyze_pulse
    analysis_calls = 0

    def fail_second_analysis(*args, **kwargs):
        nonlocal analysis_calls
        analysis_calls += 1
        if analysis_calls == 2:
            raise RuntimeError("analysis fixture failed")
        return real_analyze(*args, **kwargs)

    monkeypatch.setattr(session_module, "analyze_pulse", fail_second_analysis)
    pipeline.start_capture(uuid4())
    try:
        assert source.closed.wait(1.0)
        pipeline.abort("wait for the existing analysis abort")

        metrics = pipeline.engine.metrics.snapshot()
        accepted = metrics["counters"]["accepted"]
        assert metrics["counters"]["analyzed"] == 1
        assert metrics["counters"]["unprocessed_after_analysis_failure"] == accepted - 1
        assert metrics["counters"]["persisted"] == 1
        assert metrics["terminal_error"] == "Analysis stage failure: RuntimeError: analysis fixture failed"
        session = spool.load()
        assert len(session.snapshots) == 1
        assert session.snapshots[0].manifest.final_sequence == 0
        assert updates[-1].stop_reason == metrics["terminal_error"]
    finally:
        pipeline.shutdown()


def test_persistence_failure_preserves_committed_snapshots_and_accounts_for_lost_batches(
    tmp_path,
    monkeypatch,
) -> None:
    pipeline, source, store, spool, updates = _make_pipeline(
        tmp_path,
        range(1, 9),
        rotation=RotationConfig(pulse_limit=1, trigger_idle_seconds=100.0),
        config=PipelineConfig(capture_queue_capacity=16, persistence_queue_capacity=16),
    )
    real_write = store.write
    write_calls = 0

    def fail_second_write(*args, **kwargs):
        nonlocal write_calls
        write_calls += 1
        if write_calls == 2:
            raise OSError("storage fixture failed")
        return real_write(*args, **kwargs)

    monkeypatch.setattr(store, "write", fail_second_write)
    pipeline.start_capture(uuid4())
    try:
        assert source.closed.wait(1.0)
        pipeline.abort("wait for the existing persistence abort")

        metrics = pipeline.engine.metrics.snapshot()
        accepted = metrics["counters"]["accepted"]
        assert metrics["counters"]["persisted"] == 1
        assert metrics["counters"]["persistence_failures"] == 1
        assert metrics["counters"]["unpersisted_after_storage_failure"] == accepted - 1
        assert metrics["terminal_error"] == "Persistence stage failure: OSError: storage fixture failed"
        session = spool.load()
        assert len(session.snapshots) == 1
        assert session.snapshots[0].manifest.final_sequence == 0
        assert updates[-1].stop_reason == metrics["terminal_error"]
    finally:
        pipeline.shutdown()


def test_shutdown_waits_for_persistence_and_drains_every_accepted_pulse(
    tmp_path,
    monkeypatch,
) -> None:
    pipeline, source, store, spool, updates = _make_pipeline(
        tmp_path,
        range(1, 9),
        rotation=RotationConfig(pulse_limit=1, trigger_idle_seconds=100.0),
        config=PipelineConfig(capture_queue_capacity=16, persistence_queue_capacity=16),
    )
    write_started = threading.Event()
    release_write = threading.Event()
    real_write = store.write

    def blocking_write(*args, **kwargs):
        if not write_started.is_set():
            write_started.set()
            assert release_write.wait(1.0)
        return real_write(*args, **kwargs)

    monkeypatch.setattr(store, "write", blocking_write)
    pipeline.start_capture(uuid4())
    shutdown_errors = []

    def shut_down() -> None:
        try:
            pipeline.shutdown()
        except Exception as exc:
            shutdown_errors.append(exc)

    shutdown_thread = threading.Thread(target=shut_down)
    try:
        assert write_started.wait(1.0)
        _wait_until(lambda: source.captured_count == 8)
        shutdown_thread.start()
        shutdown_thread.join(timeout=0.05)
        assert shutdown_thread.is_alive()

        release_write.set()
        shutdown_thread.join(timeout=1.0)

        assert not shutdown_thread.is_alive()
        assert shutdown_errors == []
        metrics = pipeline.engine.metrics.snapshot()
        assert metrics["counters"]["accepted"] == 8
        assert metrics["counters"]["persisted"] == 8
        assert metrics["terminal_error"] == "Acquisition server is shutting down."
        assert spool.load().final_sequence == 7
        assert updates[-1].stop_reason == "Acquisition server is shutting down."
    finally:
        release_write.set()
        shutdown_thread.join(timeout=1.0)
        pipeline.shutdown()


def test_flush_drain_timeout_aborts_but_finishes_the_in_flight_write(
    tmp_path,
    monkeypatch,
) -> None:
    pipeline, _source, store, spool, updates = _make_pipeline(
        tmp_path,
        (1,),
        rotation=RotationConfig(pulse_limit=1, trigger_idle_seconds=100.0),
        config=PipelineConfig(drain_timeout_seconds=0.05),
    )
    write_started = threading.Event()
    release_write = threading.Event()
    real_write = store.write

    def blocking_write(*args, **kwargs):
        write_started.set()
        assert release_write.wait(1.0)
        return real_write(*args, **kwargs)

    monkeypatch.setattr(store, "write", blocking_write)
    pipeline.start_capture(uuid4())
    try:
        assert write_started.wait(1.0)

        with pytest.raises(TimeoutError, match="flush exceeded its drain deadline"):
            pipeline.flush()

        release_write.set()
        _wait_until(lambda: not pipeline.active)

        metrics = pipeline.engine.metrics.snapshot()
        assert metrics["counters"]["accepted"] == 1
        assert metrics["counters"]["persisted"] == 1
        assert metrics["terminal_error"] == "Capture pipeline flush exceeded its drain deadline."
        assert spool.load().final_sequence == 0
        assert updates[-1].stop_reason == metrics["terminal_error"]
    finally:
        release_write.set()
        pipeline.shutdown()
