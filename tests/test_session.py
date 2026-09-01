from collections import deque
import os
import threading
import time
from uuid import uuid4

import numpy as np
import pytest

from euv_acquisition.models import (
    CaptureConfig,
    CapturedPulse,
    NativePulseAnalysis,
    PulseQuality,
    SnapshotCloseReason,
)
from euv_acquisition.session import (
    CaptureEngine,
    CapturePurpose,
    CaptureSessionManifest,
    CaptureSessionState,
    RotationConfig,
    SpoolRepository,
)
from euv_acquisition.snapshot import SnapshotStore, read_snapshot


class QueuePulseSource:
    def __init__(self, samples, timestamps):
        self.capture_config = CaptureConfig(
            sample_rate_hz=1_000_000.0,
            window_seconds=4e-6,
            pretrigger_seconds=1e-6,
        )
        self._samples = samples
        self._timestamps = deque(timestamps)
        self._open = False

    def open(self):
        self._open = True

    def capture(self):
        if not self._open:
            raise RuntimeError("closed")
        if not self._timestamps:
            return None
        timestamp = self._timestamps.popleft()
        return CapturedPulse(self._samples.copy(), timestamp, timestamp)

    def close(self):
        self._open = False


def _engine(tmp_path, samples=None, timestamps=(1, 2, 3), rotation=None):
    samples = np.asarray(samples if samples is not None else [0.0, 0.2, 0.2, 0.0], dtype=np.float32)
    source = QueuePulseSource(samples, timestamps)
    store = SnapshotStore(tmp_path)
    spool = SpoolRepository(tmp_path, server_boot_id=uuid4())
    engine = CaptureEngine(
        source,
        store,
        spool,
        source_kind="simulated",
        source_id="test",
        rotation=rotation or RotationConfig(pulse_limit=2),
        unix_time_ns=lambda: 10,
        monotonic_time_ns=lambda: 10,
    )
    return engine, store, spool


def test_capture_engine_assigns_sequences_and_rotates_at_pulse_limit(tmp_path) -> None:
    engine, _store, spool = _engine(tmp_path)
    session_id = engine.start()

    first = engine.capture_once()
    second = engine.capture_once()

    assert first.report.sequence == 0
    assert second.report.sequence == 1
    assert second.report.session_id == session_id
    assert len(second.closed_snapshots) == 1
    assert second.closed_snapshots[0].close_reason is SnapshotCloseReason.PULSE_LIMIT
    assert spool.load().snapshots[0].manifest == second.closed_snapshots[0]


def test_capture_engine_preserves_source_provided_native_analysis(tmp_path) -> None:
    engine, store, _spool = _engine(tmp_path, timestamps=())
    engine.start()
    supplied = NativePulseAnalysis(
        baseline_volts=0.125,
        integral_volt_seconds=1.234567890123456e-9,
        minimum_volts=0.0,
        maximum_volts=0.2,
        peak_absolute_volts=0.2,
        quality=PulseQuality.OK,
        algorithm_version="siglent-legacy-integral-v1",
    )
    pulse = CapturedPulse(
        np.asarray([0.0, 0.2, 0.2, 0.0], dtype=np.float64),
        1,
        1,
        native_analysis=supplied,
    )

    processed = engine.process_accepted(engine.accept_pulse(pulse))
    manifest = engine.flush().closed_snapshots[0]
    contents = read_snapshot(store.path_for(manifest))

    assert processed.report.analysis == supplied
    assert contents.native_analysis_version == supplied.algorithm_version
    assert contents.integral_volt_seconds[0] == supplied.integral_volt_seconds


def test_capture_engine_rotates_nonempty_snapshot_on_idle_tick(tmp_path) -> None:
    rotation = RotationConfig(pulse_limit=250, wall_time_seconds=5.0, trigger_idle_seconds=0.5)
    engine, _store, _spool = _engine(tmp_path, timestamps=(1_000_000_000,), rotation=rotation)
    engine.start()
    engine.capture_once()

    update = engine.tick(1_500_000_000)

    assert len(update.closed_snapshots) == 1
    assert update.closed_snapshots[0].close_reason is SnapshotCloseReason.TRIGGER_IDLE
    assert engine.tick(2_000_000_000).closed_snapshots == ()


def test_three_clipped_pulses_in_rolling_window_stop_capture(tmp_path) -> None:
    rotation = RotationConfig(pulse_limit=250, clipped_pulse_limit=3, clipped_pulse_window=100)
    engine, _store, spool = _engine(tmp_path, samples=[0.0, 1.0, 1.0, 0.0], rotation=rotation)
    engine.start()

    engine.capture_once()
    engine.capture_once()
    final = engine.capture_once()

    assert final.stop_reason.startswith("Clipping limit reached")
    assert final.closed_snapshots[0].close_reason is SnapshotCloseReason.ACQUISITION_ERROR
    assert engine.active is False
    assert spool.load().state is CaptureSessionState.STOPPED
    assert spool.load().final_sequence == 2


def test_spool_requires_acknowledged_terminal_session_before_release(tmp_path) -> None:
    engine, store, spool = _engine(tmp_path, timestamps=(1,))
    engine.start()
    engine.capture_once()
    stopped = engine.stop()
    snapshot_id = stopped.closed_snapshots[0].snapshot_id

    with pytest.raises(RuntimeError, match="unacknowledged"):
        spool.release(store)
    with pytest.raises(RuntimeError, match="unreleased"):
        engine.start()

    spool.acknowledge(snapshot_id)
    spool.release(store)

    assert spool.load() is None
    assert list(tmp_path.glob("snap_*.h5")) == []


def test_schema_one_session_defaults_to_experiment_purpose(tmp_path) -> None:
    spool = SpoolRepository(tmp_path, server_boot_id=uuid4())
    legacy = spool.begin(uuid4(), "simulated", "legacy", 1).to_dict()
    legacy["schema_version"] = 1
    legacy.pop("purpose")

    decoded = CaptureSessionManifest.from_dict(legacy)

    assert decoded.purpose is CapturePurpose.EXPERIMENT


def test_spool_purges_only_acknowledged_snapshot_data(tmp_path) -> None:
    engine, store, spool = _engine(tmp_path, timestamps=(1,))
    engine.start(purpose=CapturePurpose.DIAGNOSTIC)
    engine.capture_once()
    snapshot = engine.flush().closed_snapshots[0]

    with pytest.raises(RuntimeError, match="unacknowledged"):
        spool.purge_snapshot(store, snapshot.snapshot_id)
    spool.acknowledge(snapshot.snapshot_id)
    retained = spool.purge_snapshot(store, snapshot.snapshot_id)

    assert retained.snapshots[0].acknowledged is True
    assert not store.path_for(snapshot).exists()
    assert spool.load() is not None


def test_spool_discards_only_terminal_diagnostic_sessions(tmp_path) -> None:
    engine, store, spool = _engine(tmp_path / "diagnostic", timestamps=(1,))
    session_id = engine.start(purpose=CapturePurpose.DIAGNOSTIC)
    with pytest.raises(RuntimeError, match="active diagnostic"):
        spool.discard_diagnostic(store, session_id)
    engine.capture_once()
    snapshot = engine.stop().closed_snapshots[0]

    spool.discard_diagnostic(store, session_id)

    assert spool.load() is None
    assert not store.path_for(snapshot).exists()

    experiment, experiment_store, experiment_spool = _engine(tmp_path / "experiment", timestamps=())
    experiment_id = experiment.start()
    with pytest.raises(RuntimeError, match="experiment"):
        experiment_spool.discard_diagnostic(experiment_store, experiment_id)
    experiment.stop()
    with pytest.raises(RuntimeError, match="experiment"):
        experiment_spool.discard_diagnostic(experiment_store, experiment_id)


def test_restart_marks_active_spool_session_orphaned(tmp_path) -> None:
    spool = SpoolRepository(tmp_path, server_boot_id=uuid4())
    spool.begin(uuid4(), "simulated", "test", 1)

    recovered = SpoolRepository(tmp_path, server_boot_id=uuid4()).mark_active_session_orphaned()

    assert recovered.state is CaptureSessionState.ORPHANED
    assert recovered.stop_reason == "Digitizer service restarted during capture."


def test_stop_waits_for_an_in_progress_capture_before_closing_the_source(tmp_path) -> None:
    class BlockingPulseSource(QueuePulseSource):
        def __init__(self):
            super().__init__(np.array([0.0, 0.2, 0.2, 0.0], dtype=np.float32), (1,))
            self.capture_entered = threading.Event()
            self.capture_release = threading.Event()

        def capture(self):
            self.capture_entered.set()
            assert self.capture_release.wait(1.0)
            return super().capture()

    source = BlockingPulseSource()
    store = SnapshotStore(tmp_path)
    spool = SpoolRepository(tmp_path, server_boot_id=uuid4())
    engine = CaptureEngine(source, store, spool, source_kind="simulated", source_id="blocking")
    engine.start()
    captured = []
    stopped = []
    capture_thread = threading.Thread(target=lambda: captured.append(engine.capture_once()))
    stop_thread = threading.Thread(target=lambda: stopped.append(engine.stop("test stop")))
    capture_thread.start()
    assert source.capture_entered.wait(1.0)
    stop_thread.start()
    time.sleep(0.05)
    assert not stopped

    source.capture_release.set()
    capture_thread.join(timeout=1.0)
    stop_thread.join(timeout=1.0)

    assert len(captured) == 1
    assert len(stopped) == 1
    assert spool.load().state is CaptureSessionState.STOPPED


def test_spool_retries_a_transient_windows_manifest_replace_failure(tmp_path, monkeypatch) -> None:
    spool = SpoolRepository(tmp_path, server_boot_id=uuid4())
    real_replace = os.replace
    attempts = []

    def retrying_replace(source, destination):
        attempts.append((source, destination))
        if len(attempts) < 3:
            raise PermissionError(5, "Access is denied", str(destination))
        return real_replace(source, destination)

    monkeypatch.setattr("euv_acquisition.session.os.replace", retrying_replace)

    manifest = spool.begin(uuid4(), "simulated", "test", 1)

    assert manifest.state is CaptureSessionState.ACTIVE
    assert len(attempts) == 3