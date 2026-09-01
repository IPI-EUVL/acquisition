from dataclasses import replace
from uuid import uuid4

import h5py
import numpy as np
import pytest

import euv_acquisition.snapshot as snapshot_module
from euv_acquisition.analysis import analyze_pulse
from euv_acquisition.models import CaptureConfig, CapturedPulse, PulseRecord, SnapshotCloseReason, SourceBatchEnvelope
from euv_acquisition.snapshot import SnapshotManifest, SnapshotStore, read_snapshot


def _records(count=3):
    config = CaptureConfig(sample_rate_hz=1_000_000.0, window_seconds=4e-6, pretrigger_seconds=1e-6)
    session_id = uuid4()
    records = []
    for sequence in range(10, 10 + count):
        samples = np.asarray([0.1, 0.2 + sequence * 0.01, 0.3, 0.1], dtype=np.float32)
        pulse = CapturedPulse(samples, 1_000 + sequence, 2_000 + sequence)
        records.append(PulseRecord(session_id, sequence, pulse, analyze_pulse(samples, config)))
    return config, records


def test_snapshot_store_writes_uncompressed_atomic_hdf5_and_round_trips(tmp_path) -> None:
    config, records = _records()
    store = SnapshotStore(tmp_path)

    manifest = store.write(
        records,
        config,
        SnapshotCloseReason.PULSE_LIMIT,
        source_kind="simulated",
        source_id="seed-1",
    )

    path = store.path_for(manifest)
    contents = read_snapshot(path)
    store.verify(manifest)
    assert manifest.first_sequence == 10
    assert manifest.final_sequence == 12
    assert manifest.pulse_count == 3
    assert contents.samples_v.shape == (3, 4)
    assert contents.samples_v.dtype == np.float32
    assert contents.sequence.tolist() == [10, 11, 12]
    assert contents.source_kind == "simulated"
    assert contents.native_analysis_version == records[0].analysis.algorithm_version
    assert contents.source_batch is None
    assert list(tmp_path.glob("*.tmp")) == []
    with h5py.File(path, "r") as snapshot:
        assert all(dataset.compression is None for dataset in snapshot.values())


def test_snapshot_store_round_trips_source_batch_envelope(tmp_path) -> None:
    config, records = _records()
    store = SnapshotStore(tmp_path)
    envelope = SourceBatchEnvelope(
        batch_id=uuid4(),
        batch_kind="siglent_sequence",
        capture_started_unix_ns=1_000,
        capture_completed_unix_ns=2_000,
    )

    manifest = store.write(
        records,
        config,
        SnapshotCloseReason.SOURCE_BATCH,
        source_kind="siglent",
        source_id="scope-1",
        source_batch=envelope,
    )

    contents = read_snapshot(store.path_for(manifest))
    store.verify(manifest)
    assert contents.close_reason is SnapshotCloseReason.SOURCE_BATCH
    assert contents.source_batch == envelope


def test_snapshot_manifest_is_strict_and_round_trips(tmp_path) -> None:
    config, records = _records(1)
    store = SnapshotStore(tmp_path)
    manifest = store.write(records, config, SnapshotCloseReason.CAPTURE_STOP, source_kind="simulated", source_id="one")

    assert SnapshotManifest.from_dict(manifest.to_dict()) == manifest
    invalid = manifest.to_dict()
    invalid["extra"] = True
    with pytest.raises(ValueError, match="unknown or missing"):
        SnapshotManifest.from_dict(invalid)


def test_snapshot_store_rejects_empty_and_noncontiguous_records(tmp_path) -> None:
    config, records = _records()
    store = SnapshotStore(tmp_path)

    with pytest.raises(ValueError, match="empty"):
        store.write([], config, SnapshotCloseReason.EXPLICIT_FLUSH, source_kind="simulated", source_id="test")
    with pytest.raises(ValueError, match="contiguous"):
        store.write(
            [records[0], replace(records[1], sequence=99)],
            config,
            SnapshotCloseReason.EXPLICIT_FLUSH,
            source_kind="simulated",
            source_id="test",
        )


def test_snapshot_store_semantically_validates_new_artifact(tmp_path, monkeypatch) -> None:
    config, records = _records(1)
    store = SnapshotStore(tmp_path)

    def reject_snapshot(_path):
        raise ValueError("semantic validation fixture")

    monkeypatch.setattr(snapshot_module, "read_snapshot", reject_snapshot)

    with pytest.raises(ValueError, match="semantic validation fixture"):
        store.write(
            records,
            config,
            SnapshotCloseReason.PULSE_LIMIT,
            source_kind="simulated",
            source_id="semantic-check",
        )


def test_snapshot_verification_detects_tampering(tmp_path) -> None:
    config, records = _records(1)
    store = SnapshotStore(tmp_path)
    manifest = store.write(records, config, SnapshotCloseReason.WALL_TIME, source_kind="simulated", source_id="test")
    path = store.path_for(manifest)

    with path.open("ab") as artifact:
        artifact.write(b"tampered")

    with pytest.raises(ValueError, match="byte count"):
        store.verify(manifest)