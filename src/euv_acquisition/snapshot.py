from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence
from uuid import UUID, uuid4

import h5py
import numpy as np

from euv_acquisition.models import CaptureConfig, PulseRecord, SnapshotCloseReason


SNAPSHOT_SCHEMA_VERSION = 1
SNAPSHOT_RESOURCE_TYPE = "euv_snapshot"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class SnapshotManifest:
    snapshot_id: UUID
    session_id: UUID
    filename: str
    byte_count: int
    sha256: str
    first_sequence: int
    final_sequence: int
    pulse_count: int
    close_reason: SnapshotCloseReason
    first_capture_unix_ns: int
    final_capture_unix_ns: int

    def to_dict(self) -> dict:
        return {
            "schema_version": SNAPSHOT_SCHEMA_VERSION,
            "snapshot_id": str(self.snapshot_id),
            "session_id": str(self.session_id),
            "filename": self.filename,
            "byte_count": self.byte_count,
            "sha256": self.sha256,
            "first_sequence": self.first_sequence,
            "final_sequence": self.final_sequence,
            "pulse_count": self.pulse_count,
            "close_reason": self.close_reason.value,
            "first_capture_unix_ns": self.first_capture_unix_ns,
            "final_capture_unix_ns": self.final_capture_unix_ns,
        }

    @classmethod
    def from_dict(cls, value: object) -> "SnapshotManifest":
        expected = {
            "schema_version",
            "snapshot_id",
            "session_id",
            "filename",
            "byte_count",
            "sha256",
            "first_sequence",
            "final_sequence",
            "pulse_count",
            "close_reason",
            "first_capture_unix_ns",
            "final_capture_unix_ns",
        }
        if not isinstance(value, dict) or set(value) != expected:
            raise ValueError("Snapshot manifest contains unknown or missing fields.")
        if value["schema_version"] != SNAPSHOT_SCHEMA_VERSION:
            raise ValueError("Unsupported snapshot manifest schema version.")
        manifest = cls(
            snapshot_id=UUID(str(value["snapshot_id"])),
            session_id=UUID(str(value["session_id"])),
            filename=str(value["filename"]),
            byte_count=int(value["byte_count"]),
            sha256=str(value["sha256"]),
            first_sequence=int(value["first_sequence"]),
            final_sequence=int(value["final_sequence"]),
            pulse_count=int(value["pulse_count"]),
            close_reason=SnapshotCloseReason(value["close_reason"]),
            first_capture_unix_ns=int(value["first_capture_unix_ns"]),
            final_capture_unix_ns=int(value["final_capture_unix_ns"]),
        )
        if manifest.filename != f"snap_{manifest.snapshot_id}.h5":
            raise ValueError("Snapshot manifest filename does not match its snapshot ID.")
        if manifest.byte_count <= 0:
            raise ValueError("Snapshot byte count must be positive.")
        if len(manifest.sha256) != 64 or any(character not in "0123456789abcdef" for character in manifest.sha256):
            raise ValueError("Snapshot SHA-256 must be lowercase hexadecimal.")
        if manifest.pulse_count <= 0:
            raise ValueError("Snapshot pulse count must be positive.")
        if manifest.final_sequence - manifest.first_sequence + 1 != manifest.pulse_count:
            raise ValueError("Snapshot sequence range does not match its pulse count.")
        if manifest.final_capture_unix_ns < manifest.first_capture_unix_ns:
            raise ValueError("Snapshot capture timestamps are reversed.")
        return manifest


@dataclass(frozen=True)
class SnapshotContents:
    snapshot_id: UUID
    session_id: UUID
    capture_config: CaptureConfig
    source_kind: str
    source_id: str
    close_reason: SnapshotCloseReason
    samples_v: np.ndarray
    sequence: np.ndarray
    captured_at_unix_ns: np.ndarray
    captured_at_monotonic_ns: np.ndarray
    baseline_volts: np.ndarray
    integral_volt_seconds: np.ndarray
    minimum_volts: np.ndarray
    maximum_volts: np.ndarray
    peak_absolute_volts: np.ndarray
    quality: np.ndarray


class SnapshotStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _validate_records(records: Sequence[PulseRecord], capture_config: CaptureConfig) -> tuple[PulseRecord, ...]:
        normalized = tuple(records)
        if not normalized:
            raise ValueError("Cannot write an empty snapshot.")
        session_id = normalized[0].session_id
        first_sequence = normalized[0].sequence
        for offset, record in enumerate(normalized):
            if record.session_id != session_id:
                raise ValueError("Snapshot pulses must belong to one capture session.")
            if record.sequence != first_sequence + offset:
                raise ValueError("Snapshot pulse sequences must be contiguous and ordered.")
            if len(record.pulse.samples_v) != capture_config.window_samples:
                raise ValueError("Snapshot pulse sample count does not match capture configuration.")
        return normalized

    def write(
        self,
        records: Sequence[PulseRecord],
        capture_config: CaptureConfig,
        close_reason: SnapshotCloseReason,
        *,
        source_kind: str,
        source_id: str,
    ) -> SnapshotManifest:
        normalized = self._validate_records(records, capture_config)
        if not source_kind.strip() or not source_id.strip():
            raise ValueError("Snapshot source kind and source ID cannot be empty.")

        snapshot_id = uuid4()
        filename = f"snap_{snapshot_id}.h5"
        destination = self.root / filename
        temporary = self.root / f".{filename}.{uuid4().hex}.tmp"
        first = normalized[0]
        final = normalized[-1]

        try:
            with h5py.File(temporary, "x") as snapshot:
                snapshot.attrs["schema_version"] = SNAPSHOT_SCHEMA_VERSION
                snapshot.attrs["snapshot_id"] = str(snapshot_id)
                snapshot.attrs["session_id"] = str(first.session_id)
                snapshot.attrs["source_kind"] = source_kind
                snapshot.attrs["source_id"] = source_id
                snapshot.attrs["close_reason"] = close_reason.value
                snapshot.attrs["sample_rate_hz"] = capture_config.sample_rate_hz
                snapshot.attrs["window_seconds"] = capture_config.window_seconds
                snapshot.attrs["pretrigger_seconds"] = capture_config.pretrigger_seconds
                snapshot.attrs["input_full_scale_volts"] = capture_config.input_full_scale_volts
                snapshot.attrs["clipping_fraction"] = capture_config.clipping_fraction
                snapshot.attrs["native_analysis_version"] = first.analysis.algorithm_version

                snapshot.create_dataset("samples_v", data=np.stack([item.pulse.samples_v for item in normalized]))
                snapshot.create_dataset("sequence", data=np.asarray([item.sequence for item in normalized], dtype=np.uint64))
                snapshot.create_dataset(
                    "captured_at_unix_ns",
                    data=np.asarray([item.pulse.captured_at_unix_ns for item in normalized], dtype=np.int64),
                )
                snapshot.create_dataset(
                    "captured_at_monotonic_ns",
                    data=np.asarray([item.pulse.captured_at_monotonic_ns for item in normalized], dtype=np.int64),
                )
                for dataset_name in (
                    "baseline_volts",
                    "integral_volt_seconds",
                    "minimum_volts",
                    "maximum_volts",
                    "peak_absolute_volts",
                ):
                    snapshot.create_dataset(
                        dataset_name,
                        data=np.asarray([getattr(item.analysis, dataset_name) for item in normalized], dtype=np.float64),
                    )
                snapshot.create_dataset(
                    "quality",
                    data=np.asarray([int(item.analysis.quality) for item in normalized], dtype=np.uint32),
                )
                snapshot.flush()

            with temporary.open("r+b") as published:
                os.fsync(published.fileno())
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)

        manifest = SnapshotManifest(
            snapshot_id=snapshot_id,
            session_id=first.session_id,
            filename=filename,
            byte_count=destination.stat().st_size,
            sha256=_sha256(destination),
            first_sequence=first.sequence,
            final_sequence=final.sequence,
            pulse_count=len(normalized),
            close_reason=close_reason,
            first_capture_unix_ns=first.pulse.captured_at_unix_ns,
            final_capture_unix_ns=final.pulse.captured_at_unix_ns,
        )
        self.verify(manifest)
        return manifest

    def path_for(self, manifest: SnapshotManifest) -> Path:
        return self.root / manifest.filename

    def verify(self, manifest: SnapshotManifest) -> None:
        path = self.path_for(manifest)
        if not path.is_file():
            raise FileNotFoundError(path)
        if path.stat().st_size != manifest.byte_count:
            raise ValueError("Snapshot byte count does not match its manifest.")
        if _sha256(path) != manifest.sha256:
            raise ValueError("Snapshot SHA-256 does not match its manifest.")
        contents = read_snapshot(path)
        if contents.snapshot_id != manifest.snapshot_id or contents.session_id != manifest.session_id:
            raise ValueError("Snapshot identity does not match its manifest.")
        if contents.close_reason is not manifest.close_reason:
            raise ValueError("Snapshot close reason does not match its manifest.")
        if len(contents.sequence) != manifest.pulse_count:
            raise ValueError("Snapshot pulse count does not match its manifest.")
        if int(contents.sequence[0]) != manifest.first_sequence or int(contents.sequence[-1]) != manifest.final_sequence:
            raise ValueError("Snapshot sequence range does not match its manifest.")


def read_snapshot(path: str | Path) -> SnapshotContents:
    source_path = Path(path)
    with h5py.File(source_path, "r") as snapshot:
        if int(snapshot.attrs.get("schema_version", -1)) != SNAPSHOT_SCHEMA_VERSION:
            raise ValueError("Unsupported HDF5 snapshot schema version.")
        required = {
            "samples_v",
            "sequence",
            "captured_at_unix_ns",
            "captured_at_monotonic_ns",
            "baseline_volts",
            "integral_volt_seconds",
            "minimum_volts",
            "maximum_volts",
            "peak_absolute_volts",
            "quality",
        }
        if set(snapshot.keys()) != required:
            raise ValueError("HDF5 snapshot contains unknown or missing datasets.")
        samples = snapshot["samples_v"][:]
        if samples.ndim != 2 or samples.dtype != np.dtype("float32"):
            raise ValueError("HDF5 samples_v must be a two-dimensional float32 dataset.")
        pulse_count = samples.shape[0]
        arrays = {name: snapshot[name][:] for name in required - {"samples_v"}}
        if any(array.ndim != 1 or len(array) != pulse_count for array in arrays.values()):
            raise ValueError("HDF5 pulse datasets must be one-dimensional and equal in length.")
        sequence = arrays["sequence"]
        if pulse_count == 0 or not np.array_equal(sequence, np.arange(sequence[0], sequence[0] + pulse_count)):
            raise ValueError("HDF5 snapshot sequences must be non-empty, contiguous, and ordered.")
        if not np.isfinite(samples).all() or any(
            not np.isfinite(arrays[name]).all()
            for name in (
                "baseline_volts",
                "integral_volt_seconds",
                "minimum_volts",
                "maximum_volts",
                "peak_absolute_volts",
            )
        ):
            raise ValueError("HDF5 snapshot contains non-finite measurements.")

        capture_config = CaptureConfig(
            sample_rate_hz=float(snapshot.attrs["sample_rate_hz"]),
            window_seconds=float(snapshot.attrs["window_seconds"]),
            pretrigger_seconds=float(snapshot.attrs["pretrigger_seconds"]),
            input_full_scale_volts=float(snapshot.attrs["input_full_scale_volts"]),
            clipping_fraction=float(snapshot.attrs["clipping_fraction"]),
        )
        if samples.shape[1] != capture_config.window_samples:
            raise ValueError("HDF5 sample shape does not match capture configuration.")

        return SnapshotContents(
            snapshot_id=UUID(str(snapshot.attrs["snapshot_id"])),
            session_id=UUID(str(snapshot.attrs["session_id"])),
            capture_config=capture_config,
            source_kind=str(snapshot.attrs["source_kind"]),
            source_id=str(snapshot.attrs["source_id"]),
            close_reason=SnapshotCloseReason(str(snapshot.attrs["close_reason"])),
            samples_v=samples,
            sequence=sequence,
            captured_at_unix_ns=arrays["captured_at_unix_ns"],
            captured_at_monotonic_ns=arrays["captured_at_monotonic_ns"],
            baseline_volts=arrays["baseline_volts"],
            integral_volt_seconds=arrays["integral_volt_seconds"],
            minimum_volts=arrays["minimum_volts"],
            maximum_volts=arrays["maximum_volts"],
            peak_absolute_volts=arrays["peak_absolute_volts"],
            quality=arrays["quality"],
        )