from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum, IntFlag
from uuid import UUID

import numpy as np


@dataclass(frozen=True)
class CaptureConfig:
    sample_rate_hz: float = 125_000_000.0
    window_seconds: float = 10e-6
    pretrigger_seconds: float = 1e-6
    input_full_scale_volts: float = 1.0
    clipping_fraction: float = 0.99

    def __post_init__(self) -> None:
        for name in (
            "sample_rate_hz",
            "window_seconds",
            "pretrigger_seconds",
            "input_full_scale_volts",
            "clipping_fraction",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
                raise ValueError(f"{name} must be a finite number.")

        if self.sample_rate_hz <= 0:
            raise ValueError("sample_rate_hz must be positive.")
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be positive.")
        if not 0 < self.pretrigger_seconds < self.window_seconds:
            raise ValueError("pretrigger_seconds must be inside the capture window.")
        if self.input_full_scale_volts <= 0:
            raise ValueError("input_full_scale_volts must be positive.")
        if not 0 < self.clipping_fraction <= 1:
            raise ValueError("clipping_fraction must be in (0, 1].")
        if self.pretrigger_samples < 1:
            raise ValueError("Capture configuration must include at least one pre-trigger sample.")
        if self.window_samples < 2:
            raise ValueError("Capture configuration must include at least two samples.")

    @property
    def sample_interval_seconds(self) -> float:
        return 1.0 / self.sample_rate_hz

    @property
    def window_samples(self) -> int:
        return int(round(self.window_seconds * self.sample_rate_hz))

    @property
    def pretrigger_samples(self) -> int:
        return int(round(self.pretrigger_seconds * self.sample_rate_hz))


@dataclass(frozen=True)
class CapturedPulse:
    samples_v: np.ndarray
    captured_at_unix_ns: int
    captured_at_monotonic_ns: int
    native_analysis: NativePulseAnalysis | None = None

    def __post_init__(self) -> None:
        samples = np.asarray(self.samples_v)
        if samples.ndim != 1 or len(samples) < 2:
            raise ValueError("samples_v must be a one-dimensional array with at least two samples.")
        if not np.issubdtype(samples.dtype, np.floating):
            raise ValueError("samples_v must contain floating-point calibrated volts.")
        if not np.isfinite(samples).all():
            raise ValueError("samples_v contains non-finite values.")
        if isinstance(self.captured_at_unix_ns, bool) or not isinstance(self.captured_at_unix_ns, int):
            raise ValueError("captured_at_unix_ns must be an integer.")
        if isinstance(self.captured_at_monotonic_ns, bool) or not isinstance(self.captured_at_monotonic_ns, int):
            raise ValueError("captured_at_monotonic_ns must be an integer.")
        if self.captured_at_unix_ns < 0 or self.captured_at_monotonic_ns < 0:
            raise ValueError("Capture timestamps must be non-negative.")
        if self.native_analysis is not None and not isinstance(self.native_analysis, NativePulseAnalysis):
            raise ValueError("native_analysis must be NativePulseAnalysis when provided.")
        object.__setattr__(self, "samples_v", np.ascontiguousarray(samples, dtype=np.float32))


class PulseQuality(IntFlag):
    OK = 0
    CLIPPED = 1 << 0


@dataclass(frozen=True)
class NativePulseAnalysis:
    baseline_volts: float
    integral_volt_seconds: float
    minimum_volts: float
    maximum_volts: float
    peak_absolute_volts: float
    quality: PulseQuality
    algorithm_version: str

    def __post_init__(self) -> None:
        if not isinstance(self.quality, PulseQuality):
            raise ValueError("Native pulse analysis quality must be PulseQuality.")
        if not isinstance(self.algorithm_version, str) or not self.algorithm_version:
            raise ValueError("Native pulse analysis algorithm version cannot be empty.")
        for name in (
            "baseline_volts",
            "integral_volt_seconds",
            "minimum_volts",
            "maximum_volts",
            "peak_absolute_volts",
        ):
            if not math.isfinite(float(getattr(self, name))):
                raise ValueError(f"Native pulse analysis {name} must be finite.")

    @classmethod
    def from_dict(cls, value: object) -> "NativePulseAnalysis":
        expected = {
            "baseline_volts",
            "integral_volt_seconds",
            "minimum_volts",
            "maximum_volts",
            "peak_absolute_volts",
            "quality",
            "algorithm_version",
        }
        if not isinstance(value, dict) or set(value) != expected:
            raise ValueError("Native pulse analysis contains unknown or missing fields.")
        try:
            quality = PulseQuality(int(value["quality"]))
        except (TypeError, ValueError) as exc:
            raise ValueError("Native pulse analysis quality is invalid.") from exc
        analysis = cls(
            baseline_volts=float(value["baseline_volts"]),
            integral_volt_seconds=float(value["integral_volt_seconds"]),
            minimum_volts=float(value["minimum_volts"]),
            maximum_volts=float(value["maximum_volts"]),
            peak_absolute_volts=float(value["peak_absolute_volts"]),
            quality=quality,
            algorithm_version=str(value["algorithm_version"]),
        )
        return analysis


@dataclass(frozen=True)
class PulseRecord:
    session_id: UUID
    sequence: int
    pulse: CapturedPulse
    analysis: NativePulseAnalysis

    def __post_init__(self) -> None:
        if not isinstance(self.session_id, UUID):
            raise ValueError("session_id must be a UUID.")
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int) or self.sequence < 0:
            raise ValueError("sequence must be a non-negative integer.")
        for name in (
            "baseline_volts",
            "integral_volt_seconds",
            "minimum_volts",
            "maximum_volts",
            "peak_absolute_volts",
        ):
            if not math.isfinite(float(getattr(self.analysis, name))):
                raise ValueError(f"analysis {name} must be finite.")


class SnapshotCloseReason(str, Enum):
    PULSE_LIMIT = "pulse_limit"
    WALL_TIME = "wall_time"
    TRIGGER_IDLE = "trigger_idle"
    SOURCE_BATCH = "source_batch"
    EXPLICIT_FLUSH = "explicit_flush"
    CAPTURE_STOP = "capture_stop"
    WATCHDOG = "watchdog"
    DISK_GUARD = "disk_guard"
    ACQUISITION_ERROR = "acquisition_error"


@dataclass(frozen=True)
class SourceBatchEnvelope:
    batch_id: UUID
    batch_kind: str
    capture_started_unix_ns: int
    capture_completed_unix_ns: int

    def __post_init__(self) -> None:
        if not isinstance(self.batch_id, UUID):
            raise ValueError("Source batch ID must be a UUID.")
        if not isinstance(self.batch_kind, str) or not self.batch_kind.strip():
            raise ValueError("Source batch kind cannot be empty.")
        for name in ("capture_started_unix_ns", "capture_completed_unix_ns"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer.")
        if self.capture_completed_unix_ns < self.capture_started_unix_ns:
            raise ValueError("Source batch completion cannot precede its start.")


@dataclass(frozen=True)
class SourceCaptureBatch:
    pulses: tuple[CapturedPulse, ...]
    envelope: SourceBatchEnvelope

    def __post_init__(self) -> None:
        pulses = tuple(self.pulses)
        if not pulses:
            raise ValueError("Source capture batch must contain at least one pulse.")
        if not all(isinstance(pulse, CapturedPulse) for pulse in pulses):
            raise ValueError("Source capture batch contains an invalid pulse.")
        for previous, current in zip(pulses, pulses[1:]):
            if current.captured_at_unix_ns <= previous.captured_at_unix_ns:
                raise ValueError("Source capture batch Unix timestamps must increase.")
            if current.captured_at_monotonic_ns <= previous.captured_at_monotonic_ns:
                raise ValueError("Source capture batch monotonic timestamps must increase.")
        if not isinstance(self.envelope, SourceBatchEnvelope):
            raise ValueError("Source capture batch envelope is invalid.")
        object.__setattr__(self, "pulses", pulses)


@dataclass(frozen=True)
class PulseReport:
    session_id: UUID
    sequence: int
    captured_at_unix_ns: int
    captured_at_monotonic_ns: int
    analysis: NativePulseAnalysis

    def __post_init__(self) -> None:
        if not isinstance(self.session_id, UUID):
            raise ValueError("session_id must be a UUID.")
        for name in ("sequence", "captured_at_unix_ns", "captured_at_monotonic_ns"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer.")
        if not isinstance(self.analysis, NativePulseAnalysis):
            raise ValueError("analysis must be NativePulseAnalysis.")

    @classmethod
    def from_record(cls, record: PulseRecord) -> "PulseReport":
        return cls(
            session_id=record.session_id,
            sequence=record.sequence,
            captured_at_unix_ns=record.pulse.captured_at_unix_ns,
            captured_at_monotonic_ns=record.pulse.captured_at_monotonic_ns,
            analysis=record.analysis,
        )

    def to_dict(self) -> dict:
        return {
            "schema_version": 1,
            "session_id": str(self.session_id),
            "sequence": self.sequence,
            "captured_at_unix_ns": self.captured_at_unix_ns,
            "captured_at_monotonic_ns": self.captured_at_monotonic_ns,
            "analysis": {
                "baseline_volts": self.analysis.baseline_volts,
                "integral_volt_seconds": self.analysis.integral_volt_seconds,
                "minimum_volts": self.analysis.minimum_volts,
                "maximum_volts": self.analysis.maximum_volts,
                "peak_absolute_volts": self.analysis.peak_absolute_volts,
                "quality": int(self.analysis.quality),
                "algorithm_version": self.analysis.algorithm_version,
            },
        }

    @classmethod
    def from_dict(cls, value: object) -> "PulseReport":
        expected = {
            "schema_version",
            "session_id",
            "sequence",
            "captured_at_unix_ns",
            "captured_at_monotonic_ns",
            "analysis",
        }
        if not isinstance(value, dict) or set(value) != expected:
            raise ValueError("Pulse report contains unknown or missing fields.")
        if value["schema_version"] != 1:
            raise ValueError("Unsupported pulse report schema version.")
        return cls(
            session_id=UUID(str(value["session_id"])),
            sequence=int(value["sequence"]),
            captured_at_unix_ns=int(value["captured_at_unix_ns"]),
            captured_at_monotonic_ns=int(value["captured_at_monotonic_ns"]),
            analysis=NativePulseAnalysis.from_dict(value["analysis"]),
        )
