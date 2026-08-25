from __future__ import annotations

import json
import math
from dataclasses import dataclass
from uuid import UUID


ACQUISITION_HEALTH_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class AcquisitionHealth:
    capture_active: bool
    session_id: UUID | None
    last_sequence: int | None
    last_pulse_age_seconds: float | None
    pulse_loss: bool
    recovery_ready: bool
    resume_authorized: bool
    reason: str | None = None

    def __post_init__(self) -> None:
        for name in ("capture_active", "pulse_loss", "recovery_ready", "resume_authorized"):
            if not isinstance(getattr(self, name), bool):
                raise ValueError(f"{name} must be boolean.")
        if self.session_id is not None and not isinstance(self.session_id, UUID):
            raise ValueError("session_id must be a UUID or null.")
        if self.last_sequence is not None and (isinstance(self.last_sequence, bool) or not isinstance(self.last_sequence, int) or self.last_sequence < 0):
            raise ValueError("last_sequence must be a non-negative integer or null.")
        if self.last_pulse_age_seconds is not None:
            value = self.last_pulse_age_seconds
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)) or value < 0:
                raise ValueError("last_pulse_age_seconds must be finite and non-negative or null.")
        if self.reason is not None and (not isinstance(self.reason, str) or not self.reason.strip()):
            raise ValueError("reason must be non-empty text or null.")

    def to_dict(self) -> dict:
        return {
            "schema_version": ACQUISITION_HEALTH_SCHEMA_VERSION,
            "capture_active": self.capture_active,
            "session_id": None if self.session_id is None else str(self.session_id),
            "last_sequence": self.last_sequence,
            "last_pulse_age_seconds": self.last_pulse_age_seconds,
            "pulse_loss": self.pulse_loss,
            "recovery_ready": self.recovery_ready,
            "resume_authorized": self.resume_authorized,
            "reason": self.reason,
        }

    def encode(self) -> bytes:
        return json.dumps(self.to_dict(), allow_nan=False, separators=(",", ":")).encode("utf-8")

    @classmethod
    def decode(cls, payload: bytes) -> "AcquisitionHealth":
        try:
            value = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("Acquisition health must be UTF-8 JSON.") from exc
        expected = {
            "schema_version",
            "capture_active",
            "session_id",
            "last_sequence",
            "last_pulse_age_seconds",
            "pulse_loss",
            "recovery_ready",
            "resume_authorized",
            "reason",
        }
        if not isinstance(value, dict) or set(value) != expected or value["schema_version"] != ACQUISITION_HEALTH_SCHEMA_VERSION:
            raise ValueError("Unsupported acquisition health schema.")
        return cls(
            capture_active=value["capture_active"],
            session_id=None if value["session_id"] is None else UUID(str(value["session_id"])),
            last_sequence=value["last_sequence"],
            last_pulse_age_seconds=value["last_pulse_age_seconds"],
            pulse_loss=value["pulse_loss"],
            recovery_ready=value["recovery_ready"],
            resume_authorized=value["resume_authorized"],
            reason=value["reason"],
        )