from __future__ import annotations

from typing import Protocol

from euv_acquisition.models import CaptureConfig, CapturedPulse, SourceCaptureBatch


class PulseSource(Protocol):
    @property
    def capture_config(self) -> CaptureConfig: ...

    def open(self) -> None: ...

    def capture(self) -> CapturedPulse | SourceCaptureBatch | None: ...

    def close(self) -> None: ...
