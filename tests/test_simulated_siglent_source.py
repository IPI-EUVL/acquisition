from uuid import UUID

import numpy as np

from euv_acquisition.models import CaptureConfig
from euv_acquisition.sources.siglent import (
    SIGLENT_BATCH_KIND,
    SIGLENT_NATIVE_ANALYSIS_VERSION,
)
from euv_acquisition.sources.simulated import SimulatedPulseConfig
from euv_acquisition.sources.simulated_siglent import SimulatedSiglentPulseSource


class _Clock:
    def __init__(self) -> None:
        self.unix_ns = 1_000_000_000
        self.monotonic_ns = 2_000_000_000

    def sleep(self, seconds: float) -> None:
        elapsed_ns = int(round(seconds * 1e9))
        self.unix_ns += elapsed_ns
        self.monotonic_ns += elapsed_ns


def test_simulated_siglent_source_emits_atomic_native_sequence_batch() -> None:
    clock = _Clock()
    config = CaptureConfig(
        sample_rate_hz=1_000_000,
        window_seconds=50e-6,
        pretrigger_seconds=25e-6,
    )
    source = SimulatedSiglentPulseSource(
        config,
        SimulatedPulseConfig(seed=7, trigger_rate_hz=100, center_seconds=10e-6),
        sequence_count=3,
        unix_time_ns=lambda: clock.unix_ns,
        monotonic_time_ns=lambda: clock.monotonic_ns,
        sleep=clock.sleep,
        batch_id_factory=lambda: UUID("11111111-2222-3333-4444-555555555555"),
    )

    source.open()
    batch = source.capture()
    source.close()

    assert batch.envelope.batch_kind == SIGLENT_BATCH_KIND
    assert batch.envelope.capture_started_unix_ns == 1_000_000_000
    assert batch.envelope.capture_completed_unix_ns == 1_030_000_000
    assert len(batch.pulses) == 3
    assert [pulse.captured_at_unix_ns for pulse in batch.pulses] == [
        1_000_000_000,
        1_010_000_000,
        1_020_000_000,
    ]
    assert all(pulse.samples_v.shape == (50,) for pulse in batch.pulses)
    assert all(pulse.samples_v.dtype == np.float32 for pulse in batch.pulses)
    assert all(
        pulse.native_analysis.algorithm_version == SIGLENT_NATIVE_ANALYSIS_VERSION
        for pulse in batch.pulses
    )
    assert source.capture_mode == "siglent-sequence"
    assert source.release_confirmed is True