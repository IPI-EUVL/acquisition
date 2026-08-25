import uuid

import pytest

from euv_acquisition.health import AcquisitionHealth


def test_acquisition_health_round_trips_with_strict_schema() -> None:
    health = AcquisitionHealth(True, uuid.uuid4(), 12, 0.1, True, False, False, "Pulse reports stopped.")

    assert AcquisitionHealth.decode(health.encode()) == health
    with pytest.raises(ValueError, match="schema"):
        AcquisitionHealth.decode(b'{"schema_version":2}')