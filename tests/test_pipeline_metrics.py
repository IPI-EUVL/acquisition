import json
from uuid import uuid4

import pytest

from euv_acquisition.pipeline_metrics import PipelineMetrics


def test_pipeline_metrics_are_bounded_and_json_serializable() -> None:
    monotonic = iter((100, 200))
    metrics = PipelineMetrics(
        sample_limit=3,
        unix_time_ns=lambda: 500,
        monotonic_time_ns=lambda: next(monotonic),
    )
    session_id = uuid4()
    metrics.begin_session(
        session_id,
        requested_mode="auto",
        effective_mode="single-shot",
        fallback_reason="AXI unavailable",
    )
    for duration_ns in (1_000_000, 2_000_000, 3_000_000, 4_000_000):
        metrics.record_duration("analysis", duration_ns)
    metrics.increment("accepted", 4)
    metrics.observe_queue("control", depth=2, capacity=8)
    metrics.observe_queue("control", depth=1, capacity=8)
    metrics.set_capture_worker(pid=123, cpu=1, scheduler="fifo", realtime_priority=20)

    snapshot = metrics.snapshot()

    assert snapshot["session_id"] == str(session_id)
    assert snapshot["state"] == "active"
    assert snapshot["capture_mode"] == {
        "requested": "auto",
        "effective": "single-shot",
        "fallback_reason": "AXI unavailable",
    }
    assert snapshot["capture_worker"] == {
        "pid": 123,
        "cpu": 1,
        "scheduler": "fifo",
        "realtime_priority": 20,
    }
    assert snapshot["counters"] == {"accepted": 4}
    assert snapshot["queues"]["control"] == {"depth": 1, "capacity": 8, "high_water": 2}
    assert snapshot["stages"]["analysis"] == {
        "count": 4,
        "recent_count": 3,
        "mean_ms": 3.0,
        "p95_ms": 4.0,
        "max_ms": 4.0,
    }
    json.dumps(snapshot, allow_nan=False)


def test_pipeline_metrics_reject_invalid_observations_and_retain_terminal_state() -> None:
    metrics = PipelineMetrics()
    metrics.begin_session(uuid4(), effective_mode="legacy-single-shot")

    with pytest.raises(ValueError, match="Unknown"):
        metrics.record_duration("unknown", 1)
    with pytest.raises(ValueError, match="non-negative"):
        metrics.record_duration("analysis", -1)
    with pytest.raises(ValueError, match="must not exceed"):
        metrics.observe_queue("control", depth=2, capacity=1)

    metrics.finish(terminal_error="queue overflow")

    snapshot = metrics.snapshot()
    assert snapshot["state"] == "error"
    assert snapshot["terminal_error"] == "queue overflow"


def test_pipeline_metrics_freeze_elapsed_time_when_the_session_finishes() -> None:
    monotonic = iter((1_000_000_000, 3_000_000_000, 9_000_000_000))
    metrics = PipelineMetrics(monotonic_time_ns=lambda: next(monotonic))
    metrics.begin_session(uuid4())

    metrics.finish()
    snapshot = metrics.snapshot()

    assert snapshot["sampled_at_monotonic_ns"] == 9_000_000_000
    assert snapshot["elapsed_seconds"] == 2.0
