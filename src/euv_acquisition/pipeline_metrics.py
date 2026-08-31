from __future__ import annotations

import math
import threading
import time
from collections import deque
from collections.abc import Callable
from typing import Any
from uuid import UUID


PIPELINE_METRICS_SCHEMA_VERSION = 1
PIPELINE_TIMING_STAGES = frozenset(
    {
        "prefill_wait",
        "trigger_wait",
        "buffer_fill_wait",
        "hardware_read",
        "window_copy",
        "rearm",
        "capture_queue_wait",
        "analysis",
        "persistence_queue_wait",
        "snapshot_write",
        "outbound_queue_wait",
        "socket_send",
        "trigger_to_report",
    }
)


class PipelineMetrics:
    def __init__(
        self,
        *,
        sample_limit: int = 512,
        unix_time_ns: Callable[[], int] = time.time_ns,
        monotonic_time_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        if isinstance(sample_limit, bool) or not isinstance(sample_limit, int) or sample_limit <= 0:
            raise ValueError("sample_limit must be a positive integer.")
        self._sample_limit = sample_limit
        self._unix_time_ns = unix_time_ns
        self._monotonic_time_ns = monotonic_time_ns
        self._lock = threading.Lock()
        self._session_id: UUID | None = None
        self._state = "idle"
        self._requested_mode: str | None = None
        self._effective_mode: str | None = None
        self._fallback_reason: str | None = None
        self._terminal_error: str | None = None
        self._started_at_monotonic_ns: int | None = None
        self._finished_at_monotonic_ns: int | None = None
        self._stages = {name: deque(maxlen=sample_limit) for name in PIPELINE_TIMING_STAGES}
        self._stage_totals = {name: 0 for name in PIPELINE_TIMING_STAGES}
        self._counters: dict[str, int] = {}
        self._queues: dict[str, dict[str, int]] = {}

    def begin_session(
        self,
        session_id: UUID,
        *,
        requested_mode: str | None = None,
        effective_mode: str | None = None,
        fallback_reason: str | None = None,
    ) -> None:
        if not isinstance(session_id, UUID):
            raise ValueError("session_id must be a UUID.")
        for name, value in (
            ("requested_mode", requested_mode),
            ("effective_mode", effective_mode),
            ("fallback_reason", fallback_reason),
        ):
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"{name} must be a non-empty string when present.")
        with self._lock:
            self._session_id = session_id
            self._state = "active"
            self._requested_mode = requested_mode
            self._effective_mode = effective_mode
            self._fallback_reason = fallback_reason
            self._terminal_error = None
            self._started_at_monotonic_ns = self._monotonic_time_ns()
            self._finished_at_monotonic_ns = None
            for samples in self._stages.values():
                samples.clear()
            for stage in self._stage_totals:
                self._stage_totals[stage] = 0
            self._counters.clear()
            self._queues.clear()

    def set_capture_mode(
        self,
        *,
        requested_mode: str | None,
        effective_mode: str | None,
        fallback_reason: str | None = None,
    ) -> None:
        with self._lock:
            self._requested_mode = requested_mode
            self._effective_mode = effective_mode
            self._fallback_reason = fallback_reason

    def record_duration(self, stage: str, duration_ns: int) -> None:
        if stage not in PIPELINE_TIMING_STAGES:
            raise ValueError(f"Unknown pipeline timing stage {stage!r}.")
        if isinstance(duration_ns, bool) or not isinstance(duration_ns, int) or duration_ns < 0:
            raise ValueError("duration_ns must be a non-negative integer.")
        with self._lock:
            self._stages[stage].append(duration_ns)
            self._stage_totals[stage] += 1

    def increment(self, counter: str, amount: int = 1) -> None:
        if not isinstance(counter, str) or not counter.strip():
            raise ValueError("counter must be a non-empty string.")
        if isinstance(amount, bool) or not isinstance(amount, int) or amount < 0:
            raise ValueError("amount must be a non-negative integer.")
        with self._lock:
            self._counters[counter] = self._counters.get(counter, 0) + amount

    def observe_queue(self, name: str, *, depth: int, capacity: int) -> None:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Queue name must be a non-empty string.")
        for field, value in (("depth", depth), ("capacity", capacity)):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"Queue {field} must be a non-negative integer.")
        if capacity == 0 or depth > capacity:
            raise ValueError("Queue depth must not exceed a positive capacity.")
        with self._lock:
            previous = self._queues.get(name)
            high_water = depth if previous is None else max(depth, previous["high_water"])
            self._queues[name] = {"depth": depth, "capacity": capacity, "high_water": high_water}

    def finish(self, *, terminal_error: str | None = None) -> None:
        if terminal_error is not None and (not isinstance(terminal_error, str) or not terminal_error.strip()):
            raise ValueError("terminal_error must be a non-empty string when present.")
        with self._lock:
            self._state = "error" if terminal_error is not None else "stopped"
            self._terminal_error = terminal_error
            self._finished_at_monotonic_ns = self._monotonic_time_ns()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            sampled_at_monotonic_ns = self._monotonic_time_ns()
            stages = {
                name: self._stage_value(name, tuple(samples))
                for name, samples in self._stages.items()
                if samples
            }
            elapsed_seconds = None
            if self._started_at_monotonic_ns is not None:
                elapsed_at_ns = (
                    sampled_at_monotonic_ns
                    if self._finished_at_monotonic_ns is None
                    else self._finished_at_monotonic_ns
                )
                elapsed_ns = elapsed_at_ns - self._started_at_monotonic_ns
                elapsed_seconds = max(0.0, elapsed_ns / 1e9)
            return {
                "schema_version": PIPELINE_METRICS_SCHEMA_VERSION,
                "sampled_at_unix_ns": self._unix_time_ns(),
                "sampled_at_monotonic_ns": sampled_at_monotonic_ns,
                "session_id": None if self._session_id is None else str(self._session_id),
                "state": self._state,
                "elapsed_seconds": elapsed_seconds,
                "capture_mode": {
                    "requested": self._requested_mode,
                    "effective": self._effective_mode,
                    "fallback_reason": self._fallback_reason,
                },
                "counters": dict(sorted(self._counters.items())),
                "queues": {name: dict(value) for name, value in sorted(self._queues.items())},
                "stages": stages,
                "terminal_error": self._terminal_error,
            }

    def _stage_value(self, stage: str, samples: tuple[int, ...]) -> dict[str, int | float]:
        ordered = sorted(samples)
        percentile_index = max(0, math.ceil(len(ordered) * 0.95) - 1)
        return {
            "count": self._stage_totals[stage],
            "recent_count": len(samples),
            "mean_ms": sum(samples) / len(samples) / 1e6,
            "p95_ms": ordered[percentile_index] / 1e6,
            "max_ms": ordered[-1] / 1e6,
        }
