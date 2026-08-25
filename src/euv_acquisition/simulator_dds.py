from __future__ import annotations

import threading
import uuid

from euv_acquisition.timing import LaserTimingState


class DdsLaserTimingAdapter:
    """Optional simulator adapter; core capture code remains DDS-independent."""

    def __init__(self, laser_subsystem_uuid: uuid.UUID, *, host: str = "127.0.0.1") -> None:
        self._laser_subsystem_uuid = laser_subsystem_uuid
        self._host = host
        self._state: LaserTimingState | None = None
        self._lock = threading.Lock()
        self._configured = False
        self._client = None

    def start(self) -> None:
        import ipi_ecs.dds.client as client

        self._client = client.DDSClient(uuid.uuid4(), ip=self._host)
        self._client.when_ready().then(self._on_ready)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def trigger_enabled(self) -> bool:
        with self._lock:
            return self._state is not None and self._state.triggers_enabled

    def euv_transmitting(self) -> bool:
        with self._lock:
            return self._state is not None and self._state.euv_transmitting()

    def trigger_rate_hz(self) -> float | None:
        with self._lock:
            return None if self._state is None else self._state.trigger_rate_hz

    def _on_ready(self) -> None:
        if self._configured:
            return
        import ipi_ecs.dds.subsystem as subsystem
        import ipi_ecs.dds.types as types

        self._configured = True
        handle = self._client.register_subsystem("__euv_acquisition_simulator", uuid.uuid4(), temporary=True)
        timing_kv = handle.add_remote_kv(
            self._laser_subsystem_uuid,
            subsystem.KVDescriptor(types.ByteTypeSpecifier(), b"timing_status", True, True, False),
        )
        timing_kv.on_new_data_received(self._on_timing_status)

    def _on_timing_status(self, payload: bytes) -> None:
        try:
            state = LaserTimingState.decode(payload)
        except ValueError:
            return
        with self._lock:
            self._state = state