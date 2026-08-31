from __future__ import annotations

import uuid
from typing import Any


class FaultTolerantEcsLogger:
    def __init__(self, logger: Any) -> None:
        self._logger = logger
        self._reported_failure = False

    def log(self, message: str, **kwargs: Any) -> None:
        try:
            self._logger.log(message, **kwargs)
        except Exception as exc:
            if not self._reported_failure:
                print(
                    f"[EUV Acquisition Service] ECS logging failed; continuing with journald: {type(exc).__name__}: {exc}",
                    flush=True,
                )
                self._reported_failure = True


def open_ecs_logger(host: str, port: int, *, origin_uuid: uuid.UUID | None = None):
    try:
        from ipi_ecs.core.tcp import TCPClientSocket
        from ipi_ecs.logging.client import LogClient
    except ImportError as exc:
        print(f"[EUV Acquisition Service] ECS logging unavailable; continuing with journald: {exc}", flush=True)
        return None, None

    transport = TCPClientSocket()
    try:
        transport.connect((host, port))
        transport.start()
    except Exception as exc:
        transport.close()
        print(
            f"[EUV Acquisition Service] Could not start ECS logging for {host}:{port}; continuing with journald: "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )
        return None, None
    return FaultTolerantEcsLogger(LogClient(transport, origin_uuid=origin_uuid)), transport