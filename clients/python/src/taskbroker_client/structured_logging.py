from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from enum import Enum
from typing import Any

_STRUCTURED_LOG_HANDLER = "_taskbroker_structured_log_handler"
_RESERVED_LOG_RECORD_ATTRS = set(logging.makeLogRecord({}).__dict__) | {
    "asctime",
    "message",
}


def _json_safe(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, set):
        return sorted(_json_safe(item) for item in value)
    if isinstance(value, BaseException):
        return str(value)
    return value


class StructuredLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "message": record.getMessage(),
            "severity": record.levelname,
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "logger": record.name,
        }

        for key, value in record.__dict__.items():
            if key in _RESERVED_LOG_RECORD_ATTRS or key.startswith("_"):
                continue
            payload[key] = _json_safe(value)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)

        return json.dumps(payload, separators=(",", ":"), default=str)


def configure_taskbroker_structured_logging() -> None:
    """
    Emit taskbroker_client logs as JSON on stdout so container log collectors
    ingest them as structured logs instead of stderr text payloads.
    """
    package_logger = logging.getLogger("taskbroker_client")

    for handler in package_logger.handlers:
        if getattr(handler, _STRUCTURED_LOG_HANDLER, False):
            return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(StructuredLogFormatter())
    setattr(handler, _STRUCTURED_LOG_HANDLER, True)

    package_logger.addHandler(handler)
    package_logger.propagate = False
