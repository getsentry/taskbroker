import json
import logging

from taskbroker_client.structured_logging import StructuredLogFormatter
from taskbroker_client.worker.events import TraceEvent


def test_structured_log_formatter_includes_cloud_logging_fields() -> None:
    record = logging.LogRecord(
        name="taskbroker_client.worker.worker",
        level=logging.DEBUG,
        pathname=__file__,
        lineno=1,
        msg="taskworker.activation.received",
        args=(),
        exc_info=None,
    )
    record.id = "123"
    record.event = TraceEvent.RECEIVED

    payload = json.loads(StructuredLogFormatter().format(record))

    assert payload["message"] == "taskworker.activation.received"
    assert payload["severity"] == "DEBUG"
    assert payload["logger"] == "taskbroker_client.worker.worker"
    assert payload["id"] == "123"
    assert payload["event"] == "RECEIVED"
    assert "timestamp" in payload
