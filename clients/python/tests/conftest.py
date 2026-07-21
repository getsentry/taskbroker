from collections.abc import Callable, Generator
from datetime import UTC, datetime
from typing import Any

import pytest
import sentry_sdk
import time_machine
from arroyo.backends.kafka import KafkaProducer
from sentry_sdk.envelope import Envelope
from sentry_sdk.transport import Transport

from taskbroker_client.types import AtMostOnceStore


def producer_factory(topic: str) -> KafkaProducer:
    config = {
        "bootstrap.servers": "127.0.0.1:9092",
        "compression.type": "lz4",
        "message.max.bytes": 50000000,  # 50MB
    }
    return KafkaProducer(config)


def freeze_time(t: str | datetime | None = None) -> time_machine.travel:
    if t is None:
        t = datetime.now(UTC)
    return time_machine.travel(t, tick=False)


@pytest.fixture
def sentry_init() -> Generator[Callable[..., None], None, None]:
    def inner(*a: Any, **kw: Any) -> None:
        kw.setdefault("transport", TestTransport())
        client = sentry_sdk.Client(*a, **kw)
        sentry_sdk.get_global_scope().set_client(client)

    old_client = sentry_sdk.get_global_scope().client
    try:
        sentry_sdk.get_current_scope().set_client(None)
        yield inner
    finally:
        sentry_sdk.get_global_scope().set_client(old_client)


class TestTransport(Transport):
    def capture_envelope(self, _: Envelope) -> None:
        """No-op capture_envelope for tests"""
        pass


class StubAtMostOnce(AtMostOnceStore):
    def __init__(self) -> None:
        self._keys: dict[str, str] = {}

    def add(self, key: str, value: str, timeout: int) -> bool:
        if key in self._keys:
            return False
        self._keys[key] = value
        return True
