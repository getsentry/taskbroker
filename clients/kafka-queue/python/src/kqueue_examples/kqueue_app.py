"""Minimal "app" object for the kqueue worker child. Not Sentry's TaskRegistry."""

from __future__ import annotations

import logging

from kafka_queue_worker.types import NoOpMetrics

logger = logging.getLogger(__name__)
_metrics = NoOpMetrics()


class DemoApp:
    name = "kqueue-demo"
    metrics = _metrics

    def load_modules(self) -> None:
        """Match taskbroker's load_modules hook; no-op for demo."""


app = DemoApp()
