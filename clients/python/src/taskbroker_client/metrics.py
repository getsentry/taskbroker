from __future__ import annotations

import atexit
import resource
import time
from abc import abstractmethod
from collections.abc import Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Generator, Protocol, runtime_checkable

if TYPE_CHECKING:
    from datadog.dogstatsd.base import DogStatsd

Tags = Mapping[str, str | int | float]


@runtime_checkable
class MetricsBackend(Protocol):
    """
    An abstract class that defines the interface for metrics backends.
    """

    @abstractmethod
    def gauge(
        self,
        key: str,
        value: float,
        instance: str | None = None,
        tags: Tags | None = None,
        sample_rate: float = 1,
        unit: str | None = None,
        stacklevel: int = 0,
    ) -> None:
        """
        Records a gauge metric (a point-in-time value).
        """
        raise NotImplementedError

    @abstractmethod
    def incr(
        self,
        name: str,
        value: int | float = 1,
        tags: Tags | None = None,
        sample_rate: float | None = None,
    ) -> None:
        """
        Increments a counter metric by a given value.
        """
        raise NotImplementedError

    @abstractmethod
    def distribution(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
        sample_rate: float | None = None,
    ) -> None:
        """
        Records a distribution metric.
        """
        raise NotImplementedError

    @contextmanager
    def timer(
        self,
        key: str,
        tags: Tags | None = None,
        sample_rate: float | None = None,
        stacklevel: int = 0,
    ) -> Generator[None]:
        """
        Records a distribution metric with a context manager.
        """
        raise NotImplementedError

    @contextmanager
    def track_memory_usage(
        self,
        key: str,
        tags: Tags | None = None,
    ) -> Generator[None]:
        """
        Records a distribution metric with a context manager.
        """
        raise NotImplementedError


class NoOpMetricsBackend(MetricsBackend):
    """
    Default metrics backend that does not record anything.
    """

    def gauge(
        self,
        key: str,
        value: float,
        instance: str | None = None,
        tags: Tags | None = None,
        sample_rate: float = 1,
        unit: str | None = None,
        stacklevel: int = 0,
    ) -> None:
        pass

    def incr(
        self,
        name: str,
        value: int | float = 1,
        tags: Tags | None = None,
        sample_rate: float | None = None,
    ) -> None:
        pass

    def distribution(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
        sample_rate: float | None = None,
    ) -> None:
        pass

    @contextmanager
    def timer(
        self,
        key: str,
        tags: Tags | None = None,
        sample_rate: float | None = None,
        stacklevel: int = 0,
    ) -> Generator[None]:
        yield None

    @contextmanager
    def track_memory_usage(
        self,
        key: str,
        tags: Tags | None = None,
    ) -> Generator[None]:
        """
        Records a distrubtion metric that tracks the delta
        of rss_usage between the context manager opening and closing.
        """
        yield None


class DatadogMetrics(MetricsBackend):
    """
    An opinionated metrics backend that emits to Datadog via DogStatsD.

    All metrics are tagged with ``application`` and ``processing_pool`` so that
    dashboards and alerts can be built once and shared across every application
    using taskbroker-client, without depending on a host-application metrics
    prefix.

    When ``enable_prefixed_metrics`` is enabled each metric is emitted twice: once
    prefix-free with ``application`` as a tag, and once with ``application``
    as a metric prefix (and not included in tags). This eases migrating existing
    alerts and dashboards from the prefixed form to the prefix-free form.

    The ``datadog`` package is an optional dependency. Install it with
    ``pip install taskbroker-client[datadog]``.
    """

    def __init__(
        self,
        application: str,
        processing_pool: str | None = None,
        statsd_host: str | None = None,
        statsd_port: str | int | None = None,
        sample_rate: float = 1.0,
        enable_prefixed_metrics: bool = False,
        client: DogStatsd | None = None,
    ) -> None:
        self.application = application
        self.processing_pool = processing_pool or "unknown"
        self.sample_rate = sample_rate
        self.enable_prefixed_metrics = enable_prefixed_metrics
        if client is None:
            from datadog.dogstatsd.base import DogStatsd

            client = DogStatsd(
                host=statsd_host or "localhost",
                port=int(statsd_port) if statsd_port is not None else 8125,
                disable_telemetry=True,
                # Use a background thread to send metrics
                disable_background_sender=False,
            )
            # Origin detection is enabled after 0.45 by default.
            # Disable it since it silently fails.
            # Ref: https://github.com/DataDog/datadogpy/issues/764
            client._container_id = None

            # Call wait_for_pending() before exiting to make sure all pending metrics are sent.
            atexit.register(client.wait_for_pending)

        self.client = client

    def _build_tag_list(self, tags: Tags | None, *, with_application: bool) -> list[str]:
        merged: dict[str, str | int | float] = {"processing_pool": self.processing_pool}
        if with_application:
            merged["application"] = self.application
        if tags:
            # Per-call tags win so call sites can override the structural defaults.
            merged.update(tags)
        return [f"{key}:{value}" for key, value in merged.items()]

    def _emit(
        self,
        method: str,
        name: str,
        value: float,
        tags: Tags | None,
        sample_rate: float | None,
    ) -> None:
        rate = self.sample_rate if sample_rate is None else sample_rate
        emit = getattr(self.client, method)

        # Prefix-free form: application is carried as a tag.
        emit(
            name,
            value,
            tags=self._build_tag_list(tags, with_application=True),
            sample_rate=rate,
        )

        # Prefixed form: application is in the metric name and removed from the tags.
        if self.enable_prefixed_metrics:
            emit(
                f"{self.application}.{name}",
                value,
                tags=self._build_tag_list(tags, with_application=False),
                sample_rate=rate,
            )

    def gauge(
        self,
        key: str,
        value: float,
        instance: str | None = None,
        tags: Tags | None = None,
        sample_rate: float = 1,
        unit: str | None = None,
        stacklevel: int = 0,
    ) -> None:
        # instance, unit and stacklevel have no DogStatsD equivalent and are ignored.
        self._emit("gauge", key, value, tags, sample_rate)

    def incr(
        self,
        name: str,
        value: int | float = 1,
        tags: Tags | None = None,
        sample_rate: float | None = None,
    ) -> None:
        self._emit("increment", name, value, tags, sample_rate)

    def distribution(
        self,
        name: str,
        value: int | float,
        tags: Tags | None = None,
        unit: str | None = None,
        sample_rate: float | None = None,
    ) -> None:
        # unit has no DogStatsD equivalent and is ignored.
        self._emit("distribution", name, value, tags, sample_rate)

    @contextmanager
    def timer(
        self,
        key: str,
        tags: Tags | None = None,
        sample_rate: float | None = None,
        stacklevel: int = 0,
    ) -> Generator[None]:
        start = time.monotonic()
        try:
            yield None
        finally:
            self._emit("timing", key, time.monotonic() - start, tags, sample_rate)

    @contextmanager
    def track_memory_usage(
        self,
        key: str,
        tags: Tags | None = None,
    ) -> Generator[None]:
        """
        Records a distribution metric that tracks the delta
        of rss usage between the context manager opening and closing.
        """
        start = _rss_bytes()
        try:
            yield None
        finally:
            self._emit("distribution", key, _rss_bytes() - start, tags, None)


def _rss_bytes() -> int:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
