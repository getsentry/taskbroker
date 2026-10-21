from __future__ import annotations

from unittest.mock import Mock

import pytest

from taskbroker_client.metrics import DatadogMetrics


def make_metrics(
    *,
    enable_prefixed_metrics: bool = False,
    processing_pool: str | None = "ingest-errors",
    sample_rate: float = 1.0,
    client: Mock | None = None,
) -> tuple[DatadogMetrics, Mock]:
    mock_client = client or Mock()
    metrics = DatadogMetrics(
        application="sentry",
        processing_pool=processing_pool,
        sample_rate=sample_rate,
        enable_prefixed_metrics=enable_prefixed_metrics,
        client=mock_client,
    )
    return metrics, mock_client


def test_incr_prefix_off() -> None:
    metrics, client = make_metrics(sample_rate=0.5)
    metrics.incr("taskworker.x", tags={"namespace": "n"})

    client.increment.assert_called_once()
    args, kwargs = client.increment.call_args
    assert args[0] == "taskworker.x"
    assert args[1] == 1
    assert set(kwargs["tags"]) == {
        "application:sentry",
        "processing_pool:ingest-errors",
        "namespace:n",
    }
    assert kwargs["sample_rate"] == 0.5


def test_incr_prefix_on() -> None:
    metrics, client = make_metrics(enable_prefixed_metrics=True)
    metrics.incr("taskworker.x")

    assert client.increment.call_count == 2

    first_args, first_kwargs = client.increment.call_args_list[0]
    assert first_args[0] == "taskworker.x"
    assert "application:sentry" in first_kwargs["tags"]

    second_args, second_kwargs = client.increment.call_args_list[1]
    assert second_args[0] == "sentry.taskworker.x"
    assert not any(tag.startswith("application:") for tag in second_kwargs["tags"])
    assert "processing_pool:ingest-errors" in second_kwargs["tags"]


def test_gauge_ignores_unsupported_params() -> None:
    metrics, client = make_metrics()
    metrics.gauge("taskworker.size", 12.0, instance="i", unit="bytes", stacklevel=3)

    client.gauge.assert_called_once()
    args, kwargs = client.gauge.call_args
    assert args[0] == "taskworker.size"
    assert args[1] == 12.0
    assert set(kwargs) == {"tags", "sample_rate"}


def test_distribution_ignores_unit() -> None:
    metrics, client = make_metrics()
    metrics.distribution("taskworker.duration", 0.25, unit="seconds")

    client.distribution.assert_called_once()
    args, kwargs = client.distribution.call_args
    assert args[0] == "taskworker.duration"
    assert args[1] == 0.25
    assert "unit" not in kwargs


def test_tag_precedence() -> None:
    metrics, client = make_metrics()
    metrics.incr("taskworker.x", tags={"processing_pool": "override", "namespace": "n"})

    _, kwargs = client.increment.call_args
    assert "processing_pool:override" in kwargs["tags"]
    assert "processing_pool:ingest-errors" not in kwargs["tags"]


def test_none_tags_still_emit_structural_tags() -> None:
    metrics, client = make_metrics()
    metrics.incr("taskworker.x")

    _, kwargs = client.increment.call_args
    assert set(kwargs["tags"]) == {"application:sentry", "processing_pool:ingest-errors"}


def test_sample_rate_defaulting() -> None:
    metrics, client = make_metrics(sample_rate=0.1)

    metrics.incr("taskworker.x")
    assert client.increment.call_args.kwargs["sample_rate"] == 0.1

    metrics.incr("taskworker.x", sample_rate=0.5)
    assert client.increment.call_args.kwargs["sample_rate"] == 0.5


def test_timer_happy_path() -> None:
    metrics, client = make_metrics(enable_prefixed_metrics=True)
    with metrics.timer("taskworker.duration", tags={"host": "h"}):
        pass

    assert client.timing.call_count == 2
    args, kwargs = client.timing.call_args_list[0]
    assert args[0] == "taskworker.duration"
    assert isinstance(args[1], float)
    assert "host:h" in kwargs["tags"]


def test_timer_emits_on_exception() -> None:
    metrics, client = make_metrics()
    with pytest.raises(ValueError):
        with metrics.timer("taskworker.dur"):
            raise ValueError("boom")

    client.timing.assert_called_once()
    assert isinstance(client.timing.call_args.args[1], float)


def test_track_memory_usage() -> None:
    metrics, client = make_metrics()
    with metrics.track_memory_usage("taskworker.mem"):
        var = "a" * 1000000
        var += "b"

    client.distribution.assert_called_once()
    args, _ = client.distribution.call_args
    assert args[0] == "taskworker.mem"
    assert isinstance(args[1], int)
    assert args[1] > 0


def test_track_memory_usage_prefixed() -> None:
    metrics, client = make_metrics(enable_prefixed_metrics=True)
    with metrics.track_memory_usage("taskworker.mem"):
        pass

    assert client.distribution.call_count == 2
    assert client.distribution.call_args_list[1].args[0] == "sentry.taskworker.mem"
