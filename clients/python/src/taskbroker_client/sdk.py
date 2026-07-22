from contextlib import nullcontext
from typing import Any, ContextManager

import sentry_sdk
from sentry_sdk.scope import Scope
from sentry_sdk.traces import StreamedSpan
from sentry_sdk.tracing import NoOpSpan, Span, Transaction
from sentry_sdk.tracing_utils import has_span_streaming_enabled


def start_transaction(
    name: str, op: str, origin: str, headers: dict[str, Any], sampling_context: dict[str, Any]
) -> Transaction | NoOpSpan | StreamedSpan | ContextManager[Any]:
    """Start a transaction, or a span if span streaming is enabled."""
    span = None
    try:
        is_span_streaming = has_span_streaming_enabled(sentry_sdk.get_client().options)
        if is_span_streaming:
            sentry_sdk.traces.continue_trace(headers)
            Scope.set_custom_sampling_context(sampling_context)

            return sentry_sdk.traces.start_span(
                name=name,
                attributes={
                    "sentry.op": op,
                    "sentry.origin": origin,
                },
            )

        transaction = sentry_sdk.continue_trace(
            environ_or_headers=headers,
            op=op,
            name=name,
            origin=origin,
        )

        span = sentry_sdk.start_transaction(transaction, custom_sampling_context=sampling_context)
    except Exception:
        pass

    if span is None:
        return nullcontext()
    return span


def start_span(name: str, op: str, origin: str) -> Span | StreamedSpan | ContextManager[Any]:
    """Start a span in the currently active trace lifecycle."""
    try:
        is_span_streaming = has_span_streaming_enabled(sentry_sdk.get_client().options)
        if is_span_streaming:
            return sentry_sdk.traces.start_span(
                name=name,
                attributes={
                    "sentry.op": op,
                    "sentry.origin": origin,
                },
            )

        return sentry_sdk.start_span(
            op=op,
            name=name,
            origin=origin,
        )
    except Exception:
        pass

    return nullcontext()
