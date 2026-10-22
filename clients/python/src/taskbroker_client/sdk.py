from contextlib import nullcontext
from typing import Any, ContextManager

import sentry_sdk
from sentry_sdk.scope import Scope
from sentry_sdk.traces import StreamedSpan


def start_segment(
    name: str,
    op: str,
    origin: str,
    attributes: dict[str, Any],
    headers: dict[str, Any],
    sampling_context: dict[str, Any],
) -> StreamedSpan | ContextManager[Any]:
    """Continue the trace from `headers` and start a segment span."""
    try:
        sentry_sdk.traces.continue_trace(headers)
        Scope.set_custom_sampling_context(sampling_context)

        return sentry_sdk.traces.start_span(
            name=name,
            attributes={
                "sentry.op": op,
                "sentry.origin": origin,
                **attributes,
            },
        )
    except Exception:
        pass

    return nullcontext()


def start_span(
    name: str, op: str, origin: str, attributes: dict[str, Any]
) -> StreamedSpan | ContextManager[Any]:
    """Start a child span of the currently active span."""
    try:
        # Early return to avoid large increase in span volume.
        # Only create spans when there is an active trace, e.g. a task spawned from a request.
        if sentry_sdk.traces.get_current_span() is None:
            return nullcontext()

        return sentry_sdk.traces.start_span(
            name=name,
            attributes={
                "sentry.op": op,
                "sentry.origin": origin,
                **attributes,
            },
        )
    except Exception:
        pass

    return nullcontext()
