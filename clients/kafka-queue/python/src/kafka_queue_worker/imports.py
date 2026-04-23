from __future__ import annotations

import importlib
from typing import Any, TypeVar

T = TypeVar("T")


def import_string(path: str) -> Any:
    """Dotted module + attribute, e.g. ``kqueue_proto.examples.app:app``"""
    if ":" not in path:
        return importlib.import_module(path)
    mod, attr = path.split(":", 1)
    m = importlib.import_module(mod)
    return getattr(m, attr)
