"""Shared logging for the main process and multiprocessing ``spawn`` children (each is a new interpreter with no parent ``basicConfig``)."""

from __future__ import annotations

import logging


def configure_kqueue_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(message)s",
    )
