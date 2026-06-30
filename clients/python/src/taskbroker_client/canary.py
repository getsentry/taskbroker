from __future__ import annotations

import logging
from time import sleep

CANARY_TASK_NAME = "canary_task"

logger = logging.getLogger(__name__)


def canary_task() -> None:
    logger.info("Running canary task...")
    sleep(0.1)
    print("Done running canary task!")
