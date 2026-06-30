from __future__ import annotations

import logging
from time import sleep

CANARY_TASK_NAME = "canary_task"
CANARY_TASK_SLEEP_SECONDS = 0.1

logger = logging.getLogger(__name__)


def canary_task() -> None:
    logger.info("Running canary task...")
    sleep(CANARY_TASK_SLEEP_SECONDS)
    print("Done running canary task!")
