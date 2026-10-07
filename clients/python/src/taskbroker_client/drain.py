"""
Mark the worker as draining, then wait.

Meant to run as the worker pod's preStop hook:

    python3 -m taskbroker_client.drain 15

Kubernetes only sends SIGTERM once preStop finishes, so creating the drain file
first lets the worker stop publishing occupancy while brokers stop routing to
the pod. Imports nothing beyond the constants, so it starts fast and needs no
shell in the image.
"""

import argparse
import time
from pathlib import Path

from taskbroker_client.constants import DEFAULT_WORKER_DRAIN_FILE_PATH


def drain(seconds: float, path: str = DEFAULT_WORKER_DRAIN_FILE_PATH) -> None:
    Path(path).touch()
    time.sleep(seconds)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python3 -m taskbroker_client.drain",
        description="Create the worker drain file, then sleep.",
    )
    parser.add_argument("seconds", type=float, help="how long to sleep after creating the file")
    args = parser.parse_args(argv)
    drain(args.seconds)


if __name__ == "__main__":
    main()
