import argparse
import time
from pathlib import Path

from taskbroker_client.constants import DEFAULT_WORKER_DRAIN_FILE_PATH


def drain(seconds: float, path: str = DEFAULT_WORKER_DRAIN_FILE_PATH) -> None:
    """
    Create the drain file, then sleep. Runs as the worker pod's preStop hook,
    which must finish before Kubernetes sends SIGTERM.
    """
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
