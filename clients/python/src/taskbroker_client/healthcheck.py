import argparse
import os
import sys


def healthcheck(path: str) -> None:
    """
    Remove the health file, so the next check fails unless the worker touches it
    again. Runs as the worker pod's liveness probe.
    """
    try:
        os.remove(path)
    except FileNotFoundError:
        sys.exit(f"{path} is missing: the worker has not touched it since the last probe")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python3 -m taskbroker_client.healthcheck",
        description="Remove the worker health file, or exit 1 if it is missing.",
    )
    parser.add_argument("path", help="the health check file the worker touches")
    args = parser.parse_args(argv)
    healthcheck(args.path)


if __name__ == "__main__":
    main()
