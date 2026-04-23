from __future__ import annotations

import sys

import click
from kafka_queue_worker.logging_config import configure_kqueue_logging
from kafka_queue_worker.worker import TaskQueueWorker

configure_kqueue_logging()


@click.group()
def main() -> None:
    """KIP-932 prototype: worker that consumes via Java gRPC bridge."""


@main.command("worker")
@click.option(
    "--bridge",
    "bridge_url",
    envvar="QUEUE_BRIDGE_ADDR",
    default="127.0.0.1:50060",
    help="gRPC address of the Java queue-bridge (host:port, no protocol prefix).",
)
@click.option(
    "--app",
    "app_module",
    default="kqueue_examples.kqueue_app:app",
    show_default=True,
    help="module:attr for a minimal app (metrics, load_modules).",
)
@click.option("--concurrency", default=2, type=int)
def worker_cmd(bridge_url: str, app_module: str, concurrency: int) -> None:
    w = TaskQueueWorker(
        app_module=app_module,
        bridge_url=bridge_url,
        max_child_task_count=1000,
        concurrency=concurrency,
        child_tasks_queue_maxsize=concurrency * 2,
        result_queue_maxsize=concurrency * 2,
        processing_pool_name="kqueue",
        process_type="spawn",
    )
    raise SystemExit(w.start())


if __name__ == "__main__":
    sys.exit(main())
