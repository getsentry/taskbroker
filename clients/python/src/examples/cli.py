import logging
import os
import time

import click

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    handlers=[logging.StreamHandler()],
)


@click.group()
def main() -> None:
    click.echo("Example application")


@main.command()
@click.option(
    "--count",
    help="The number of tasks to generate",
    default=1,
)
def spawn(count: int = 1) -> None:
    from examples.tasks import timed_task

    click.echo(f"Spawning {count} tasks")
    for _ in range(0, count):
        timed_task.delay(sleep_seconds=0.1)
    click.echo("Complete")


@main.command()
def scheduler() -> None:
    from redis import StrictRedis

    from examples.app import app
    from taskbroker_client.metrics import NoOpMetricsBackend
    from taskbroker_client.scheduler import RunStorage, ScheduleRunner, crontab

    redis_host = os.getenv("REDIS_HOST") or "localhost"
    redis_port = int(os.getenv("REDIS_PORT") or 6379)

    # Ensure all task modules are loaded.
    app.load_modules()

    redis = StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
    metrics = NoOpMetricsBackend()
    run_storage = RunStorage(metrics=metrics, redis=redis)
    scheduler = ScheduleRunner(app, run_storage)

    # Define a scheduled task
    scheduler.add(
        "simple-task", {"task": "examples:examples.simple_task", "schedule": crontab(minute="*/1")}
    )

    click.echo("Starting scheduler")
    scheduler.log_startup()
    while True:
        sleep_time = scheduler.tick()
        time.sleep(sleep_time)


@main.command()
@click.option(
    "--rpc-host",
    help="The address of the taskbroker this worker connects to.",
    default="127.0.0.1:50051",
)
@click.option(
    "--concurrency",
    help="The number of child processes to start.",
    default=2,
)
@click.option(
    "--push-mode", help="Whether to run in PUSH or PULL mode.", default=False, is_flag=True
)
@click.option(
    "--grpc-port",
    help="Port for the gRPC server to listen on.",
    default=50052,
    type=int,
)
def worker(rpc_host: str, concurrency: int, push_mode: bool, grpc_port: int) -> None:
    from taskbroker_client.worker import PushTaskWorker, TaskWorker

    click.echo("Starting worker")
    if push_mode:
        worker: PushTaskWorker | TaskWorker = PushTaskWorker(
            app_module="examples.app:app",
            broker_service="examples",
            max_child_task_count=100,
            concurrency=concurrency,
            child_tasks_queue_maxsize=concurrency * 2,
            result_queue_maxsize=concurrency * 2,
            rebalance_after=32,
            processing_pool_name="examples",
            process_type="forkserver",
            grpc_port=grpc_port,
        )
    else:
        worker = TaskWorker(
            app_module="examples.app:app",
            broker_hosts=[rpc_host],
            max_child_task_count=100,
            concurrency=concurrency,
            child_tasks_queue_maxsize=concurrency * 2,
            result_queue_maxsize=concurrency * 2,
            rebalance_after=32,
            processing_pool_name="examples",
            process_type="forkserver",
        )
    exitcode = worker.start()
    raise SystemExit(exitcode)


if __name__ == "__main__":
    main()
