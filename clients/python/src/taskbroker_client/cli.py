import logging
import os
import time

import click

from taskbroker_client.constants import (
    DEFAULT_GRPC_MAX_MESSAGE_SIZE,
    DEFAULT_REBALANCE_AFTER,
    DEFAULT_WORKER_HEALTH_CHECK_SEC_PER_TOUCH,
    DEFAULT_WORKER_QUEUE_SIZE,
    DEFAULT_WORKER_WARMUP_TIMEOUT_SEC,
)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    handlers=[logging.StreamHandler()],
)


@click.command()
@click.option(
    "--count",
    help="The number of tasks to generate",
    default=1,
)
@click.option(
    "--sleep-seconds",
    help="How long each task sleeps. Use a larger value to make occupancy observable.",
    default=0.1,
    type=float,
)
def spawn(count: int = 1, sleep_seconds: float = 0.1) -> None:
    from examples.tasks import timed_task

    click.echo(f"Spawning {count} tasks")
    for _ in range(0, count):
        timed_task.delay(sleep_seconds=sleep_seconds)
    click.echo("Complete")


@click.command()
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


@click.command()
@click.option(
    "--app-module",
    help="The dotted path to the TaskRegistry app, e.g. 'examples.app:app'.",
    default="examples.app:app",
)
@click.option(
    "--broker-host",
    "broker_hosts",
    help="The address of a taskbroker this worker connects to. Repeat to add multiple hosts.",
    multiple=True,
    default=("127.0.0.1:50051",),
)
@click.option(
    "--max-child-task-count",
    help="Restart a child process after it has executed this many tasks. Unset = no limit.",
    default=None,
    type=int,
)
@click.option(
    "--namespace",
    help="Restrict this worker to a single task namespace. Unset = all namespaces.",
    default=None,
)
@click.option(
    "--concurrency",
    help="The number of child processes to start.",
    default=1,
    type=int,
)
@click.option(
    "--min-concurrency",
    help="The minimum number of child processes to keep running.",
    default=0,
    type=int,
)
@click.option(
    "--child-tasks-queue-maxsize",
    help="Maximum number of pending tasks buffered for child processes.",
    default=DEFAULT_WORKER_QUEUE_SIZE,
    type=int,
)
@click.option(
    "--result-queue-maxsize",
    help="Maximum number of pending results buffered from child processes.",
    default=DEFAULT_WORKER_QUEUE_SIZE,
    type=int,
)
@click.option(
    "--rebalance-after",
    help="Rebalance to a new broker host after this many tasks.",
    default=DEFAULT_REBALANCE_AFTER,
    type=int,
)
@click.option(
    "--processing-pool-name",
    help="The name of the processing pool this worker belongs to.",
    default=None,
)
@click.option(
    "--process-type",
    help="The multiprocessing start method used for child processes.",
    default="spawn",
    type=click.Choice(["fork", "spawn", "forkserver"]),
)
@click.option(
    "--health-check-file-path",
    help="Path to a file that is touched periodically for health checks. Unset = disabled.",
    default=None,
)
@click.option(
    "--health-check-sec-per-touch",
    help="Number of seconds between health check file touches.",
    default=DEFAULT_WORKER_HEALTH_CHECK_SEC_PER_TOUCH,
    type=float,
)
@click.option(
    "--skip-awaiting-futures/--no-skip-awaiting-futures",
    help="Whether to skip awaiting futures when completing tasks.",
    default=True,
)
@click.option(
    "--future-checking-frequency",
    help="How often, in seconds, to check for completed futures.",
    default=0.1,
    type=float,
)
@click.option(
    "--track-producer-futures/--no-track-producer-futures",
    help="Set ARROYO_TRACK_PRODUCER_FUTURES to enable arroyo producer future tracking.",
    default=False,
)
def worker(
    app_module: str,
    broker_hosts: tuple[str, ...],
    max_child_task_count: int | None,
    namespace: str | None,
    concurrency: int,
    min_concurrency: int,
    child_tasks_queue_maxsize: int,
    result_queue_maxsize: int,
    rebalance_after: int,
    processing_pool_name: str | None,
    process_type: str,
    health_check_file_path: str | None,
    health_check_sec_per_touch: float,
    skip_awaiting_futures: bool,
    future_checking_frequency: float,
    track_producer_futures: bool,
) -> None:
    from taskbroker_client.worker import TaskWorker

    os.environ["ARROYO_TRACK_PRODUCER_FUTURES"] = str(track_producer_futures)

    click.echo("Starting worker")
    worker = TaskWorker(
        app_module=app_module,
        broker_hosts=list(broker_hosts),
        max_child_task_count=max_child_task_count,
        namespace=namespace,
        concurrency=concurrency,
        min_concurrency=min_concurrency,
        child_tasks_queue_maxsize=child_tasks_queue_maxsize,
        result_queue_maxsize=result_queue_maxsize,
        rebalance_after=rebalance_after,
        processing_pool_name=processing_pool_name,
        process_type=process_type,
        health_check_file_path=health_check_file_path,
        health_check_sec_per_touch=health_check_sec_per_touch,
        skip_awaiting_futures=skip_awaiting_futures,
        future_checking_frequency=future_checking_frequency,
    )
    exitcode = worker.start()
    raise SystemExit(exitcode)


@click.command()
@click.option(
    "--app-module",
    help="The dotted path to the TaskRegistry app, e.g. 'examples.app:app'.",
    default="examples.app:app",
)
@click.option(
    "--broker-service",
    help="The address of the taskbroker that pushes tasks to this worker.",
    default="127.0.0.1:50051",
)
@click.option(
    "--max-child-task-count",
    help="Restart a child process after it has executed this many tasks. Unset = no limit.",
    default=None,
    type=int,
)
@click.option(
    "--namespace",
    help="Restrict this worker to a single task namespace. Unset = all namespaces.",
    default=None,
)
@click.option(
    "--concurrency",
    help="The number of child processes to start.",
    default=1,
    type=int,
)
@click.option(
    "--min-concurrency",
    help="The minimum number of child processes to keep running.",
    default=0,
    type=int,
)
@click.option(
    "--child-tasks-queue-maxsize",
    help="Maximum number of pending tasks buffered for child processes.",
    default=DEFAULT_WORKER_QUEUE_SIZE,
    type=int,
)
@click.option(
    "--result-queue-maxsize",
    help="Maximum number of pending results buffered from child processes.",
    default=DEFAULT_WORKER_QUEUE_SIZE,
    type=int,
)
@click.option(
    "--rebalance-after",
    help="Rebalance to a new broker host after this many tasks.",
    default=DEFAULT_REBALANCE_AFTER,
    type=int,
)
@click.option(
    "--processing-pool-name",
    help="The name of the processing pool this worker belongs to.",
    default=None,
)
@click.option(
    "--pod-name",
    help="The name of the pod this worker runs in.",
    default=None,
)
@click.option(
    "--process-type",
    help="The multiprocessing start method used for child processes.",
    default="spawn",
    type=click.Choice(["fork", "spawn", "forkserver"]),
)
@click.option(
    "--health-check-file-path",
    help="Path to a file that is touched periodically for health checks. Unset = disabled.",
    default=None,
)
@click.option(
    "--health-check-sec-per-touch",
    help="Number of seconds between health check file touches.",
    default=DEFAULT_WORKER_HEALTH_CHECK_SEC_PER_TOUCH,
    type=float,
)
@click.option(
    "--grpc-port",
    help="Port for the gRPC server to listen on.",
    default=50052,
    type=int,
)
@click.option(
    "--grpc-max-message-size",
    help="Maximum gRPC send/receive message size, in bytes.",
    default=DEFAULT_GRPC_MAX_MESSAGE_SIZE,
    type=int,
)
@click.option(
    "--push-task-timeout",
    help="Timeout, in seconds, for pushed tasks.",
    default=5,
    type=float,
)
@click.option(
    "--skip-awaiting-futures/--no-skip-awaiting-futures",
    help="Whether to skip awaiting futures when completing tasks.",
    default=True,
)
@click.option(
    "--warmup-timeout",
    help="Timeout, in seconds, to wait for child processes to warm up.",
    default=DEFAULT_WORKER_WARMUP_TIMEOUT_SEC,
    type=float,
)
@click.option(
    "--prometheus-port",
    help="Expose prometheus metrics on this port for scraping. Unset = disabled.",
    default=None,
    type=int,
)
@click.option(
    "--future-checking-frequency",
    help="How often, in seconds, to check for completed futures.",
    default=0.1,
    type=float,
)
@click.option(
    "--track-producer-futures/--no-track-producer-futures",
    help="Set ARROYO_TRACK_PRODUCER_FUTURES to enable arroyo producer future tracking.",
    default=False,
)
def push_worker(
    app_module: str,
    broker_service: str,
    max_child_task_count: int | None,
    namespace: str | None,
    concurrency: int,
    min_concurrency: int,
    child_tasks_queue_maxsize: int,
    result_queue_maxsize: int,
    rebalance_after: int,
    processing_pool_name: str | None,
    pod_name: str | None,
    process_type: str,
    health_check_file_path: str | None,
    health_check_sec_per_touch: float,
    grpc_port: int,
    grpc_max_message_size: int,
    push_task_timeout: float,
    skip_awaiting_futures: bool,
    warmup_timeout: float,
    prometheus_port: int | None,
    future_checking_frequency: float,
    track_producer_futures: bool,
) -> None:
    from taskbroker_client.worker import PushTaskWorker

    os.environ["ARROYO_TRACK_PRODUCER_FUTURES"] = str(track_producer_futures)

    click.echo("Starting push worker")
    worker = PushTaskWorker(
        app_module=app_module,
        broker_service=broker_service,
        max_child_task_count=max_child_task_count,
        namespace=namespace,
        concurrency=concurrency,
        min_concurrency=min_concurrency,
        child_tasks_queue_maxsize=child_tasks_queue_maxsize,
        result_queue_maxsize=result_queue_maxsize,
        rebalance_after=rebalance_after,
        processing_pool_name=processing_pool_name,
        pod_name=pod_name,
        process_type=process_type,
        health_check_file_path=health_check_file_path,
        health_check_sec_per_touch=health_check_sec_per_touch,
        grpc_port=grpc_port,
        grpc_max_message_size=grpc_max_message_size,
        push_task_timeout=push_task_timeout,
        skip_awaiting_futures=skip_awaiting_futures,
        warmup_timeout=warmup_timeout,
        prometheus_port=prometheus_port,
        future_checking_frequency=future_checking_frequency,
    )
    exitcode = worker.start()
    raise SystemExit(exitcode)
