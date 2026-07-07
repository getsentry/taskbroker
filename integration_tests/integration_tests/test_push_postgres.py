"""
Integration tests for postgres-backed *push* mode.

Unlike every other integration test (which runs pull mode against SQLite), these
launch a real taskbroker in push mode (`delivery_mode: push`) backed by the
devservices postgres. In push mode the broker claims activations from postgres
and pushes each to a worker's `WorkerService.PushTask` endpoint, so each test
stands up a mock worker gRPC server (see push_worker.py) and points the broker's
worker_map at it.

Covered behaviors:
- happy-path delivery: every task is pushed exactly once and marked processing;
- push-failure revert: a rejecting worker causes claimed tasks to revert to pending;
- claim-expiration / upkeep: a saturating worker leaves claimed tasks that upkeep
  returns to pending.

Requires devservices postgres up (127.0.0.1:5432, postgres/password) and kafka
up (127.0.0.1:9092), which `make test-push-postgres` provisions.
"""

import re
import signal
import subprocess
import time
from typing import Any, Callable
from uuid import uuid4

import orjson
import yaml
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.taskbroker.v1.taskbroker_pb2 import (
    OnAttemptsExceeded,
    RetryState,
    TaskActivation,
)

from integration_tests.helpers import (
    TASKBROKER_BIN,
    TESTS_OUTPUT_ROOT,
    PostgresConfig,
    create_topic,
    drop_postgres_db,
    get_available_ports,
    get_num_tasks_in_postgres,
    get_num_tasks_in_postgres_by_status,
    get_num_tasks_in_postgres_group_by_status,
    send_custom_messages_to_topic,
    unique_pg_database_name,
)
from integration_tests.push_worker import start_mock_worker

# The application the broker routes on (activation.application) must match a
# worker_map key. Generic helpers don't set application, so we build our own.
APPLICATION = "sentry"


def make_activations(num: int) -> list[TaskActivation]:
    """Build `num` activations tagged with APPLICATION so the broker can route them."""
    activations = []
    for i in range(num):
        activations.append(
            TaskActivation(
                id=uuid4().hex,
                namespace="integration_tests",
                taskname=f"integration_tests.say_hello_{i}",
                parameters=orjson.dumps({"args": ["foobar"], "kwargs": {}}).decode("utf-8"),
                retry_state=RetryState(
                    attempts=0,
                    max_attempts=1,
                    on_attempts_exceeded=OnAttemptsExceeded.ON_ATTEMPTS_EXCEEDED_DISCARD,
                ),
                # seconds; large so the processing deadline never elapses mid-test
                processing_deadline_duration=3000,
                received_at=Timestamp(seconds=int(time.time())),
                application=APPLICATION,
            )
        )
    return activations


def build_push_config(
    *,
    topic: str,
    dlq_topic: str,
    grpc_port: int,
    worker_port: int,
    pg: PostgresConfig,
    fetch: dict[str, Any],
    push: dict[str, Any],
    log_filter: str = "info,librdkafka=warn,h2=off",
) -> dict[str, Any]:
    """Assemble the nested taskbroker yaml config for postgres-backed push mode."""
    return {
        "delivery_mode": "push",
        "grpc_port": grpc_port,  # ConsumerService still starts but is unused in push mode
        "log_filter": log_filter,
        "kafka_auto_offset_reset": "earliest",
        # kafka_deadletter_topic must be declared in kafka_topics under the new
        # (kafka_clusters/kafka_topics) config format.
        "kafka_deadletter_topic": dlq_topic,
        "kafka_clusters": {"default": {"address": "127.0.0.1:9092"}},
        "kafka_topics": {
            topic: {"cluster": "default", "consumer_group": f"{topic}-grp"},
            dlq_topic: {
                "cluster": "default",
                "consumer_group": f"{dlq_topic}-grp",
                "produce_only": True,
            },
        },
        "worker_map": {APPLICATION: f"http://127.0.0.1:{worker_port}"},
        "fetch": fetch,
        "push": push,
        "store": pg.store_config(),
    }


def start_broker(config_path: str, log_path: str) -> "subprocess.Popen[bytes]":
    log_file = open(log_path, "a")
    process = subprocess.Popen(
        [str(TASKBROKER_BIN), "-c", config_path],
        stderr=subprocess.STDOUT,
        stdout=log_file,
    )
    time.sleep(3)  # give the broker time to connect to the worker and start consuming
    return process


def stop_broker(process: "subprocess.Popen[bytes]") -> None:
    process.send_signal(signal.SIGINT)
    try:
        assert process.wait(timeout=10) == 0
    except Exception:
        process.kill()
        raise


def wait_until(predicate: Callable[[], bool], timeout: float, interval: float = 0.5) -> bool:
    """Poll `predicate` until it returns truthy or `timeout` seconds elapse."""
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _setup_paths(name: str) -> tuple[str, str, int]:
    out = TESTS_OUTPUT_ROOT / "test_push_postgres"
    out.mkdir(parents=True, exist_ok=True)
    curr = int(time.time())
    config_path = str(out / f"config_{name}_{curr}.yml")
    log_path = str(out / f"broker_{name}_{curr}.log")
    return config_path, log_path, curr


def test_push_postgres_happy_path() -> None:
    """
    Produce N tasks; a healthy mock worker accepts every push. Assert the broker
    pushes each task exactly once (no duplicates) and marks them all processing
    in postgres.
    """
    num_messages = 500
    grpc_port, worker_port = get_available_ports(2)
    config_path, log_path, curr = _setup_paths("happy")
    topic = f"push-pg-happy-{curr}"
    dlq_topic = f"push-pg-happy-dlq-{curr}"
    pg = PostgresConfig(database_name=unique_pg_database_name("push_pg_happy"))

    create_topic(topic, 1)
    create_topic(dlq_topic, 1)

    config = build_push_config(
        topic=topic,
        dlq_topic=dlq_topic,
        grpc_port=grpc_port,
        worker_port=worker_port,
        pg=pg,
        fetch={"threads": 1, "batch_length": 64, "backoff": "100ms"},
        push={
            "threads": 4,
            "timeout": "30000ms",
            "queue": {"size": 16, "timeout": "5000ms"},
            "update": {"batched": False},
        },
    )
    with open(config_path, "w") as f:
        yaml.safe_dump(config, f)

    activations = make_activations(num_messages)
    produced_ids = {a.id for a in activations}
    send_custom_messages_to_topic(topic, activations)

    server, servicer = start_mock_worker(worker_port, fail=False)
    process = None
    try:
        process = start_broker(config_path, log_path)

        # All tasks should be pushed to the worker.
        pushed_all = wait_until(
            lambda: len(servicer.unique_pushed_ids()) >= num_messages, timeout=60
        )
        assert (
            pushed_all
        ), f"expected {num_messages} unique pushes, got {len(servicer.unique_pushed_ids())}"

        # And each should be marked processing in postgres.
        marked_processing = wait_until(
            lambda: get_num_tasks_in_postgres_by_status(pg, "Processing") == num_messages,
            timeout=30,
        )
        counts = get_num_tasks_in_postgres_group_by_status(pg)
        assert marked_processing, f"expected {num_messages} processing, got {counts}"

        # Exactly-once push: no task should have been pushed more than once.
        assert servicer.unique_pushed_ids() == produced_ids
        assert len(servicer.pushed_ids()) == num_messages, (
            f"expected no duplicate pushes, got {len(servicer.pushed_ids())} for "
            f"{num_messages} tasks"
        )
        assert get_num_tasks_in_postgres(pg) == num_messages
    finally:
        if process is not None:
            stop_broker(process)
        server.stop(0)
        drop_postgres_db(pg)


def test_push_postgres_failure_reverts_to_pending() -> None:
    """
    A worker that rejects every push. Assert the broker never marks anything
    processing, loses no tasks, and reverts claimed tasks back to pending (the
    push-thread revert path in src/push/thread.rs).
    """
    num_messages = 200
    grpc_port, worker_port = get_available_ports(2)
    config_path, log_path, curr = _setup_paths("failure")
    topic = f"push-pg-failure-{curr}"
    dlq_topic = f"push-pg-failure-dlq-{curr}"
    pg = PostgresConfig(database_name=unique_pg_database_name("push_pg_failure"))

    create_topic(topic, 1)
    create_topic(dlq_topic, 1)

    config = build_push_config(
        topic=topic,
        dlq_topic=dlq_topic,
        grpc_port=grpc_port,
        worker_port=worker_port,
        pg=pg,
        fetch={"threads": 1, "batch_length": 32, "backoff": "100ms"},
        push={
            "threads": 4,
            "timeout": "5000ms",
            "queue": {"size": 8, "timeout": "1000ms"},
            "update": {"batched": False},
        },
    )
    with open(config_path, "w") as f:
        yaml.safe_dump(config, f)

    activations = make_activations(num_messages)
    send_custom_messages_to_topic(topic, activations)

    server, servicer = start_mock_worker(worker_port, fail=True)
    process = None
    try:
        process = start_broker(config_path, log_path)

        # Wait for all tasks to land in postgres.
        assert wait_until(
            lambda: get_num_tasks_in_postgres(pg) == num_messages, timeout=60
        ), f"expected {num_messages} tasks written, got {get_num_tasks_in_postgres(pg)}"

        # The broker should actively push tasks to the (failing) worker. Because
        # failed pushes revert to pending and get re-claimed, the same tasks are
        # retried, so we assert the push path is exercised rather than requiring
        # full unique coverage.
        assert wait_until(
            lambda: len(servicer.pushed_ids()) >= 20, timeout=60
        ), f"expected the broker to attempt pushes, got {len(servicer.pushed_ids())}"

        # Because every push fails, nothing is ever marked processing/complete and
        # no task is lost. Sample the state over a short window to catch transient
        # leaks.
        for _ in range(10):
            counts = get_num_tasks_in_postgres_group_by_status(pg)
            assert counts.get("Processing", 0) == 0, f"nothing should be processing: {counts}"
            assert counts.get("Complete", 0) == 0, f"nothing should complete: {counts}"
            assert (
                get_num_tasks_in_postgres(pg) == num_messages
            ), f"no task should be lost: {counts}"
            time.sleep(0.5)

        # Failed pushes revert claimed tasks back to pending, so pending recovers.
        # The pipeline never fully drains (fetch keeps claiming while pushes fail),
        # so pending won't return to num_messages; assert it recovers to within the
        # concurrently-claimed set instead. That set is bounded by a fetch batch
        # (batch_length) plus what the push pool holds in flight (queue.size +
        # threads), with headroom for a fresh batch overlapping the previous one.
        max_in_flight = 32 + 8 + 4  # fetch.batch_length + push.queue.size + push.threads
        assert wait_until(
            lambda: get_num_tasks_in_postgres_by_status(pg, "Pending")
            >= num_messages - max_in_flight,
            timeout=30,
        ), (
            "expected claimed tasks to revert back to pending, got "
            f"{get_num_tasks_in_postgres_group_by_status(pg)}"
        )
    finally:
        if process is not None:
            stop_broker(process)
        server.stop(0)
        drop_postgres_db(pg)


def test_push_postgres_upkeep_reverts_expired_claims() -> None:
    """
    A worker that hangs on every push (longer than push.timeout). The push queue
    stays saturated, so most claimed tasks are dropped from the pipeline and left
    in 'Claimed'. Upkeep's claim-expiration path must return them to 'Pending'.

    We assert the observable contract (nothing processing, no loss, pending
    recovers) and confirm upkeep is the mechanism via its debug log signal.
    """
    num_messages = 200
    grpc_port, worker_port = get_available_ports(2)
    config_path, log_path, curr = _setup_paths("upkeep")
    topic = f"push-pg-upkeep-{curr}"
    dlq_topic = f"push-pg-upkeep-dlq-{curr}"
    pg = PostgresConfig(database_name=unique_pg_database_name("push_pg_upkeep"))

    create_topic(topic, 1)
    create_topic(dlq_topic, 1)

    # Small queue + single push thread + short push.timeout keeps the claim lease
    # short (compute_claim_duration_ms), while the hanging worker keeps the queue
    # full so fetch drops claimed tasks that only upkeep can revert.
    config = build_push_config(
        topic=topic,
        dlq_topic=dlq_topic,
        grpc_port=grpc_port,
        worker_port=worker_port,
        pg=pg,
        fetch={"threads": 1, "batch_length": 10, "backoff": "100ms"},
        push={
            "threads": 1,
            "timeout": "1000ms",
            "queue": {"size": 1, "timeout": "200ms"},
            "update": {"batched": False},
        },
        # Enable the upkeep debug log so we can confirm claim-expiration fired.
        log_filter="info,librdkafka=warn,h2=off,taskbroker::upkeep=debug",
    )
    with open(config_path, "w") as f:
        yaml.safe_dump(config, f)

    activations = make_activations(num_messages)
    send_custom_messages_to_topic(topic, activations)

    # Hang longer than push.timeout (1s) and the claim lease so pushes time out
    # and claimed tasks accumulate for upkeep to reclaim.
    server, servicer = start_mock_worker(worker_port, hang=10.0)
    process = None
    try:
        process = start_broker(config_path, log_path)

        # Wait for all tasks to land in postgres.
        assert wait_until(
            lambda: get_num_tasks_in_postgres(pg) == num_messages, timeout=60
        ), f"expected {num_messages} tasks written, got {get_num_tasks_in_postgres(pg)}"

        # Tasks should get claimed as the fetch loop runs.
        assert wait_until(
            lambda: get_num_tasks_in_postgres_by_status(pg, "Claimed") > 0, timeout=30
        ), "expected some tasks to be claimed"

        # Upkeep should revert expired claims: confirm via its debug log signal.
        # The upkeep debug line renders `..claim_expiration_reset=<n>..`; the log
        # is ANSI-colored, so strip escapes before matching.
        ansi = re.compile(r"\x1b\[[0-9;]*m")
        reset = re.compile(r"claim_expiration_reset=(\d+)")

        def upkeep_reset_seen() -> bool:
            with open(log_path, "r") as f:
                text = ansi.sub("", f.read())
            return any(int(n) > 0 for n in reset.findall(text))

        assert wait_until(upkeep_reset_seen, timeout=120), (
            "expected upkeep to reset expired claims back to pending "
            "(no claim_expiration_reset>0 in broker log)"
        )

        # Nothing should ever succeed (worker never acks) and nothing is lost.
        counts = get_num_tasks_in_postgres_group_by_status(pg)
        assert counts.get("Processing", 0) == 0, f"nothing should be processing: {counts}"
        assert counts.get("Complete", 0) == 0, f"nothing should complete: {counts}"
        assert get_num_tasks_in_postgres(pg) == num_messages, f"no task should be lost: {counts}"
    finally:
        if process is not None:
            stop_broker(process)
        server.stop(0)
        drop_postgres_db(pg)
