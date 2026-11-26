import signal
import sqlite3
import subprocess
import time
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
    TaskbrokerConfig,
    create_topic,
    get_available_ports,
    get_num_tasks_in_sqlite,
    send_custom_messages_to_topic,
)
from integration_tests.worker import TaskWorkerClient

TEST_OUTPUT_PATH = TESTS_OUTPUT_ROOT / "test_heartbeat"

# Short enough to keep the test fast. The client enforces a 10s minimum at task
# registration, but the broker accepts any value.
HEARTBEAT_SEC = 2


def heartbeat_activation() -> TaskActivation:
    return TaskActivation(
        id=uuid4().hex,
        namespace="integration_tests",
        taskname="integration_tests.say_hello",
        parameters=orjson.dumps({"args": [], "kwargs": {}}).decode("utf-8"),
        retry_state=RetryState(
            attempts=0,
            max_attempts=3,
            on_attempts_exceeded=OnAttemptsExceeded.ON_ATTEMPTS_EXCEEDED_DISCARD,
        ),
        processing_deadline_duration=3000,
        received_at=Timestamp(seconds=int(time.time())),
        heartbeat=HEARTBEAT_SEC,
    )


def task_state(config: TaskbrokerConfig, task_id: str) -> tuple[str, int]:
    con = sqlite3.connect(config.db_path)
    try:
        status, attempts = con.execute(
            "SELECT status, processing_attempts FROM inflight_taskactivations WHERE id = ?",
            (task_id,),
        ).fetchone()
    finally:
        con.close()
    return status, attempts


def test_heartbeat() -> None:
    """
    A pull worker fetches two tasks that have a heartbeat. It renews one of them with
    `BatchUpdateHeartbeat` and lets the other one lapse. The broker
    should keep the renewed task processing and reset the lapsed one to pending, and
    report the lapsed one in `not_found_ids` from then on.
    """
    curr_time = int(time.time())
    topic_name = f"taskworker-heartbeat-{curr_time}"
    (grpc_port,) = get_available_ports(1)
    host = f"127.0.0.1:{grpc_port}"

    create_topic(topic_name, 1)
    TEST_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    db_name = f"db_0_{curr_time}_test_heartbeat"
    config = TaskbrokerConfig(
        db_name=db_name,
        db_path=str(TEST_OUTPUT_PATH / f"{db_name}.sqlite"),
        max_pending_count=1000,
        kafka_topic=topic_name,
        kafka_deadletter_topic=f"{topic_name}-dlq",
        kafka_consumer_group=topic_name,
        kafka_auto_offset_reset="earliest",
        grpc_port=grpc_port,
    )
    config_path = TEST_OUTPUT_PATH / "config_0_test_heartbeat.yml"
    with open(config_path, "w") as f:
        # Upkeep skips deadline resets right after startup by default.
        yaml.safe_dump({**config.to_dict(), "upkeep_deadline_reset_skip_after_startup_sec": 0}, f)

    send_custom_messages_to_topic(topic_name, [heartbeat_activation(), heartbeat_activation()])

    log_path = TEST_OUTPUT_PATH / f"taskbroker_0_{curr_time}_test_heartbeat.log"
    with open(log_path, "a") as log_file:
        process = subprocess.Popen(
            [str(TASKBROKER_BIN), "-c", str(config_path)],
            stderr=subprocess.STDOUT,
            stdout=log_file,
        )
    try:
        time.sleep(3)  # Let the broker create the sqlite file before we open it.
        end = time.time() + 60
        while time.time() < end:
            if get_num_tasks_in_sqlite(config) == 2:
                break
            time.sleep(1)
        else:
            raise AssertionError("Broker did not write both tasks to sqlite")

        worker = TaskWorkerClient(host)
        renewed = worker.get_task()
        lapsed = worker.get_task()
        assert renewed is not None and lapsed is not None

        # Renew one task each second (like the worker's heartbeat process) for a few
        # heartbeat periods, which is long enough for upkeep to reset the other one.
        end = time.time() + HEARTBEAT_SEC * 4
        while time.time() < end:
            assert worker.update_heartbeats([renewed.id]) == []
            time.sleep(1)

        assert task_state(config, renewed.id) == ("Processing", 0)
        assert task_state(config, lapsed.id) == ("Pending", 1)
        assert worker.update_heartbeats([renewed.id, lapsed.id]) == [lapsed.id]
    finally:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
