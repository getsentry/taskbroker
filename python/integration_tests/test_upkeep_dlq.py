import orjson
import sqlite3
import signal
import subprocess
import threading
import time

import yaml
from uuid import uuid4
from google.protobuf.timestamp_pb2 import Timestamp
from python.integration_tests.helpers import (
    TASKBROKER_BIN,
    TESTS_OUTPUT_ROOT,
    send_messages_to_kafka,
    create_topic,
    check_num_tasks_written,
    ConsumerConfig,
)
from sentry_protos.taskbroker.v1.taskbroker_pb2 import (
    OnAttemptsExceeded,
    RetryState,
    TaskActivation,
)


TEST_OUTPUT_PATH = TESTS_OUTPUT_ROOT / "test_upkeep_dlq"


def manage_consumer(
    consumer_path: str,
    config_file_path: str,
    consumer_config: ConsumerConfig,
    log_file_path: str,
    timeout: int,
    num_messages: int,
) -> None:
    with open(log_file_path, "a") as log_file:
        print(f"[consumer_0] Starting consumer, writing log file to {log_file_path}")
        process = subprocess.Popen(
            [consumer_path, "-c", config_file_path],
            stderr=subprocess.STDOUT,
            stdout=log_file,
        )
        time.sleep(3)

        # Keep gRPC consumer alive until taskworker is done processing
        print("[consumer_0]: Waiting for upkeep to discard/deadletter tasks")
        while True:
            attach_db_stmt = f"ATTACH DATABASE '{consumer_config.db_path}' AS {consumer_config.db_name};\n"
            query = f"""SELECT * FROM {consumer_config.db_name}.inflight_taskactivations;"""
            con = sqlite3.connect(consumer_config.db_path)
            cur = con.cursor()
            cur.executescript(attach_db_stmt)
            rows = cur.execute(query).fetchall()
            print(rows)
            time.sleep(3)
        print("[consumer_0]: Received shutdown signal from all taskworker(s)")

        # Stop the consumer
        print("[consumer_0]: Shutting down consumer")
        process.send_signal(signal.SIGINT)
        try:
            return_code = process.wait(timeout=10)
            assert return_code == 0
        except Exception:
            process.kill()


def test_upkeep_dlq() -> None:
    """
    Testing DLQ and discard when remove_at elapses.
    - Produce N messages to kafka where the tasks retry policy max_attempts is 5 (does not matter) and on_attempts_exceeded is set to deadletter and expires to 0.

    Testing DLQ and discard when max_attempts is reached.
    - Produce N messages to kafka where the tasks retry policy max_attempts is 3 and set to deadletter
    - Expect the tasks to try 3 times, then be deadlettered
    __________________________
    What does this test do?

    How does it accomplish this?


    Sequence diagram:
 
    """

    # Test configuration
    consumer_path = str(TASKBROKER_BIN)
    num_messages = 5000
    num_partitions = 4
    max_pending_count = 100_000
    consumer_timeout = (
        60  # the time in seconds to wait for all messages to be written to sqlite
    )
    topic_name = "task-worker"
    curr_time = int(time.time())

    print(
        f"""
Running test with the following configuration:
        num of messages: {num_messages},
        num of partitions: {num_partitions},
        max pending count: {max_pending_count},
        topic name: {topic_name}
    """
    )

    create_topic(topic_name, num_partitions)

    # Create config file for consumer
    print("Creating config file for consumer")
    TEST_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    db_name = f"db_0_{curr_time}_test_upkeep_dlq"
    config_filename = "config_0_test_upkeep_dlq.yml"
    consumer_config = ConsumerConfig(
        db_name,
        str(TEST_OUTPUT_PATH / f"{db_name}.sqlite"),
        max_pending_count,
        topic_name,
        topic_name,
        "earliest",
        50051
    )

    with open(str(TEST_OUTPUT_PATH / config_filename), "w") as f:
        yaml.safe_dump(consumer_config.to_dict(), f)

    try:
        # Produce N messages to kafka that expires immediately
        retry_state = RetryState(
            attempts=0,
            max_attempts=1,
            on_attempts_exceeded=OnAttemptsExceeded.ON_ATTEMPTS_EXCEEDED_DISCARD,
        )
        custom_task_activation = TaskActivation(
            id=uuid4().hex,
            namespace="integration_tests",
            taskname="integration_tests.say_hello",
            parameters=orjson.dumps({"args": ["foobar"], "kwargs": {}}),
            retry_state=retry_state,
            processing_deadline_duration=3000,
            received_at=Timestamp(seconds=int(time.time())),
            expires=0,
        )
        send_messages_to_kafka(topic_name, num_messages, custom_task_activation)
        consumer_thread = threading.Thread(
            target=manage_consumer,
            args=(
                consumer_path,
                str(TEST_OUTPUT_PATH / config_filename),
                consumer_config,
                str(
                    TEST_OUTPUT_PATH
                    / f"consumer_0_{curr_time}_test_upkeep_dlq.log"
                ),
                consumer_timeout,
                num_messages,
            ),
        )
        consumer_thread.start()
        consumer_thread.join()
    except Exception as e:
        raise Exception(f"Error running taskbroker: {e}")
