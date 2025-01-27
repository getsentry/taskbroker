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
    send_custom_messages_to_topic,
    create_topic,
    get_num_tasks_in_sqlite,
    get_num_tasks_in_sqlite_by_status,
    ConsumerConfig,
)
from python.integration_tests.worker import TaskWorkerClient
from sentry_protos.taskbroker.v1.taskbroker_pb2 import (
    OnAttemptsExceeded,
    RetryState,
    TaskActivation,
    TASK_ACTIVATION_STATUS_COMPLETE
)


TEST_OUTPUT_PATH = TESTS_OUTPUT_ROOT / "test_upkeep_dlq"


def manage_taskworker(
    consumer_config: ConsumerConfig,
    tasks_written_event: threading.Event,
    shutdown_event: threading.Event,
    last_task_id: str,
) -> None:
    """
    A special task worker that only fetches one most recent task (not the oldest) and completes it
    """
    worker_id = 0
    print(f"[taskworker_{worker_id}] Starting taskworker_{worker_id}")
    client = TaskWorkerClient(f"127.0.0.1:{consumer_config.grpc_port}")

    # Wait for consumer to initialize sqlite and write tasks to it
    print(
        f"[taskworker_{worker_id}]: Waiting for consumer to initialize sqlite and write tasks to it..."
    )
    while not tasks_written_event.is_set():
        time.sleep(1)

    try:
        client.update_task(
            task_id=last_task_id,
            status=TASK_ACTIVATION_STATUS_COMPLETE,
            fetch_next_task=None,
        )
        print(f"[taskworker_{worker_id}]: Completed the most recent (not the oldest) task. Upkeep will now be able to discard/deadletter all the task before it")
        print(f"[taskworker_{worker_id}]: Shutting down successfully now")
    except Exception as e:
        print(f"[taskworker_{worker_id}]: Worker process crashed: {e}")
        print(f"[taskworker_{worker_id}]: Unable to complete the most recent task. Sending shutdown signal to consumer")
        shutdown_event.set()
        return


def manage_consumer(
    consumer_path: str,
    config_file_path: str,
    consumer_config: ConsumerConfig,
    log_file_path: str,
    timeout: int,
    num_messages: int,
    tasks_written_event: threading.Event,
    shutdown_event: threading.Event,
) -> None:
    with open(log_file_path, "a") as log_file:
        print(f"[consumer_0] Starting consumer, writing log file to {log_file_path}")
        process = subprocess.Popen(
            [consumer_path, "-c", config_file_path],
            stderr=subprocess.STDOUT,
            stdout=log_file,
        )
        time.sleep(3)

        # Let the consumer write the messages to sqlite
        end = time.time() + timeout
        while (time.time() < end) and (not tasks_written_event.is_set()):
            written_tasks = get_num_tasks_in_sqlite(consumer_config)
            if written_tasks == num_messages:
                print(
                    f"[consumer_0]: Finishing writting all {num_messages} task(s) to sqlite. Sending signal to taskworker to start processing"
                )
                tasks_written_event.set()
            time.sleep(1)

        print("[consumer_0]: Waiting for upkeep to discard/deadletter tasks")
        end = time.time() + timeout
        cur_time = time.time()
        num_completed_tasks = 0
        while (not shutdown_event.is_set()) and (cur_time < end) and (num_completed_tasks < num_messages):
            num_completed_tasks = get_num_tasks_in_sqlite_by_status(consumer_config, "Complete")
            print(num_completed_tasks)
            time.sleep(3)
            cur_time = time.time()
        if shutdown_event.is_set():
            print("[consumer_0]: Received shutdown signal from taskworker. Consumer was unable to complete task. Shutting down taskbroker.")

        if cur_time >= end:
            print("[consumer_0]: Taskbroker (upkeep) did not finish discarding/deadlettering tasks before timeout. Shutting down taskbroker.")

        # Stop the taskbroker
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
        60  # the time in seconds to wait for taskbroker to process
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
        custom_messages = []
        last_task_id = ""
        for i in range(num_messages):
            id = uuid4().hex

            # Capture the last task id so that we can complete it later
            if i == num_messages - 1:
                last_task_id = id

            retry_state = RetryState(
                attempts=0,
                max_attempts=1,
                on_attempts_exceeded=OnAttemptsExceeded.ON_ATTEMPTS_EXCEEDED_DISCARD,
            )
            task_activation = TaskActivation(
                id=id,
                namespace="integration_tests",
                taskname=f"integration_tests.say_hello_{i}",
                parameters=orjson.dumps({"args": ["foobar"], "kwargs": {}}),
                retry_state=retry_state,
                processing_deadline_duration=3000,
                received_at=Timestamp(seconds=int(time.time())),
                expires=1,
            )
            custom_messages.append(task_activation)

        send_custom_messages_to_topic(topic_name, custom_messages)
        tasks_written_event = threading.Event()
        shutdown_event = threading.Event()

        # Create consumer thread
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
                tasks_written_event,
                shutdown_event,
            ),
        )
        consumer_thread.start()

        # Create worker thread
        worker_thread = threading.Thread(
            target=manage_taskworker,
            args=(
                consumer_config,
                tasks_written_event,
                shutdown_event,
                last_task_id,
            ),
        )
        worker_thread.start()

        consumer_thread.join()
        worker_thread.join()
    except Exception as e:
        raise Exception(f"Error running taskbroker: {e}")
