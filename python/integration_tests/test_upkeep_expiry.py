import orjson
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
    get_num_tasks_group_by_status,
    TaskbrokerConfig,
    get_topic_size,
)

from sentry_protos.taskbroker.v1.taskbroker_pb2 import (
    OnAttemptsExceeded,
    RetryState,
    TaskActivation,
)


TEST_OUTPUT_PATH = TESTS_OUTPUT_ROOT / "test_upkeep_expiry"


def generate_task_activation(
    on_attempts_exceeded: OnAttemptsExceeded, expires: int
) -> TaskActivation:
    retry_state = RetryState(
        attempts=0,
        max_attempts=1,
        on_attempts_exceeded=on_attempts_exceeded,
    )
    return TaskActivation(
        id=uuid4().hex,
        namespace="integration_tests",
        taskname="integration_tests.say_hello",
        parameters=orjson.dumps({"args": ["foobar"], "kwargs": {}}),
        retry_state=retry_state,
        processing_deadline_duration=3000,
        received_at=Timestamp(seconds=int(time.time())),
        expires=expires,
    )


def manage_taskbroker(
    taskbroker_path: str,
    config_file_path: str,
    taskbroker_config: TaskbrokerConfig,
    log_file_path: str,
    results_log_path: str,
    timeout: int,
    num_messages: int,
) -> None:
    with open(log_file_path, "a") as log_file:
        print(
            f"[taskbroker_0] Starting taskbroker, writing log file to "
            f"{log_file_path}"
        )
        process = subprocess.Popen(
            [taskbroker_path, "-c", config_file_path],
            stderr=subprocess.STDOUT,
            stdout=log_file,
        )
        time.sleep(3)

        # Let the taskbroker write the messages to sqlite
        end = time.time() + timeout
        finished_writing_tasks = False
        while (time.time() < end) and (not finished_writing_tasks):
            written_tasks = get_num_tasks_in_sqlite(taskbroker_config)
            print(f"[taskbroker_0]: Written {written_tasks} tasks to sqlite.")
            if written_tasks == num_messages:
                print(
                    f"[taskbroker_0]: Finishing writting all {num_messages} "
                    "task(s) to sqlite."
                )
                finished_writing_tasks = True
            time.sleep(1)

        if not finished_writing_tasks:
            print(
                "[taskbroker_0]: Taskbroker was unable to write all tasks to "
                "sqlite within timeout."
            )

        print("[taskbroker_0]: Waiting for upkeep to discard/deadletter tasks")
        cur_time = time.time()
        while (cur_time < end) and finished_writing_tasks:
            task_count_in_sqlite = get_num_tasks_group_by_status(taskbroker_config)

            # Break successfully there are either no tasks in sqlite or
            # all tasks are completed
            complete = True
            for status, count in task_count_in_sqlite.items():
                if status != "Complete" and count != 0:
                    complete = False
            if complete:
                print("[taskbroker_0]: Upkeep has completed all tasks.")
                break
            print(
                f"[taskbroker_0]: Waiting for upkeep to complete all tasks. "
                f"Sqlite count: {task_count_in_sqlite}"
            )

            time.sleep(3)
            cur_time = time.time()

        if cur_time >= end:
            print(
                "[taskbroker_0]: Taskbroker (upkeep) did not finish "
                "discarding/deadlettering all tasks before timeout. "
                "Shutting down taskbroker."
            )
        total_hanging_tasks = sum(
            [
                count
                for status, count in task_count_in_sqlite.items()
                if status != "Complete"
            ]
        )

        with open(results_log_path, "a") as results_log_file:
            results_log_file.write(f"total_hanging_tasks:{total_hanging_tasks}")

        time.sleep(5)  # Give some extra time for tasks to flush to DLQ topic

        # Stop the taskbroker
        print("[taskbroker_0]: Shutting down taskbroker")
        process.send_signal(signal.SIGINT)
        try:
            return_code = process.wait(timeout=10)
            assert return_code == 0
        except Exception:
            process.kill()


def test_upkeep_expiry() -> None:
    """
    What does this test do?
    This tests is responsible for checking the integrity of the expires_at
    mechanism responsible for discarding/deadlettering tasks. This
    functionality is executed in the upkeep thread of taskbroker.
    An initial amount of messages is produced to kafka where half of the
    messages' on_attempts_exceeded is set to discard and the other half
    to deadletter. These messages have an `expires_at` value of 3 second.
    During an interval, the upkeep thread collect all tasks that have expired,
    sets them all to a failed status, then appropriately discards or
    deadletters these messages. This process continues until all tasks have
    have a completed status (this means all tasks have either been discarded
    or deadlettered).

    How does it accomplish this?
    The test starts 1 taskworker thread. Once the upkeep thread
    has successfully completed all messages in sqlite, taskbroker shuts down.
    Finally, this total number of completed messages and messages in the DLQ
    topic is validated.

    Sequence diagram:
    [Thread 1: Taskbroker]
             |
             |
    Start taskbroker
             |
             |
    Consume kafka and write to sqlite
             .
             .
    Done initializing and writing to sqlite
             |
             |
             |
    Upkeep thread collects expired tasks and discards/deadletters
             .
             .
             |
    When it finishes or timeout is elapsed,
    Stop taskbroker
    """

    # Test configuration
    taskbroker_path = str(TASKBROKER_BIN)
    num_messages = 10_000
    num_partitions = 4
    max_pending_count = 100_000
    taskbroker_timeout = 600  # the time in seconds to wait for taskbroker to process
    topic_name = "task-worker"
    dlq_topic_name = "task-worker-dlq"
    curr_time = int(time.time())

    print(
        f"""
Running test with the following configuration:
        num of messages: {num_messages},
        num of partitions: {num_partitions},
        max pending count: {max_pending_count},
        topic name: {topic_name},
        dlq topic name: {dlq_topic_name}
    """
    )

    create_topic(topic_name, num_partitions)
    create_topic(dlq_topic_name, 1)

    # Create config file for taskbroker
    print("Creating config file for taskbroker")
    TEST_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    db_name = f"db_0_{curr_time}_test_upkeep_expiry"
    config_filename = "config_0_test_upkeep_expiry.yml"
    taskbroker_config = TaskbrokerConfig(
        db_name=db_name,
        db_path=str(TEST_OUTPUT_PATH / f"{db_name}.sqlite"),
        max_pending_count=max_pending_count,
        kafka_topic=topic_name,
        kafka_deadletter_topic=dlq_topic_name,
        kafka_consumer_group=topic_name,
        kafka_auto_offset_reset="earliest",
        grpc_port=50051,
    )

    with open(str(TEST_OUTPUT_PATH / config_filename), "w") as f:
        yaml.safe_dump(taskbroker_config.to_dict(), f)

    try:
        custom_messages = []
        for i in range(num_messages):
            if i % 2 == 0:
                task_activation = generate_task_activation(
                    OnAttemptsExceeded.ON_ATTEMPTS_EXCEEDED_DISCARD, 15
                )
            else:
                task_activation = generate_task_activation(
                    OnAttemptsExceeded.ON_ATTEMPTS_EXCEEDED_DEADLETTER, 15
                )
            custom_messages.append(task_activation)

        send_custom_messages_to_topic(topic_name, custom_messages)

        # Create taskbroker thread
        results_log_path = str(
            TEST_OUTPUT_PATH
            / f"taskbroker_0_{curr_time}_test_upkeep_expiry_results.log"
        )
        taskbroker_thread = threading.Thread(
            target=manage_taskbroker,
            args=(
                taskbroker_path,
                str(TEST_OUTPUT_PATH / config_filename),
                taskbroker_config,
                str(
                    TEST_OUTPUT_PATH
                    / f"taskbroker_0_{curr_time}_test_upkeep_expiry.log"
                ),
                results_log_path,
                taskbroker_timeout,
                num_messages,
            ),
        )
        taskbroker_thread.start()
        taskbroker_thread.join()
    except Exception as e:
        raise Exception(f"Error running taskbroker: {e}")

    total_hanging_tasks = 0

    with open(results_log_path, "r") as log_file:
        line = log_file.readline()
        total_hanging_tasks = int(line.split(":")[1])

    dlq_size = get_topic_size(dlq_topic_name)

    assert (
        total_hanging_tasks == 0
    )  # there should no tasks in sqlite that are not completed or removed
    assert dlq_size == (num_messages) / 2  # half of the tasks should be deadlettered
