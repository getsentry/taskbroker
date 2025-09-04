import random
import signal
import subprocess
import threading
import time
from datetime import datetime
from uuid import uuid4

import orjson
import yaml
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.taskbroker.v1.taskbroker_pb2 import TaskActivation

from python.integration_tests.helpers import (
    TASKBROKER_BIN,
    TESTS_OUTPUT_ROOT,
    TaskbrokerConfig,
    create_topic,
    get_num_tasks_group_by_status,
    get_num_tasks_in_sqlite,
    send_custom_messages_to_topic,
)

TEST_OUTPUT_PATH = TESTS_OUTPUT_ROOT / "test_upkeep_delay"


def generate_task_activation(delay: int) -> TaskActivation:
    return TaskActivation(
        id=uuid4().hex,
        namespace="integration_tests",
        taskname="integration_tests.say_hello",
        parameters=orjson.dumps({"args": ["foobar"], "kwargs": {}}),
        retry_state=None,
        processing_deadline_duration=3000,
        received_at=Timestamp(seconds=int(time.time())),
        expires=None,
        delay=delay,
    )


def manage_taskbroker(
    taskbroker_path: str,
    config_file_path: str,
    taskbroker_config: TaskbrokerConfig,
    log_file_path: str,
    results_log_path: str,
    timeout: int,
    num_messages: int,
    end_of_delay: int,
) -> None:
    with open(log_file_path, "a") as log_file:
        print(f"[taskbroker_0] Starting taskbroker, writing log file to " f"{log_file_path}")
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
                    f"[taskbroker_0]: Finishing writting all {num_messages} " "task(s) to sqlite."
                )
                finished_writing_tasks = True
            time.sleep(1)

        if not finished_writing_tasks:
            print(
                "[taskbroker_0]: Taskbroker was unable to write all tasks to "
                "sqlite within timeout."
            )

        # Keep running taskbroker until:
        # - timeout is reached
        # - all tasks have been moved to pending state
        print("[taskbroker_0]: Waiting for upkeep to move delayed tasks to " "pending state")
        cur_time = time.time()
        while (cur_time < end) and finished_writing_tasks:
            task_count_in_sqlite = get_num_tasks_group_by_status(taskbroker_config)

            complete = False
            delay_count = task_count_in_sqlite.get("Delay", 0)
            if delay_count == 0:
                if cur_time < end_of_delay:
                    print(
                        f"[taskbroker_0]: Current time: "
                        f"{datetime.fromtimestamp(cur_time)}. "
                        "All delayed tasks have been moved to pending state "
                        f"before countdown elapsed at "
                        f"{datetime.fromtimestamp(end_of_delay)}."
                    )
                    break
                else:
                    print(
                        f"[taskbroker_0]: Current time: "
                        f"{datetime.fromtimestamp(cur_time)}. "
                        "All delayed tasks have been moved to pending state "
                        f"after countdown has elapsed at "
                        f"{datetime.fromtimestamp(end_of_delay)}."
                    )
                    complete = True
            print(
                f"[taskbroker_0]: Waiting for upkeep to move all delayed "
                "tasks to pending state. "
                f"Sqlite count by status: {task_count_in_sqlite}"
            )
            if complete:
                print(
                    "[taskbroker_0]: Upkeep has successfully move all delayed "
                    "tasks to pending state"
                )
                break

            time.sleep(3)
            cur_time = time.time()

        if cur_time >= end:
            print(
                "[taskbroker_0]: Taskbroker (upkeep) did not finish "
                "discarding/deadlettering all tasks before timeout. "
                "Shutting down taskbroker."
            )

        pending_count = task_count_in_sqlite.get("Pending", 0)
        with open(results_log_path, "a") as results_log_file:
            results_log_file.write(
                f"total_delayed_tasks:{delay_count},"
                f"total_pending_tasks:{pending_count},"
                f"delay_has_elapsed:{int(cur_time >= end_of_delay)}"
            )

        # Stop the taskbroker
        print("[taskbroker_0]: Shutting down taskbroker")
        process.send_signal(signal.SIGINT)
        try:
            return_code = process.wait(timeout=10)
            assert return_code == 0
        except Exception:
            process.kill()


def test_upkeep_delay() -> None:
    """
    What does this test do?
    This test is responsible for checking the integrity of the countdown
    mechanism responsible for delaying tasks to be executed at a later time.
    This functionality is made possible by the the upkeep thread of taskbroker
    which is responsible for shifting tasks from the delay state to the
    pending state after the countdown has elapsed. An initial amount of
    messages is produced to kafka with a set delay time. These messages are
    first written into sqlite with a delay state. During an interval, the
    upkeep thread periodically checks whether a delayed tasks can be updated.
    This process continues until all tasks have been shifted to a pending
    state.

    How does it accomplish this?
    The test starts 1 broker thread. Once the upkeep thread
    has successfully shifted all delayed tasks in sqlite, taskbroker shuts
    down. Finally, this total number of delayed tasks and whether the delay
    has elapsed is validated.

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
    Upkeep thread collects delayed tasks and shifts them to pending
             .
             .
             |
    When it finishes or timeout is elapsed,
    Stop taskbroker
    """

    # Test configuration
    taskbroker_path = str(TASKBROKER_BIN)
    num_messages = 5_000
    num_partitions = 4
    max_pending_count = 100_000
    taskbroker_timeout = 600
    topic_name = "taskworker"
    dlq_topic_name = "taskworker-dlq"
    curr_time = int(time.time())
    min_delay = 20
    max_delay = 40

    print(
        f"""
Running test with the following configuration:
        num of messages: {num_messages},
        num of partitions: {num_partitions},
        max pending count: {max_pending_count},
        topic name: {topic_name},
        dlq topic name: {dlq_topic_name},
        delay range: {min_delay} - {max_delay}
    """
    )

    create_topic(topic_name, num_partitions)

    # Create config file for taskbroker
    print("Creating config file for taskbroker")
    TEST_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    db_name = f"db_0_{curr_time}_test_upkeep_delay"
    config_filename = "config_0_test_upkeep_delay.yml"
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
        for _ in range(num_messages):
            task_activation = generate_task_activation(random.randint(min_delay, max_delay))
            custom_messages.append(task_activation)

        send_custom_messages_to_topic(topic_name, custom_messages)

        first_message_received_at = custom_messages[0].received_at.ToSeconds()
        end_of_delay = first_message_received_at + max_delay

        # Create taskbroker thread
        results_log_path = str(
            TEST_OUTPUT_PATH / f"taskbroker_0_{curr_time}_test_upkeep_delay_results.log"
        )
        taskbroker_thread = threading.Thread(
            target=manage_taskbroker,
            args=(
                taskbroker_path,
                str(TEST_OUTPUT_PATH / config_filename),
                taskbroker_config,
                str(TEST_OUTPUT_PATH / f"taskbroker_0_{curr_time}_test_upkeep_delay.log"),
                results_log_path,
                taskbroker_timeout,
                num_messages,
                end_of_delay,
            ),
        )
        taskbroker_thread.start()
        taskbroker_thread.join()
    except Exception as e:
        raise Exception(f"Error running taskbroker: {e}")

    with open(results_log_path, "r") as log_file:
        line = log_file.readline()
        total_delayed_tasks = int(line.split(",")[0].split(":")[1])
        total_pending_tasks = int(line.split(",")[1].split(":")[1])
        delay_has_elapsed = int(line.split(",")[2].split(":")[1])

    assert total_delayed_tasks == 0  # there should no delayed tasks in sqlite
    assert total_pending_tasks == num_messages  # all tasks should have been moved to pending state
    assert delay_has_elapsed == 1  # delay should have elapsed
