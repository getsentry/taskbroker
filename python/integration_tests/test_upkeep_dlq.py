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
from python.integration_tests.worker import TaskWorkerClient
from sentry_protos.taskbroker.v1.taskbroker_pb2 import (
    OnAttemptsExceeded,
    RetryState,
    TaskActivation,
    TASK_ACTIVATION_STATUS_COMPLETE,
)


TEST_OUTPUT_PATH = TESTS_OUTPUT_ROOT / "test_upkeep_dlq"


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


def manage_taskworker(
    taskbroker_config: TaskbrokerConfig,
    tasks_written_event: threading.Event,
    shutdown_event: threading.Event,
    id_to_complete: str,
) -> None:
    """
    A special task worker that only fetches one most recent task
    (not the oldest) and completes it
    """
    worker_id = 0
    print(f"[taskworker_{worker_id}] Starting taskworker_{worker_id}")
    client = TaskWorkerClient(f"127.0.0.1:{taskbroker_config.grpc_port}")

    # Wait for taskbroker to initialize sqlite and write tasks to it
    print(
        f"[taskworker_{worker_id}]: Waiting for taskbroker to initialize "
        "sqlite and write tasks to it..."
    )
    while not tasks_written_event.is_set():
        time.sleep(1)

    try:
        client.update_task(
            task_id=id_to_complete,
            status=TASK_ACTIVATION_STATUS_COMPLETE,
            fetch_next_task=None,
        )
        print(
            f"[taskworker_{worker_id}]: Completed the most recent "
            "(not the oldest) task. Upkeep will now be able to "
            "discard/deadletter all the task before it"
        )
        print(f"[taskworker_{worker_id}]: Shutting down successfully now")
    except Exception as e:
        print(f"[taskworker_{worker_id}]: Worker process crashed: {e}")
        print(
            f"[taskworker_{worker_id}]: Unable to complete the most "
            f"recent task. Sending shutdown signal to taskbroker"
        )
        shutdown_event.set()
        return


def manage_taskbroker(
    taskbroker_path: str,
    config_file_path: str,
    taskbroker_config: TaskbrokerConfig,
    log_file_path: str,
    results_log_path: str,
    timeout: int,
    num_messages: int,
    tasks_written_event: threading.Event,
    shutdown_event: threading.Event,
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
        while (time.time() < end) and (not tasks_written_event.is_set()):
            written_tasks = get_num_tasks_in_sqlite(taskbroker_config)
            if written_tasks == num_messages + 2:
                print(
                    f"[taskbroker_0]: Finishing writting all {num_messages} "
                    "task(s) to sqlite. Sending signal to taskworker to "
                    "start processing"
                )
                tasks_written_event.set()
            time.sleep(1)

        print("[taskbroker_0]: Waiting for upkeep to discard/deadletter tasks")
        end = time.time() + timeout
        cur_time = time.time()
        while (not shutdown_event.is_set()) and (cur_time < end):
            task_count_in_sqlite = get_num_tasks_group_by_status(taskbroker_config)
            print(
                "[taskbroker_0]: Current state tasks in sqlite: "
                f"{task_count_in_sqlite}"
            )

            # Break successfully if there is only one pending task
            # left in sqlite
            if task_count_in_sqlite["Pending"] == 1:
                complete = True
                for status, count in task_count_in_sqlite.items():
                    if status != "Pending" and count != 0:
                        complete = False
                if complete:
                    print(
                        "[taskbroker_0]: Upkeep has completed and removed "
                        "all tasks except the last buffer task."
                    )
                    break

            time.sleep(3)
            cur_time = time.time()

        if shutdown_event.is_set():
            print(
                "[taskbroker_0]: Received shutdown signal from taskworker. "
                "Taskbroker was unable to complete task. "
                "Shutting down taskbroker."
            )

        if cur_time >= end:
            print(
                "[taskbroker_0]: Taskbroker (upkeep) did not finish "
                "discarding/deadlettering all tasks before timeout. "
                "Shutting down taskbroker."
            )
        total = sum([count for count in task_count_in_sqlite.values()])

        with open(results_log_path, "a") as results_log_file:
            results_log_file.write(f"total:{total}")

        # Stop the taskbroker
        print("[taskbroker_0]: Shutting down taskbroker")
        process.send_signal(signal.SIGINT)
        try:
            return_code = process.wait(timeout=10)
            assert return_code == 0
        except Exception:
            process.kill()


def test_upkeep_dlq() -> None:
    """
    What does this test do?
    This tests is responsible for checking the integrity of the discard
    and deadletter mechanism implemented in the upkeep thread of taskbroker.
    An initial amount of messages is produced to kafka where half of the
    messages' on_attempts_exceeded is set to discard and the other half
    to deadletter. These messages have an `expires` value of 1 second.
    An extra message is produced to the topic which is to be completed
    by a taskworker first. This allows all the previous messages to be
    discarded/deadlettered by upkeep. During an interval, the upkeep thread
    collect all tasks that have expired, sets them all to a failed status,
    then appropriately discards or deadletters these messages. This process
    continues until all tasks have have a completed status (this means
    all tasks have either been discarded or deadlettered).

    How does it accomplish this?
    The test starts a taskworker and a taskbroker in separate
    threads. Synchronization events are use to instruct the taskworker
    when complete the last message and shutdown. Once the upkeep thread
    has successfully completed all messages in sqlite, taskbroker shuts down.
    Finally, this total number of completed messages and messages in the DLQ
    topic is validated.

    Sequence diagram:
    [Thread 1: Taskbroker]                                      [Thread 2: Taskworker]
             |                                                              |
             |                                                              |
    Start taskbroker                                                Start taskworker
             |                                                              |
             |                                                              |
    Consume kafka and write to sqlite                                       |
             .                                                              |
             .                                                              |
    Done initializing and writing to sqlite ---------[Send signal]--------->|
             |                                                              |
             |                                                     Complete last message
             |                                                              .
             |                                                        Stop taskworker
             |                                                              |
             |                                                              |
    Upkeep thread collects expired tasks and discards/deadletters           |
             .                                                              |
             .                                                              |
             .                                                              |
             .                                                              |
             .                                                              |
             |                                                              |
    When it finishes or timeout is elapsed,                                 |
    Stop taskbroker                                                         |
    """

    # Test configuration
    taskbroker_path = str(TASKBROKER_BIN)
    num_messages = 10_000
    num_partitions = 4
    max_pending_count = 100_000
    taskbroker_timeout = 120  # the time in seconds to wait for taskbroker to process
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
    db_name = f"db_0_{curr_time}_test_upkeep_dlq"
    config_filename = "config_0_test_upkeep_dlq.yml"
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
        # Produce num_messages + 2 to kafka.
        # The first num_messages messages are produced with an expiration
        # of 1 second. The last two messages are produced with an expiration
        # of 2400 seconds. The second last message is the one that will be
        # completed by taskworker such that all messages can be
        # discarded/deadlettered. The last message will remain in a pending
        # state which will allow upkeep to cleanup and remove all
        # previous completed messages from sqlite.

        custom_messages = []
        for i in range(num_messages):
            if i % 2 == 0:
                task_activation = generate_task_activation(
                    OnAttemptsExceeded.ON_ATTEMPTS_EXCEEDED_DISCARD, 1
                )
            else:
                task_activation = generate_task_activation(
                    OnAttemptsExceeded.ON_ATTEMPTS_EXCEEDED_DEADLETTER, 1
                )
            custom_messages.append(task_activation)

        # Produce second last message to be completed by taskworker
        task_activation = generate_task_activation(
            OnAttemptsExceeded.ON_ATTEMPTS_EXCEEDED_DISCARD, 2400
        )
        id_to_complete = task_activation.id
        custom_messages.append(task_activation)

        # Produce last buffer message
        task_activation = generate_task_activation(
            OnAttemptsExceeded.ON_ATTEMPTS_EXCEEDED_DISCARD, 2400
        )
        custom_messages.append(task_activation)

        send_custom_messages_to_topic(topic_name, custom_messages)
        tasks_written_event = threading.Event()
        shutdown_event = threading.Event()

        # Create taskbroker thread
        results_log_path = str(
            TEST_OUTPUT_PATH / f"taskbroker_0_{curr_time}_test_upkeep_dlq_results.log"
        )
        taskbroker_thread = threading.Thread(
            target=manage_taskbroker,
            args=(
                taskbroker_path,
                str(TEST_OUTPUT_PATH / config_filename),
                taskbroker_config,
                str(TEST_OUTPUT_PATH / f"taskbroker_0_{curr_time}_test_upkeep_dlq.log"),
                results_log_path,
                taskbroker_timeout,
                num_messages,
                tasks_written_event,
                shutdown_event,
            ),
        )
        taskbroker_thread.start()

        # Create worker thread
        worker_thread = threading.Thread(
            target=manage_taskworker,
            args=(
                taskbroker_config,
                tasks_written_event,
                shutdown_event,
                id_to_complete,
            ),
        )
        worker_thread.start()

        taskbroker_thread.join()
        worker_thread.join()
    except Exception as e:
        raise Exception(f"Error running taskbroker: {e}")

    total_count = 0

    with open(results_log_path, "r") as log_file:
        line = log_file.readline()
        total_count = int(line.split(":")[1])

    dlq_size = get_topic_size(dlq_topic_name)

    assert (
        total_count == 1
    )  # there should only be one task left in sqlite since all tasks before the last buffer task were discarded or deadlettered
    assert dlq_size == (num_messages) / 2  # half of the tasks should be deadlettered
