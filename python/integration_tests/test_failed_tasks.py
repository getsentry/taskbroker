import orjson
import pytest
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
    create_topic,
    get_num_tasks_in_sqlite,
    TaskbrokerConfig,
    send_custom_messages_to_topic,
    get_topic_size,
)

from python.integration_tests.worker import (
    ConfigurableTaskWorker,
    TaskWorkerClient,
)
from sentry_protos.taskbroker.v1.taskbroker_pb2 import (
    OnAttemptsExceeded,
    RetryState,
    TaskActivation,
)


TEST_OUTPUT_PATH = TESTS_OUTPUT_ROOT / "test_failed_tasks"


def generate_task_activation(
    on_attempts_exceeded: OnAttemptsExceeded
) -> TaskActivation:
    """Generate a task activation with the specified retry behavior."""
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
        expires=None,
    )


def manage_taskworker(
    taskbroker_config: TaskbrokerConfig,
    taskworker_results_log: str,
    tasks_written_event: threading.Event,
    shutdown_event: threading.Event,
) -> None:
    """Manage the taskworker process lifecycle.

    This function:
    1. Initializes a taskworker that will fail all tasks
    2. Waits for tasks to be written to sqlite
    3. Processes and fails all tasks
    4. Records the number of failed tasks
    """
    print("[taskworker_0] Starting taskworker_0")
    worker = ConfigurableTaskWorker(
        TaskWorkerClient(f"127.0.0.1:{taskbroker_config.grpc_port}"),
        failure_rate=1.0,
    )
    failed_tasks = 0

    next_task = None
    task = None

    # Wait for taskbroker to initialize sqlite and write tasks to it
    print(
        "[taskworker_0]: Waiting for taskbroker to initialize sqlite "
        "and write tasks to it..."
    )
    while not tasks_written_event.is_set() and not shutdown_event.is_set():
        time.sleep(1)

    if not shutdown_event.is_set():
        print("[taskworker_0]: Failing tasks on purpose...")
    try:
        while not shutdown_event.is_set():
            if next_task:
                task = next_task
                next_task = None
            else:
                task = worker.fetch_task()
            if not task:
                break
            next_task = worker.process_task(task)
            failed_tasks += 1

    except Exception as e:
        print(f"[taskworker_0]: Worker process crashed: {e}")
        return

    print("[taskworker_0]: Finished failing all tasks in sqlite")
    with open(taskworker_results_log, "a") as log_file:
        log_file.write(f"failed_tasks:{failed_tasks}")

    shutdown_event.set()


def wait_for_tasks_to_be_written(
    taskbroker_config: TaskbrokerConfig,
    num_messages: int,
    tasks_written_event: threading.Event,
    timeout: int,
) -> bool:
    """
    Wait for all tasks to be written to sqlite.
    Returns whether all tasks were written successfully.
    """
    end = time.time() + timeout
    while time.time() < end and not tasks_written_event.is_set():
        written_tasks = get_num_tasks_in_sqlite(taskbroker_config)
        if written_tasks == num_messages:
            print(
                f"[taskbroker_0]: Finished writing all {num_messages} "
                f"task(s) to sqlite. Sending signal to taskworker(s) "
                f"to start processing"
            )
            tasks_written_event.set()
            return True
        time.sleep(1)
    return False


def wait_for_upkeep_to_handle_tasks(
    taskbroker_config: TaskbrokerConfig,
    timeout: int,
) -> None:
    """Wait for all tasks to be handled by upkeep (discarded or DLQ'ed)."""
    end = time.time() + timeout
    while time.time() < end:
        task_count = get_num_tasks_in_sqlite(taskbroker_config)
        if task_count == 0:
            print("[taskbroker_0]: Upkeep has discarded or DLQ'ed all tasks.")
            return
        print(
            f"[taskbroker_0]: Waiting for upkeep to discard or DLQ all tasks. "
            f"Tasks in sqlite count: {task_count}"
        )
        time.sleep(3)


def manage_taskbroker(
    taskbroker_path: str,
    config_file_path: str,
    taskbroker_config: TaskbrokerConfig,
    log_file_path: str,
    timeout: int,
    num_messages: int,
    tasks_written_event: threading.Event,
    shutdown_event: threading.Event,
) -> None:
    """Manage the taskbroker process lifecycle.
    This function:
    1. Starts the taskbroker process
    2. Waits for tasks to be written to sqlite
    3. Waits for tasks to be processed
    4. Gracefully shuts down the taskbroker
    """
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
        time.sleep(3)  # give the taskbroker some time to start

        # Wait for tasks to be written to sqlite
        if not wait_for_tasks_to_be_written(
            taskbroker_config, num_messages, tasks_written_event, timeout
        ):
            print(
                "[taskbroker_0]: Timeout elapsed and not all tasks have been "
                "written to sqlite. Signalling taskworker to stop"
            )
            shutdown_event.set()
        else:
            # Wait for tasks to be processed
            print(
                "[taskbroker_0]: Waiting for taskworker(s) to finish "
                "processing..."
            )
            wait_for_upkeep_to_handle_tasks(taskbroker_config, timeout)

        # Stop the taskbroker
        print("[taskbroker_0]: Shutting down taskbroker")
        process.send_signal(signal.SIGINT)
        try:
            return_code = process.wait(timeout=10)
            if return_code != 0:
                raise RuntimeError(
                    f"Taskbroker exited with non-zero code: {return_code}"
                )
        except subprocess.TimeoutExpired:
            print("[taskbroker_0]: Force killing taskbroker after timeout")
            process.kill()
            raise


def test_failed_tasks() -> None:
    """
    What does this test do?
    This tests is responsible for ensuring that failed tasks are properly
    handled by taskbroker. When these tasks are failed, taskbroker is
    responsible for discarding or deadlettering the task according to
    the task's configuration. This occurs in the upkeep thread.

    How does it accomplish this?
    To accomplish this, the test starts a taskworker and a taskbroker in
    separate threads. Synchronization events are used to instruct the
    taskworker when start processing and shutdown. The taskworker will
    purposely fail to process a task and set it to a failed state.
    During the upkeep interval, the upkeep thread will appropriately
    discard and deadletter tasks. Finally, the total number of
    discarded and deadlettered tasks are validated.

    Sequence diagram:
    [Thread 1: Taskbroker]                                     [Thread 2: Taskworker]
             |                                                              |
             |                                                              |
    Start Taskbroker                                               Start taskworker
             |                                                              |
             |                                                              |
    Consume kafka and write to sqlite                                       |
             .                                                              |
             .                                                              |
    Done initializing and writing to sqlite ---------[Send signal]--------->|
             |                                                              |
    Upkeep thread runs in background                                   Fail tasks
             .                                                              .
             .                                                              .
             .                                                              .
             .<---------------[send signal]------------------------Finish failing tasks
             .                                                              |
             .                                                      Stop taskworker
             .                                                              |
    Upkeep thread finsihes discarding and DLQ'ing                           |
             .                                                              |
    Once received shutdown signal                                           |
    from workers, Stop taskbroker                                           |
    """

    # Test configuration
    taskbroker_path = str(TASKBROKER_BIN)
    num_messages = 10_000
    num_partitions = 1
    num_workers = 1
    max_pending_count = 100_000
    # Time in seconds to for messages to be written to sqlite and processeed by upkeep
    taskbroker_timeout = 60
    topic_name = "taskworker"
    kafka_deadletter_topic = "taskworker-dlq"
    curr_time = int(time.time())

    print(
        f"""
Running test with the following configuration:
        num of messages: {num_messages},
        num of partitions: {num_partitions},
        num of workers: {num_workers},
        max pending count: {max_pending_count},
        topic name: {topic_name}
    """
    )

    create_topic(topic_name, num_partitions)

    # Create config file for taskbroker
    print("Creating config file for taskbroker")
    TEST_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    db_name = f"db_0_{curr_time}_test_failed_tasks"
    config_filename = "config_0_test_failed_tasks.yml"
    taskbroker_config = TaskbrokerConfig(
        db_name=db_name,
        db_path=str(TEST_OUTPUT_PATH / f"{db_name}.sqlite"),
        max_pending_count=max_pending_count,
        kafka_topic=topic_name,
        kafka_deadletter_topic=kafka_deadletter_topic,
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
                    OnAttemptsExceeded.ON_ATTEMPTS_EXCEEDED_DISCARD
                )
            else:
                task_activation = generate_task_activation(
                    OnAttemptsExceeded.ON_ATTEMPTS_EXCEEDED_DEADLETTER
                )
            custom_messages.append(task_activation)

        send_custom_messages_to_topic(topic_name, custom_messages)

        tasks_written_event = threading.Event()
        shutdown_event = threading.Event()
        taskbroker_log_path = str(
            TEST_OUTPUT_PATH
            / f"taskbroker_0_{curr_time}_test_failed_tasks.log"
        )
        taskbroker_thread = threading.Thread(
            target=manage_taskbroker,
            args=(
                taskbroker_path,
                str(TEST_OUTPUT_PATH / config_filename),
                taskbroker_config,
                taskbroker_log_path,
                taskbroker_timeout,
                num_messages,
                tasks_written_event,
                shutdown_event,
            ),
        )
        taskbroker_thread.start()

        worker_results_log_path = str(
            TEST_OUTPUT_PATH
            / f"taskworker_results_{curr_time}_test_failed_tasks.log"
        )
        worker_thread = threading.Thread(
            target=manage_taskworker,
            args=(
                taskbroker_config,
                worker_results_log_path,
                tasks_written_event,
                shutdown_event,
            ),
        )
        worker_thread.start()

        taskbroker_thread.join()
        worker_thread.join()
    except Exception as e:
        raise Exception(f"Error running taskbroker: {e}")

    if not tasks_written_event.is_set():
        pytest.fail("Not all messages were written to sqlite.")

    total_failed = 0
    with open(worker_results_log_path, "r") as worker_results_file:
        line = worker_results_file.readline()
        total_failed += int(line.split(":")[1])

    remaining_in_sqlite = get_num_tasks_in_sqlite(taskbroker_config)

    print(
        f"\nTotal tasks failed by worker on purpose: {total_failed},"
        f"\nTotal tasks remaining in sqlite: {remaining_in_sqlite}"
    )

    dlq_size = get_topic_size(kafka_deadletter_topic)
    # Verify all tasks were failed on purpose
    assert total_failed == num_messages
    # Verify half of tasks were sent to DLQ
    assert dlq_size == num_messages / 2
    # Verify no tasks remain in sqlite
    assert remaining_in_sqlite == 0
