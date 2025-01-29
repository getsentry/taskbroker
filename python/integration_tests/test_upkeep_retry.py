import pytest
import signal
import subprocess
import threading
import time

import yaml

from collections import defaultdict
from python.integration_tests.helpers import (
    TASKBROKER_BIN,
    TESTS_OUTPUT_ROOT,
    send_generic_messages_to_topic,
    create_topic,
    get_num_tasks_in_sqlite,
    TaskbrokerConfig,
)

from python.integration_tests.worker import ConfigurableTaskWorker, TaskWorkerClient


TEST_OUTPUT_PATH = TESTS_OUTPUT_ROOT / "test_upkeep_retry"


class TasksRetriedCounter:
    """
    A thread safe class that tracks the total number of tasks that have
    been retried.
    """

    def __init__(self):
        self.total_retried = 0
        self.tasks_retried = defaultdict(
            int
        )  # key: task_name, value: number of retries
        self._lock = threading.Lock()

    def increment(self, task_name: str):
        with self._lock:
            self.total_retried += 1
            self.tasks_retried[task_name] += 1
            return self.total_retried

    def get_total_retried(self):
        with self._lock:
            return self.total_retried

    def get_tasks_retried(self):
        with self._lock:
            return self.tasks_retried


counter = TasksRetriedCounter()


def manage_taskworker(
    worker_id: int,
    taskbroker_config: TaskbrokerConfig,
    log_file_path: str,
    tasks_written_event: threading.Event,
    shutdown_event: threading.Event,
    num_messages: int,
    retries_per_task: int,
    timeout: int,
) -> None:
    print(f"[taskworker_{worker_id}] Starting taskworker_{worker_id}")
    worker = ConfigurableTaskWorker(
        TaskWorkerClient(f"127.0.0.1:{taskbroker_config.grpc_port}"), retry_rate=1
    )
    retried_tasks = 0
    next_task = None
    task = None
    end = time.time() + timeout

    # Wait for taskbroker to initialize sqlite and write tasks to it
    print(
        f"[taskworker_{worker_id}]: Waiting for taskbroker to initialize "
        f"sqlite and write tasks to it..."
    )
    while not tasks_written_event.is_set() and not shutdown_event.is_set():
        time.sleep(1)

    if not shutdown_event.is_set():
        print(f"[taskworker_{worker_id}]: Processing tasks...")
    try:
        while not shutdown_event.is_set():
            if time.time() > end:
                print(
                    f"[taskworker_{worker_id}]: Timeout elapse. "
                    f"Shutting down taskworker_{worker_id}"
                )
                shutdown_event.set()
                break
            if next_task:
                task = next_task
                next_task = None
            else:
                task = worker.fetch_task()

            # If there are no more pending task to be fetched, check if all
            # tasks have been retried. If so, shutdown the taskworker.
            # If not, wait for upkeep to re-produce the task to kafka.
            if not task:
                task_retried_count = counter.get_total_retried()
                if task_retried_count >= num_messages * retries_per_task:
                    print(
                        f"[taskworker_{worker_id}]: Total tasks retried reached: "
                        f"{task_retried_count}/{num_messages * retries_per_task}. "
                        f"Shutting down taskworker_{worker_id}"
                    )
                    shutdown_event.set()
                    break
                else:
                    time.sleep(1)
                    continue

            # If the tasks's retry policy is less than specificed attempts,
            # set the task to retry state. Otherwise, complete the task.
            if task.retry_state.attempts < retries_per_task:
                next_task = worker.process_task(task)
                retried_tasks += 1
                curr_retried = counter.increment(task.taskname)
                print(
                    f"[taskworker_{worker_id}]: Total tasks retried: "
                    f"{curr_retried}/{num_messages * retries_per_task}"
                )
            else:
                next_task = worker.complete_task(task)
    except Exception as e:
        print(f"[taskworker_{worker_id}]: Worker process crashed: {e}")
        return

    with open(log_file_path, "a") as log_file:
        log_file.write(f"Retried:{retried_tasks}")


def manage_taskbroker(
    taskbroker_path: str,
    config_file_path: str,
    taskbroker_config: TaskbrokerConfig,
    log_file_path: str,
    timeout: int,
    num_messages: int,
    tasks_written_event: threading.Event,
    shutdown_events: list[threading.Event],
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
        time.sleep(3)  # give the taskbroker some time to start

        # Let the taskbroker write the messages to sqlite
        end = time.time() + timeout
        while (time.time() < end) and (not tasks_written_event.is_set()):
            written_tasks = get_num_tasks_in_sqlite(taskbroker_config)
            if written_tasks == num_messages:
                print(
                    f"[taskbroker_0]: Finishing writting all {num_messages} "
                    f"task(s) to sqlite. Sending signal to taskworker(s) "
                    f"to start processing"
                )
                tasks_written_event.set()
            time.sleep(1)

        # Keep gRPC taskbroker alive until taskworker is done processing
        if tasks_written_event.is_set():
            print(
                "[taskbroker_0]: Waiting for taskworker(s) to finish " "processing..."
            )
            while not all(
                shutdown_event.is_set() for shutdown_event in shutdown_events
            ):
                time.sleep(1)
            print("[taskbroker_0]: Received shutdown signal from all " "taskworker(s)")
        else:
            print(
                "[taskbroker_0]: Timeout elapse and not all tasks have been "
                "written to sqlite. Signalling taskworker(s) to stop"
            )
            for shutdown_event in shutdown_events:
                shutdown_event.set()

        # Stop the taskbroker
        print("[taskbroker_0]: Shutting down taskbroker")
        process.send_signal(signal.SIGINT)
        try:
            return_code = process.wait(timeout=10)
            assert return_code == 0
        except Exception:
            process.kill()


def test_upkeep_retry() -> None:
    """
    What does this test do?
    This tests is responsible for checking the integrity of the retry
    mechanism implemented in the upkeep thread of taskbroker. An initial
    amount of messages is produced to kafka with a set number of retries
    in its retry policy. Then, the taskworkers fetch and update the
    task's status to retry. During an interval, the upkeep thread will
    collect these tasks and re-produce the task to kafka. This process
    continues until all tasks have been retried the specified number of times.

    How does it accomplish this?
    The test starts N number of taskworker(s) and a taskbroker in separate
    threads. Synchronization events are use to instruct the taskworker(s)
    when start processing and shutdown. A shared data structured access by
    a mutex called TaskRetriedCounter is used to globally keep track of
    every task retried. Finally, this total number is validated alongside the
    number of times each individual task was retried.

    Sequence diagram:
    [Thread 1: Taskbroker]                                     [Thread 2-N: Taskworker(s)]
             |                                                              |
             |                                                              |
    Start taskbroker                                               Start taskworker(s)
             |                                                              |
             |                                                              |
    Consume kafka and write to sqlite                                       |
             .                                                              |
             .                                                              |
    Done initializing and writing to sqlite ---------[Send signal]--------->|
             |                                                              |
             |                                                         Retry tasks
             |                                                              .
             |                                                              .
    Upkeep thread collects retry tasks and reproduces to topic              .
             |                                                              .
             |                                                              .
             |                                                              .
             |                                                              .
             |<-------------[send shutdown signal(s)]----------Completed retrying all tasks
             |                                                              |
             |                                                      Stop taskworker
             |                                                              |
    Once received shutdown signal(s)                                        |
    from all workers, Stop taskbroker                                       |
    """

    # Test configuration
    taskbroker_path = str(TASKBROKER_BIN)
    num_messages = 5000
    retries_per_task = 3
    num_partitions = 1
    num_workers = 20
    max_pending_count = 100_000
    taskbroker_timeout = (
        60  # the time in seconds to wait for all messages to be written to sqlite
    )
    taskworker_timeout = 600  # the time in seconds for taskworker to finish processing
    topic_name = "task-worker"
    kafka_deadletter_topic = "task-worker-dlq"
    curr_time = int(time.time())

    print(
        f"""
Running test with the following configuration:
        num of messages: {num_messages},
        attempts per task: {retries_per_task},
        num of partitions: {num_partitions},
        num of workers: {num_workers},
        max pending count: {max_pending_count},
        topic name: {topic_name}
    """
    )

    create_topic(topic_name, num_partitions)

    # Create taskbroker config file
    print("Creating config file for taskbroker")
    TEST_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    db_name = f"db_0_{curr_time}_test_upkeep_retry"
    config_filename = "config_0_test_upkeep_retry.yml"
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
        send_generic_messages_to_topic(topic_name, num_messages)
        tasks_written_event = threading.Event()
        shutdown_events = [threading.Event() for _ in range(num_workers)]
        taskbroker_thread = threading.Thread(
            target=manage_taskbroker,
            args=(
                taskbroker_path,
                str(TEST_OUTPUT_PATH / config_filename),
                taskbroker_config,
                str(
                    TEST_OUTPUT_PATH / f"taskbroker_0_{curr_time}_test_upkeep_retry.log"
                ),
                taskbroker_timeout,
                num_messages,
                tasks_written_event,
                shutdown_events,
            ),
        )
        taskbroker_thread.start()

        worker_threads = []
        worker_log_files = []
        for i in range(num_workers):
            log_file = str(
                TEST_OUTPUT_PATH
                / f"taskworker_{i}_output_{curr_time}_test_upkeep_retry.log"
            )
            worker_log_files.append(log_file)
            worker_thread = threading.Thread(
                target=manage_taskworker,
                args=(
                    i,
                    taskbroker_config,
                    log_file,
                    tasks_written_event,
                    shutdown_events[i],
                    num_messages,
                    retries_per_task,
                    taskworker_timeout,
                ),
            )
            worker_thread.start()
            worker_threads.append(worker_thread)

        taskbroker_thread.join()
        for t in worker_threads:
            t.join()
    except Exception as e:
        raise Exception(f"Error running taskbroker: {e}")

    if not tasks_written_event.is_set():
        pytest.fail("Not all messages were written to sqlite.")

    total_retried = 0

    for log_file in worker_log_files:
        with open(log_file, "r") as log_file:
            line = log_file.readline()
            total_retried += int(line.split(",")[0].split(":")[1])

    print(f"\nTotal tasks retried: {total_retried}")

    assert total_retried == num_messages * retries_per_task
    assert all(
        [val == retries_per_task for val in counter.get_tasks_retried().values()]
    )
