import pytest
import signal
import sqlite3
import subprocess
import threading
import time

import yaml

from python.integration_tests.helpers import (
    TASKBROKER_BIN,
    TESTS_OUTPUT_ROOT,
    send_messages_to_kafka,
    create_topic,
)

from python.integration_tests.worker import SimpleTaskWorker, TaskWorkerClient


TEST_OUTPUT_PATH = TESTS_OUTPUT_ROOT / "test_upkeep_retry"


class TaskRetriedCounter:
    """
    A thread safe class that track of the total number of tasks that have been retried.
    """
    def __init__(self):
        self.task_retried_counter = 0
        self._lock = threading.Lock()

    def increment(self):
        with self._lock:
            self.task_retried_counter += 1
            return self.task_retried_counter

    def get(self):
        with self._lock:
            return self.task_retried_counter


counter = TaskRetriedCounter()


def manage_taskworker(
    worker_id: int,
    consumer_config: dict,
    log_file_path: str,
    tasks_written_event: threading.Event,
    shutdown_event: threading.Event,
    num_messages: int,
    retries_per_task: int,
) -> None:
    """
    TODO: Retry consumer.
    """
    print(f"[taskworker_{worker_id}] Starting taskworker_{worker_id}")
    worker = SimpleTaskWorker(
        TaskWorkerClient(f"127.0.0.1:{consumer_config['grpc_port']}")
    )
    fetched_tasks = 0
    completed_tasks = 0
    retried_tasks = 0

    next_task = None
    task = None

    # Wait for consumer to initialize sqlite and write tasks to it
    print(
        f"[taskworker_{worker_id}]: Waiting for consumer to initialize sqlite and write tasks to it..."
    )
    while not tasks_written_event.is_set() and not shutdown_event.is_set():
        time.sleep(1)

    if not shutdown_event.is_set():
        print(f"[taskworker_{worker_id}]: Processing tasks...")
    try:
        while not shutdown_event.is_set():
            if next_task:
                task = next_task
                next_task = None
                fetched_tasks += 1
            else:
                task = worker.fetch_task()
                if task:
                    fetched_tasks += 1
            if not task:
                task_retried_count = counter.get()
                if task_retried_count >= num_messages * retries_per_task:
                    print(
                        f"[taskworker_{worker_id}]: All tasks have been retried {retries_per_task} times, shutting down taskworker_{worker_id}"
                    )
                    shutdown_event.set()
                    break
                else:
                    time.sleep(3)
                    continue

            # If the tasks's retry policy is less than specificed attempts, set the task to retry state.
            # Otherwise, set the task to completed state.
            if task.retry_state.attempts < retries_per_task:
                next_task = worker.process_task_with_retry(task)
                retried_tasks += 1
                current_retried_count = counter.increment()
                print(f"[taskworker_{worker_id}]:Tasks retry attempts: {current_retried_count}/{num_messages * retries_per_task}")
            else:
                next_task = worker.process_task(task)
                completed_tasks += 1
    except Exception as e:
        print(f"[taskworker_{worker_id}]: Worker process crashed: {e}")
        return

    with open(log_file_path, "a") as log_file:
        log_file.write(f"Fetched:{fetched_tasks}, Retried:{retried_tasks}, Completed:{completed_tasks}")


def manage_consumer(
    consumer_path: str,
    config_file_path: str,
    consumer_config: dict,
    log_file_path: str,
    timeout: int,
    num_messages: int,
    tasks_written_event: threading.Event,
    shutdown_events: list[threading.Event],
) -> None:
    with open(log_file_path, "a") as log_file:
        print(f"[consumer_0] Starting consumer, writing log file to {log_file_path}")
        process = subprocess.Popen(
            [consumer_path, "-c", config_file_path],
            stderr=subprocess.STDOUT,
            stdout=log_file,
        )
        time.sleep(3)  # give the consumer some time to start

        # Let the consumer write the messages to sqlite
        end = time.time() + timeout
        while (time.time() < end) and (not tasks_written_event.is_set()):
            written_tasks = check_num_tasks_written(consumer_config)
            if written_tasks == num_messages:
                print(
                    f"[consumer_0]: Finishing writting all {num_messages} task(s) to sqlite. Sending signal to taskworker(s) to start processing"
                )
                tasks_written_event.set()
            time.sleep(1)

        # Keep gRPC consumer alive until taskworker is done processing
        if tasks_written_event.is_set():
            print("[consumer_0]: Waiting for taskworker(s) to finish processing...")
            while not all(
                shutdown_event.is_set() for shutdown_event in shutdown_events
            ):
                time.sleep(1)
            print("[consumer_0]: Received shutdown signal from all taskworker(s)")
        else:
            print(
                "[consumer_0]: Timeout elapse and not all tasks have been written to sqlite. Signalling taskworker(s) to stop"
            )
            for shutdown_event in shutdown_events:
                shutdown_event.set()

        # Stop the consumer
        print("[consumer_0]: Shutting down consumer")
        process.send_signal(signal.SIGINT)
        try:
            return_code = process.wait(timeout=10)
            assert return_code == 0
        except Exception:
            process.kill()


def check_num_tasks_written(consumer_config: dict) -> int:
    attach_db_stmt = f"ATTACH DATABASE '{consumer_config['db_path']}' AS {consumer_config['db_name']};\n"
    query = f"""SELECT count(*) as count FROM {consumer_config['db_name']}.inflight_taskactivations;"""
    con = sqlite3.connect(consumer_config["db_path"])
    cur = con.cursor()
    cur.executescript(attach_db_stmt)
    rows = cur.execute(query).fetchall()
    count = rows[0][0]
    return count


def test_upkeep_retry() -> None:
    """
    Notes on this test:
    Q: What is the mechanism that sets a task to retry?
    A: The upkeep step itself gets retry tests, and reproduces them to kafka.
        The worker is in charge if setting a task to retry state.
    Q: What is the mechanism that sets a task to DLQ?
    A: handle_deadletter_at() in upkeep  is the function that sets tasks that are past their deadletter_at deadline to failure state.
        Then, handle_failed_tasks() is called to set the task to DLQ.

    1. Append a known number of tasks to the topic.
        a. The tasks should error out in the worker and workers should set its state to retry
    2. Ensure that all tasks have been set to a retry state.
    3. Process all the tasks until they are now completed.
    4. Append a known number of tasks that have an extremely short or instant deadletter limit
    5. sleep for a bit
    6. Ensure that the known number of tasks are send to the DLQ.
    
    ------------------------------------------------------------------------------------------------
    """

    # Test configuration
    consumer_path = str(TASKBROKER_BIN)
    num_messages = 5_000
    retries_per_task = 3
    num_partitions = 1
    num_workers = 20
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
        attempts per task: {retries_per_task},
        num of partitions: {num_partitions},
        num of workers: {num_workers},
        max pending count: {max_pending_count},
        topic name: {topic_name}
    """
    )

    create_topic(topic_name, num_partitions)

    # Create config file for consumer
    print("Creating config file for consumer")
    TEST_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    db_name = f"db_0_{curr_time}_test_upkeep_retry"
    config_filename = "config_0_test_upkeep_retry.yml"
    consumer_config = {
        "db_name": db_name,
        "db_path": str(TEST_OUTPUT_PATH / f"{db_name}.sqlite"),
        "max_pending_count": max_pending_count,
        "kafka_topic": topic_name,
        "kafka_consumer_group": topic_name,
        "kafka_auto_offset_reset": "earliest",
        "grpc_port": 50051,
    }

    with open(str(TEST_OUTPUT_PATH / config_filename), "w") as f:
        yaml.safe_dump(consumer_config, f)

    try:
        send_messages_to_kafka(topic_name, num_messages)
        tasks_written_event = threading.Event()
        shutdown_events = [threading.Event() for _ in range(num_workers)]
        consumer_thread = threading.Thread(
            target=manage_consumer,
            args=(
                consumer_path,
                str(TEST_OUTPUT_PATH / config_filename),
                consumer_config,
                str(
                    TEST_OUTPUT_PATH
                    / f"consumer_0_{curr_time}_test_upkeep_retry.log"
                ),
                consumer_timeout,
                num_messages,
                tasks_written_event,
                shutdown_events,
            ),
        )
        consumer_thread.start()

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
                    consumer_config,
                    log_file,
                    tasks_written_event,
                    shutdown_events[i],
                    num_messages,
                    retries_per_task,
                ),
            )
            worker_thread.start()
            worker_threads.append(worker_thread)

        consumer_thread.join()
        for t in worker_threads:
            t.join()
    except Exception as e:
        raise Exception(f"Error running taskbroker: {e}")

    if not tasks_written_event.is_set():
        pytest.fail(f"Not all messages were written to sqlite.")

    total_fetched = 0
    total_retried = 0
    total_completed = 0

    for log_file in worker_log_files:
        with open(log_file, "r") as log_file:
            line = log_file.readline()
            total_fetched += int(line.split(",")[0].split(":")[1])
            total_retried += int(line.split(",")[1].split(":")[1])
            total_completed += int(line.split(",")[2].split(":")[1])

    print(
        f"\nTotal tasks fetched: {total_fetched}, Total tasks retried: {total_retried}, Total tasks completed: {total_completed}"
    )

    # assert total_fetched == num_messages
    # assert total_completed == num_messages
