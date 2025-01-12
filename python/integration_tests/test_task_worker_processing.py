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
    send_messages_to_kafka,
    create_topic,
    check_num_tasks_written,
    ConsumerConfig
)

from python.integration_tests.worker import ConfigurableTaskWorker, TaskWorkerClient


TEST_OUTPUT_PATH = TESTS_OUTPUT_ROOT / "test_task_worker_processing"
processed_tasks = defaultdict(list)  # key: task_id, value: worker_id
mutex = threading.Lock()


def manage_taskworker(
    worker_id: int,
    consumer_config: ConsumerConfig,
    log_file_path: str,
    tasks_written_event: threading.Event,
    shutdown_event: threading.Event,
) -> None:
    print(f"[taskworker_{worker_id}] Starting taskworker_{worker_id}")
    worker = ConfigurableTaskWorker(
        TaskWorkerClient(f"127.0.0.1:{consumer_config.grpc_port}")
    )
    fetched_tasks = 0
    completed_tasks = 0

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
                print(
                    f"[taskworker_{worker_id}]: No more pending tasks to retrieve, shutting down taskworker_{worker_id}"
                )
                shutdown_event.set()
                break
            next_task = worker.process_task(task)
            completed_tasks += 1
            with mutex:
                processed_tasks[task.id].append(worker_id)

    except Exception as e:
        print(f"[taskworker_{worker_id}]: Worker process crashed: {e}")
        return

    with open(log_file_path, "a") as log_file:
        log_file.write(f"Fetched:{fetched_tasks}, Completed:{completed_tasks}")


def manage_consumer(
    consumer_path: str,
    config_file_path: str,
    consumer_config: ConsumerConfig,
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


def test_task_worker_processing() -> None:
    """
    What does this test do?
    This tests is responsible for ensuring that all tasks sent to taskbroker
    are processed and completed by taskworker only once. An initial
    amount of messages is produced to kafka. Then, mock taskworkers fetches
    and completes the task (without actually running the activation).
    This process continues until all tasks have been completed only once.

    How does it accomplish this?
    To accomplish this, the test starts N number of taskworker(s) and a
    consumer in separate. threads. Synchronization events are use to
    instruct the taskworker(s) when start processing and shutdown.
    A shared dictionary accessed by a mutex is used to collect duplicate
    processed tasks. Finally, the total number of fetched and completed
    tasks are compared to the number of messages sent to taskbroker.

    Sequence diagram:
    [Thread 1: Consumer]                                        [Thread 2-N: Taskworker(s)]
             |                                                              |
             |                                                              |
    Start consumer                                                 Start taskworker(s)
             |                                                              |
             |                                                              |
    Consume kafka and write to sqlite                                       |
             .                                                              |
             .                                                              |
    Done initializing and writing to sqlite ---------[Send signal]--------->|
             |                                                              |
             |                                                        Process tasks
             |                                                              .
             |                                                              .
             |                                                              .
             |<-------------[send shutdown signal(s)]----------Completed processing all tasks
             |                                                              |
             |                                                      Stop taskworker
             |                                                              |
    Once received shutdown signal(s)                                        |
    from all workers, Stop consumer                                         |
    """

    # Test configuration
    consumer_path = str(TASKBROKER_BIN)
    num_messages = 10_000
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
    db_name = f"db_0_{curr_time}_test_task_worker_processing"
    config_filename = "config_0_test_task_worker_processing.yml"
    consumer_config = ConsumerConfig(db_name, str(TEST_OUTPUT_PATH / f"{db_name}.sqlite"), max_pending_count, topic_name, topic_name, "earliest", 50051)

    with open(str(TEST_OUTPUT_PATH / config_filename), "w") as f:
        yaml.safe_dump(consumer_config.to_dict(), f)

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
                    / f"consumer_0_{curr_time}_test_task_worker_processing.log"
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
                / f"taskworker_{i}_output_{curr_time}_test_task_worker_processing.log"
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
    total_completed = 0
    for log_file in worker_log_files:
        with open(log_file, "r") as log_file:
            line = log_file.readline()
            total_fetched += int(line.split(",")[0].split(":")[1])
            total_completed += int(line.split(",")[1].split(":")[1])

    print(
        f"\nTotal tasks fetched: {total_fetched}, Total tasks completed: {total_completed}"
    )
    duplicate_tasks = [
        (task_id, worker_ids)
        for task_id, worker_ids in processed_tasks.items()
        if len(worker_ids) > 1
    ]
    if duplicate_tasks:
        print("Duplicate processed and completed tasks found:")
        for task_id, worker_ids in duplicate_tasks:
            print(f"Task ID: {task_id}, Worker IDs: {worker_ids}")
    assert total_fetched == num_messages
    assert total_completed == num_messages
