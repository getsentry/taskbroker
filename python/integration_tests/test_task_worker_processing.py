import pytest
import signal
import sqlite3
import subprocess
import threading
import time

import yaml

from pathlib import Path
from python.integration_tests.helpers import (
    TASKBROKER_BIN,
    send_messages_to_kafka,
    check_topic_exists,
    create_topic,
    recreate_topic,
)

from python.integration_tests.worker import SimpleTaskWorker, TaskWorkerClient


TEST_OUTPUT_PATH = Path(__file__).parent / ".output_from_test_task_worker_processing"


def manage_taskworker(
    worker_id: int,
    consumer_config: dict,
    log_file_path: str,
    num_messages: int,
    tasks_written_event: threading.Event,
    shutdown_event: threading.Event
) -> None:
    print(f"[taskworker_{worker_id}] Starting taskworker_{worker_id}")
    worker = SimpleTaskWorker(TaskWorkerClient(f"127.0.0.1:{consumer_config['grpc_port']}"))
    fetched_tasks = 0
    completed_tasks = 0

    next_task = None
    task = None

    # Wait for consumer to initialize sqlite and write tasks to it
    print(f"[taskworker_{worker_id}]: Waiting for consumer to initialize sqlite and write tasks to it...")
    while not tasks_written_event.is_set():
        time.sleep(1)

    print(f"[taskworker_{worker_id}]: Processing tasks...")
    try:
        while not shutdown_event.is_set():
            if next_task:
                task = next_task
                next_task = None
            else:
                task = worker.fetch_task()
                if task:
                    fetched_tasks += 1

            if not task:
                if get_num_tasks_by_status(consumer_config, 'Pending') == 0:
                    print(f"[taskworker_{worker_id}]: No more pending tasks to retrieve, shutting down taskworker_{worker_id}")
                    shutdown_event.set()
                    break
                time.sleep(1)
                continue
            else:
                next_task = worker.process_task(task)
                if next_task:
                    fetched_tasks += 1
                completed_tasks += 1
    except Exception as e:
        print(f"[taskworker_{worker_id}]: Worker process crashed: {e}")
        return 2

    with open(log_file_path, "a") as log_file:
        log_file.write(f"Fetched:{fetched_tasks}, Completed:{completed_tasks}")


def manage_consumer(
    consumer_path: str,
    config_file_path: str,
    consumer_config: dict,
    log_file_path: str,
    timeout: int,
    num_messages: int,
    tasks_written_event: threading.Event,
    shutdown_event: threading.Event
) -> None:
    with open(log_file_path, "a") as log_file:
        print(
            f"[consumer_0] Starting consumer, writing log file to {log_file_path}"
        )
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
                print(f"[consumer_0]: Finishing writting all {num_messages} task(s) to sqlite. Sending signal to taskworker(s) to start processing")
                tasks_written_event.set()
            time.sleep(1)

        # Keep gRPC consumer alive until taskworker is done processing
        if tasks_written_event.is_set():
            print("[consumer_0]: Waiting for taskworker to finish processing...")
            while not shutdown_event.is_set():
                time.sleep(1)
            print("[consumer_0]: Received shutdown signal from taskworker")
        else:
            print("[consumer_0]: Timeout elapse and not all tasks have been written to sqlite. Signalling taskworker(s) to stop")
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


def get_num_tasks_by_status(consumer_config: dict, status: str) -> int:
    attach_db_stmt = f"ATTACH DATABASE '{consumer_config['db_path']}' AS {consumer_config['db_name']};\n"
    query = f"""SELECT count(*) as count FROM {consumer_config['db_name']}.inflight_taskactivations WHERE status = '{status}';"""
    con = sqlite3.connect(consumer_config["db_path"])
    cur = con.cursor()
    cur.executescript(attach_db_stmt)
    rows = cur.execute(query).fetchall()
    count = rows[0][0]
    return count


def test_task_worker_processing() -> None:
    """
    This tests is responsible for ensuring that all sent to taskbroker are processed and completed by taskworker only once.

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
             |<-------------[send shutdown signal]-------------Completed processing all tasks
             |                                                              |
             |                                                              |
    Stop consumer                                                   Stop taskworker(s)
    """

    # Test configuration
    consumer_path = str(TASKBROKER_BIN)
    num_messages = 1000
    num_partitions = 1
    num_workers = 2
    max_pending_count = 100_000
    consumer_timeout = 15  # the time in seconds to wait for all messages to be written to sqlite
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

    # Ensure topic exists, if it does, ensure a clean state
    if not check_topic_exists(topic_name):
        print(
            f"{topic_name} topic does not exist, creating it with {num_partitions} partition"
        )
        create_topic(topic_name, num_partitions)
    else:
        print(
            f"{topic_name} topic already exists, recreating it with {num_partitions} partition to ensure a clean state"
        )
        recreate_topic(topic_name, num_partitions)

    # Create config file for consumer
    print("Creating config file for consumer")
    TEST_OUTPUT_PATH.mkdir(exist_ok=True)
    db_name = f"db_0_{curr_time}_test_task_worker_processing"
    config_filename = "config_0_test_task_worker_processing.yml"
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
        shutdown_event = threading.Event()
        consumer_thread = threading.Thread(
            target=manage_consumer,
            args=(
                consumer_path,
                str(TEST_OUTPUT_PATH / config_filename),
                consumer_config,
                str(TEST_OUTPUT_PATH / f"consumer_0_{curr_time}_test_task_worker_processing.log"),
                consumer_timeout,
                num_messages,
                tasks_written_event,
                shutdown_event
            ),
        )
        consumer_thread.start()

        worker_threads = []
        worker_log_files = []
        for i in range(num_workers):
            log_file = str(TEST_OUTPUT_PATH / f"taskworker_{i}_output_{curr_time}_test_task_worker_processing.log")
            worker_log_files.append(log_file)
            worker_thread = threading.Thread(
                target=manage_taskworker,
                args=(i, consumer_config, log_file, num_messages, tasks_written_event, shutdown_event),
            )
            worker_thread.start()
            worker_threads.append(worker_thread)

        consumer_thread.join()
        for t in worker_threads:
            t.join()
    except Exception as e:
        raise Exception(f"Error running taskbroker: {e}")

    if not tasks_written_event.is_set():
        pytest.fail(f"Not all messages were written to sqlite. Produced to kafka: {num_messages}, Written to sqlite: {num_tasks_written}")

    total_fetched = 0
    total_completed = 0
    for log_file in worker_log_files:
        with open(log_file, "r") as log_file:
            line = log_file.readline()
            total_fetched += int(line.split(',')[0].split(':')[1])
            total_completed += int(line.split(',')[1].split(':')[1])

    print(f"Total tasks fetched: {total_fetched}, Total tasks completed: {total_completed}")
    assert total_fetched == num_messages
    assert total_completed == num_messages
