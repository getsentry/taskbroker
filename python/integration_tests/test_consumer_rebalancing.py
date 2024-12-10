import random
import shutil
import signal
import sqlite3
import subprocess
import threading
import time

from threading import Thread

import yaml

from python.integration_tests.helpers import (
    TASKBROKER_BIN,
    TESTS_OUTPUT_PATH,
    check_topic_exists,
    create_topic,
    update_topic_partitions,
    send_messages_to_kafka,
)


def manage_consumer(
    consumer_index: int,
    consumer_path: str,
    config_file_path: str,
    iterations: int,
    min_sleep: int,
    max_sleep: int,
    log_file_path: str,
) -> None:
    with open(log_file_path, "a") as log_file:
        print(
            f"Starting consumer {consumer_index}, writing log file to {log_file_path}"
        )
        for i in range(iterations):
            process = subprocess.Popen(
                [consumer_path, "-c", config_file_path],
                stderr=subprocess.STDOUT,
                stdout=log_file,
            )
            time.sleep(random.randint(min_sleep, max_sleep))
            print(
                f"Sending SIGINT to consumer {consumer_index}, {iterations - i - 1} SIGINTs remaining for that consumer"
            )
            process.send_signal(signal.SIGINT)
            try:
                return_code = process.wait(timeout=10)
                assert return_code == 0
            except Exception:
                process.kill()


def test_tasks_written_once_during_rebalancing() -> None:
    # Test configuration
    consumer_path = str(TASKBROKER_BIN)
    num_consumers = 8
    num_messages = 100_000
    num_restarts = 16
    num_partitions = 32
    min_restart_duration = 1
    max_restart_duration = 30
    max_pending_count = 15_000
    topic_name = "task-worker"
    curr_time = int(time.time())

    print(
        f"""
Running test with the following configuration:
        num of consumers: {num_consumers},
        num of messages: {num_messages},
        num of restarts: {num_restarts},
        num of partitions: {num_partitions},
        min restart duration: {min_restart_duration} seconds,
        max restart duration: {max_restart_duration} seconds,
        topic name: {topic_name}
        random seed value: 42
    """
    )
    random.seed(42)

    # Ensure topic has correct number of partitions
    if not check_topic_exists(topic_name):
        print(
            f"{topic_name} topic does not exist, creating it with {num_partitions} partitions"
        )
        create_topic(topic_name, num_partitions)
    else:
        print(
            f"{topic_name} topic already exists, making sure it has {num_partitions} partitions"
        )
        update_topic_partitions(topic_name, num_partitions)

    # Create config files for consumers
    print("Creating config files for consumers")
    TESTS_OUTPUT_PATH.mkdir(exist_ok=True)
    consumer_configs = {}
    for i in range(num_consumers):
        db_name = f"db_{i}_{curr_time}"
        consumer_configs[f"config_{i}.yml"] = {
            "db_name": db_name,
            "db_path": str(TESTS_OUTPUT_PATH / f"{db_name}.sqlite"),
            "max_pending_count": max_pending_count,
            "kafka_topic": topic_name,
            "kafka_consumer_group": topic_name,
            "kafka_auto_offset_reset": "earliest",
            "grpc_port": 50051 + i,
        }

    for filename, config in consumer_configs.items():
        with open(str(TESTS_OUTPUT_PATH / filename), "w") as f:
            yaml.safe_dump(config, f)

    try:
        send_messages_to_kafka(topic_name, num_messages)
        threads: list[Thread] = []
        for i in range(num_consumers):
            thread = threading.Thread(
                target=manage_consumer,
                args=(
                    i,
                    consumer_path,
                    str(TESTS_OUTPUT_PATH / f"config_{i}.yml"),
                    num_restarts,
                    min_restart_duration,
                    max_restart_duration,
                    str(TESTS_OUTPUT_PATH / f"consumer_{i}_{curr_time}.log"),
                ),
            )
            thread.start()
            threads.append(thread)

        for t in threads:
            t.join()

    except Exception as e:
        raise Exception(f"Error running taskbroker: {e}")

    # Validate that all tasks were written once during rebalancing
    attach_db_stmt = "".join(
        [
            f"ATTACH DATABASE '{config['db_path']}' AS {config['db_name']};\n"
            for config in consumer_configs.values()
        ]
    )
    from_stmt = "\n            UNION ALL\n".join(
        [
            f"            SELECT * FROM {config['db_name']}.inflight_taskactivations"
            for config in consumer_configs.values()
        ]
    )
    query = f"""        SELECT
            partition,
            (max(offset) - min(offset)) + 1 AS expected,
            count(*) AS actual,
            (max(offset) - min(offset)) + 1 - count(*) AS diff
        FROM (
{from_stmt}
        )
        GROUP BY partition
        ORDER BY partition;"""

    con = sqlite3.connect(consumer_configs["config_0.yml"]["db_path"])
    cur = con.cursor()
    cur.executescript(attach_db_stmt)
    row_count = cur.execute(query).fetchall()
    print("\n======== Verify number of rows based on max and min offset ========")
    print("Query:")
    print(query)
    print("Result:")
    print(
        f"{'Partition'.rjust(16)}{'Expected'.rjust(16)}{'Actual'.rjust(16)}{'Diff'.rjust(16)}"
    )
    for partition, expected_row_count, actual_row_count, diff in row_count:
        print(
            f"{str(partition).rjust(16)}{str(expected_row_count).rjust(16)}{str(actual_row_count).rjust(16)}{str(diff).rjust(16)}"
        )

    query = f"""        SELECT partition, offset, count(*) as count
        FROM (
{from_stmt}
        )
        GROUP BY partition, offset
        HAVING count > 1"""
    res = cur.execute(query).fetchall()
    print("\n======== Verify all (partition, offset) are unique ========")
    print("Query:")
    print(query)
    print("Result:")
    print(f"{'Partition'.rjust(16)}{'Offset'.rjust(16)}{'count'.rjust(16)}")
    for partition, offset, count in res:
        print(
            f"{str(partition).rjust(16)}{str(offset).rjust(16)}{str(count).rjust(16)}"
        )

    consumers_have_data = True
    print("\n======== Number of rows in each consumer ========")
    for i, config in enumerate(consumer_configs.values()):
        query = f"""SELECT count(*) as count from {config['db_name']}.inflight_taskactivations"""
        res = cur.execute(query).fetchall()[0][0]
        print(
            f"Consumer {i}: {res}, {str(int(res / max_pending_count * 100))}% of capacity"
        )
        consumers_have_data = consumers_have_data and res >= max_pending_count // 3

    consumer_error_logs = []
    for i in range(num_consumers):
        with open(str(TESTS_OUTPUT_PATH / f"consumer_{i}_{curr_time}.log"), "r") as f:
            lines = f.readlines()
            for log_line_index, line in enumerate(lines):
                if "[31mERROR" in line:
                    # If there is an error in log file, capture 10 lines before and after the error line
                    consumer_error_logs.append(f"Error found in consumer_{i}. Logging 10 lines before and after the error line:")
                    for j in range(max(0, log_line_index - 10), min(len(lines) - 1, log_line_index + 10)):
                        consumer_error_logs.append(lines[j].strip())
                    consumer_error_logs.append("")

    if not all([row[3] == 0 for row in row_count]):
        print("Test failed! Got duplicate/missing kafka messages in sqlite")

    if not consumers_have_data:
        print("Test failed! Lower than expected amount of kafka messages in sqlite")

    if consumer_error_logs:
        print("Test failed! Errors in consumer logs")
        for log in consumer_error_logs:
            print(log)

    # Clean up test output files
    print(f"Cleaning up test output files in {TESTS_OUTPUT_PATH}")
    shutil.rmtree(TESTS_OUTPUT_PATH)

    assert all([row[3] == 0 for row in row_count])
    assert consumers_have_data
    assert not consumer_error_logs
