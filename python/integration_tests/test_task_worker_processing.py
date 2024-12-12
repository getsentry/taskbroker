import signal
import sqlite3
import subprocess
import threading
import time

import yaml

from python.integration_tests.helpers import (
    TASKBROKER_BIN,
    TESTS_OUTPUT_PATH,
    send_messages_to_kafka,
    check_topic_exists,
    create_topic,
    update_topic_partitions,
    recreate_topic,
)


def manage_consumer(
    consumer_path: str,
    config_file_path: str,
    log_file_path: str,
    timeout: int,
) -> None:
    with open(log_file_path, "a") as log_file:
        print(
            f"Starting consumer, writing log file to {log_file_path}"
        )
        process = subprocess.Popen(
            [consumer_path, "-c", config_file_path],
            stderr=subprocess.STDOUT,
            stdout=log_file,
        )
        end = time.time() + timeout
        all_messages_written = False
        while (time.time() < end) and (not all_messages_written):
            # TODO: make sure consumer writes all messages in the topic to sqlite
            # TODO: gracefully terminate the consumer
            pass
        process.send_signal(signal.SIGINT)
        try:
            return_code = process.wait(timeout=10)
            assert return_code == 0
        except Exception:
            process.kill()


def test_tasks_written_once_during_rebalancing() -> None:
    # Test configuration
    consumer_path = str(TASKBROKER_BIN)
    num_messages = 3_000
    num_partitions = 1
    max_pending_count = 15_000
    consumer_timeout = 30
    topic_name = "task-worker"
    curr_time = int(time.time())

    print(
        f"""
Running test with the following configuration:
        num of messages: {num_messages},
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
            f"recreating {topic_name} topic with {num_partitions} partition"
        )
        recreate_topic(topic_name, num_partitions)

    # Create config file for consumer
    print("Creating config file for consumer")
    TESTS_OUTPUT_PATH.mkdir(exist_ok=True)
    db_name = f"db_0_{curr_time}_test_tasks_written_once_during_rebalancing"
    config_filename = "config_0_test_tasks_written_once_during_rebalancing.yml"
    consumer_config = {
        "db_name": db_name,
        "db_path": str(TESTS_OUTPUT_PATH / f"{db_name}.sqlite"),
        "max_pending_count": max_pending_count,
        "kafka_topic": topic_name,
        "kafka_consumer_group": topic_name,
        "kafka_auto_offset_reset": "earliest",
        "grpc_port": 50051,
    }

    with open(str(TESTS_OUTPUT_PATH / config_filename), "w") as f:
        yaml.safe_dump(consumer_config, f)

    try:
        send_messages_to_kafka(topic_name, num_messages)
        thread = threading.Thread(
            target=manage_consumer,
            args=(
                consumer_path,
                str(TESTS_OUTPUT_PATH / config_filename),
                str(TESTS_OUTPUT_PATH / f"consumer_0_{curr_time}_test_tasks_written_once_during_rebalancing.log"),
                consumer_timeout,
            ),
        )
        thread.start()
        thread.join()
    except Exception as e:
        raise Exception(f"Error running taskbroker: {e}")

    # Validate that all tasks were written once during rebalancing
    attach_db_stmt = f"ATTACH DATABASE '{consumer_config['db_path']}' AS {consumer_config['db_name']};\n"
    query = f"""SELECT count(*) as count
FROM {consumer_config['db_name']}.inflight_taskactivations;"""

    con = sqlite3.connect(consumer_config["db_path"])
    cur = con.cursor()
    cur.executescript(attach_db_stmt)
    row_count = cur.execute(query).fetchall()
    print("\n======== Verify all messages were written ========")
    print("Query:")
    print(query)
    print("Result:")
    print(f"{'count'.rjust(16)}")
    for (count,) in row_count:
        print(f"{str(count).rjust(16)}")


    test_query = f"""        SELECT
            partition,
            (max(offset) - min(offset)) + 1 AS expected,
            count(*) AS actual,
            (max(offset) - min(offset)) + 1 - count(*) AS diff
        FROM {consumer_config['db_name']}.inflight_taskactivations
        GROUP BY partition
        ORDER BY partition;"""


    test_row_count = cur.execute(test_query).fetchall()
    print("\n======== Verify number of rows based on max and min offset ========")
    print("Query:")
    print(test_query)
    print("Result:")
    print(
        f"{'Partition'.rjust(16)}{'Expected'.rjust(16)}{'Actual'.rjust(16)}{'Diff'.rjust(16)}"
    )
    for partition, expected_row_count, actual_row_count, diff in test_row_count:
        print(
            f"{str(partition).rjust(16)}{str(expected_row_count).rjust(16)}{str(actual_row_count).rjust(16)}{str(diff).rjust(16)}"
        )