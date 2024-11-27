import os
import random
import signal
import sqlite3
import subprocess
import threading
import time

from pathlib import Path
from threading import Thread

import yaml


def manage_consumer(
        consumer_index: int,
        consumer_path: str,
        config_file: str,
        iterations: int,
        min_sleep: int,
        max_sleep: int,
        log_file_path: str,
) -> None:
    with open(log_file_path, "a") as log_file:
        print(f"Starting consumer {consumer_index}, writing log file to {log_file_path}")
        for i in range(iterations):
            config_file_path = f"integration_tests/tmp/{config_file}"
            process = subprocess.Popen(
                [consumer_path, "-c", config_file_path], stderr=subprocess.STDOUT, stdout=log_file
            )
            time.sleep(random.randint(min_sleep, max_sleep))
            print(
                f"Sending SIGINT to consumer {consumer_index}, {iterations - i - 1} SIGINTs remaining"
            )
            process.send_signal(signal.SIGINT)
            try:
                return_code = process.wait(timeout=10)
                assert return_code == 0
            except Exception:
                process.kill()

def test_tasks_written_once_during_rebalancing():
    # Test configuration
    consumer_path = "../target/debug/taskbroker"
    num_consumers = 8
    num_messages = 80_000
    num_restarts = 4
    min_restart_duration = 5
    max_restart_duration = 20

    # First check if task-worker topic exists
    print("Checking if task-worker topic already exists")
    check_topic_cmd = [
        "docker",
        "exec",
        "sentry_kafka",
        "kafka-topics",
        "--bootstrap-server",
        "localhost:9092",
        "--list",
    ]
    result = subprocess.run(check_topic_cmd, check=True, capture_output=True, text=True)
    topics = result.stdout.strip().split("\n")

    # Create/Update task-worker Kafka topic with 32 partitions
    if "task-worker" not in topics:
        print("Task-worker topic does not exist, creating it with 32 partitions")
        create_topic_cmd = [
            "docker",
            "exec",
            "sentry_kafka",
            "kafka-topics",
            "--bootstrap-server",
            "localhost:9092",
            "--create",
            "--topic",
            "task-worker",
            "--partitions",
            "32",
            "--replication-factor",
            "1",
        ]
        subprocess.run(create_topic_cmd, check=True)
    else:
        print("Task-worker topic already exists, making sure it has 32 partitions")
        try:
            create_topic_cmd = [
                "docker",
                "exec",
                "sentry_kafka",
                "kafka-topics",
                "--bootstrap-server",
                "localhost:9092",
                "--alter",
                "--topic",
                "task-worker",
                "--partitions",
                "32",
            ]
            subprocess.run(create_topic_cmd, check=True)
        except Exception:
            pass

    # Create config files for consumers
    print("Creating config files for consumers in taskbroker/tests")
    consumer_configs = {}
    curr_time = int(time.time())
    for i in range(num_consumers):
        consumer_configs[f"config_{i}.yml"] = {
            "db_name": f"db_{i}_{curr_time}",
            "db_path": f"{os.getcwd()}/integration_tests/tmp/db_{i}_{curr_time}.sqlite",
            "kafka_topic": "task-worker",
            "kafka_consumer_group": "task-worker-integration-test",
            "kafka_auto_offset_reset": "earliest",
            "grpc_port": 50051 + i,
        }

    config_dir = Path("integration_tests/tmp")
    config_dir.mkdir(parents=True, exist_ok=True)

    for filename, config in consumer_configs.items():
        with open(config_dir / filename, "w") as f:
            yaml.safe_dump(config, f)

    try:
        # TODO: Use sentry run CLI to produce messages to topic

        threads: list[Thread] = []
        for i in range(num_consumers):
            thread = threading.Thread(
                target=manage_consumer,
                args=(
                    i,
                    consumer_path,
                    f"config_{i}.yml",
                    num_restarts,
                    min_restart_duration,
                    max_restart_duration,
                    f"integration_tests/tmp/consumer_{i}.log",
                ),
            )
            thread.start()
            threads.append(thread)

        for t in threads:
            t.join()

    except Exception:
        raise

    # Validate that all tasks were written once during rebalancing
    attach_db_stmt = "".join(
        [
            f"ATTACH DATABASE '{config['db_path']}' AS {config['db_name']};\n"
            for config in consumer_configs.values()
        ]
    )
    from_stmt = "\nUNION ALL\n".join(
        [
            f"    SELECT * FROM {config['db_name']}.inflight_taskactivations"
            for config in consumer_configs.values()
        ]
    )
    query = f"""
        SELECT
            partition,
            (max(offset) - min(offset)) + 1 AS offset_diff,
            count(*) AS occ,
            (max(offset) - min(offset)) + 1 - count(offset) AS delta
        FROM (
        {from_stmt}
        )
        GROUP BY partition
        ORDER BY partition;
    """

    con = sqlite3.connect(consumer_configs["config_0.yml"]["db_path"])
    cur = con.cursor()
    cur.executescript(attach_db_stmt)
    res = cur.execute(query)
    assert all(
        [res[3] == 0 for res in res.fetchall()]
    )  # Assert that each value in the delta (fourth) column is 0
    print("Taskbroker integration test completed successfully.")
