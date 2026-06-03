import signal
import subprocess
import time

import pytest
import yaml

from integration_tests.helpers import (
    TASKBROKER_BIN,
    TESTS_OUTPUT_ROOT,
    TaskbrokerConfig,
    create_topic,
    get_available_ports,
    get_num_tasks_in_sqlite,
    send_generic_messages_to_topic,
)

TEST_OUTPUT_PATH = TESTS_OUTPUT_ROOT / "test_multi_topic"


def test_multi_topic_consumption() -> None:
    """
    Verify that a single taskbroker configured (new `kafka_topics` format) with
    two consumable topics consumes from BOTH into its one sqlite store.

    Each consumed topic gets its own rdkafka consumer (own group.id); both
    pipelines write to the shared store. We produce N messages to each of two
    topics and assert the broker ends up with 2*N tasks in sqlite.
    """
    num_messages_per_topic = 1_000
    num_partitions = 4
    timeout = 60
    curr_time = int(time.time())

    topic_a = f"multitopic-a-{curr_time}"
    topic_b = f"multitopic-b-{curr_time}"
    retry_topic = f"multitopic-retry-{curr_time}"
    dlq_topic = f"multitopic-dlq-{curr_time}"

    # Pre-create the topics so the test exercises consumption, not topic
    # creation. (create_missing_topics stays off.)
    for topic in (topic_a, topic_b, retry_topic, dlq_topic):
        create_topic(topic, num_partitions)

    TEST_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    db_name = f"db_multi_topic_{curr_time}"
    db_path = str(TEST_OUTPUT_PATH / f"{db_name}.sqlite")
    config_filename = f"config_multi_topic_{curr_time}.yml"
    grpc_port = get_available_ports(1)[0]

    # New multi-topic config: two consumable topics + produce-only retry/dlq,
    # all on a single cluster.
    config_dict = {
        "db_name": db_name,
        "db_path": db_path,
        "max_pending_count": 100_000,
        "grpc_port": grpc_port,
        "kafka_auto_offset_reset": "earliest",
        "kafka_deadletter_topic": dlq_topic,
        "kafka_retry_topic": retry_topic,
        "kafka_clusters": {
            "default": {"address": "127.0.0.1:9092"},
        },
        "kafka_topics": {
            topic_a: {"cluster": "default", "consumer_group": f"{topic_a}-grp"},
            topic_b: {"cluster": "default", "consumer_group": f"{topic_b}-grp"},
            retry_topic: {
                "cluster": "default",
                "consumer_group": f"{retry_topic}-grp",
                "produce_only": True,
            },
            dlq_topic: {
                "cluster": "default",
                "consumer_group": f"{dlq_topic}-grp",
                "produce_only": True,
            },
        },
    }

    config_path = str(TEST_OUTPUT_PATH / config_filename)
    with open(config_path, "w") as f:
        yaml.safe_dump(config_dict, f)

    # A TaskbrokerConfig instance is only needed for the sqlite-counting helper,
    # which reads db_name/db_path.
    query_config = TaskbrokerConfig(
        db_name=db_name,
        db_path=db_path,
        max_pending_count=100_000,
        kafka_topic=topic_a,
        kafka_deadletter_topic=dlq_topic,
        kafka_consumer_group=f"{topic_a}-grp",
        kafka_auto_offset_reset="earliest",
        grpc_port=grpc_port,
    )

    log_path = str(TEST_OUTPUT_PATH / f"taskbroker_multi_topic_{curr_time}.log")
    expected_total = num_messages_per_topic * 2

    send_generic_messages_to_topic(topic_a, num_messages_per_topic)
    send_generic_messages_to_topic(topic_b, num_messages_per_topic)

    process = None
    try:
        with open(log_path, "a") as log_file:
            process = subprocess.Popen(
                [str(TASKBROKER_BIN), "-c", config_path],
                stderr=subprocess.STDOUT,
                stdout=log_file,
            )
        time.sleep(3)  # give the broker time to start both consumers

        written = 0
        end = time.time() + timeout
        while time.time() < end:
            written = get_num_tasks_in_sqlite(query_config)
            if written >= expected_total:
                break
            # the broker should still be alive while consuming
            assert process.poll() is None, "taskbroker exited early"
            time.sleep(1)

        assert written == expected_total, (
            f"expected {expected_total} tasks in sqlite "
            f"({num_messages_per_topic} from each of two topics), got {written}"
        )
    finally:
        if process is not None:
            process.send_signal(signal.SIGINT)
            try:
                assert process.wait(timeout=10) == 0
            except Exception:
                process.kill()
                raise
