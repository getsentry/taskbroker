import socket
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

import orjson
from confluent_kafka import Consumer, KafkaException, Producer
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.taskbroker.v1.taskbroker_pb2 import (
    OnAttemptsExceeded,
    RetryState,
    TaskActivation,
)

TASKBROKER_ROOT = Path(__file__).parent.parent
TASKBROKER_BIN = TASKBROKER_ROOT / "target/debug/taskbroker"
TESTS_OUTPUT_ROOT = Path(__file__).parent / ".tests_output"
TEST_PRODUCER_CONFIG = {
    "bootstrap.servers": "127.0.0.1:9092",
    "broker.address.family": "v4",
}
TEST_CONSUMER_CONFIG = {
    "bootstrap.servers": "127.0.0.1:9092",
    "group.id": "my-group",
    "auto.offset.reset": "earliest",
}


class TaskbrokerConfig:
    def __init__(
        self,
        db_name: str,
        db_path: str,
        max_pending_count: int,
        kafka_topic: str,
        kafka_deadletter_topic: str,
        kafka_consumer_group: str,
        kafka_auto_offset_reset: str,
        grpc_port: int,
    ):
        self.db_name = db_name
        self.db_path = db_path
        self.max_pending_count = max_pending_count
        self.kafka_topic = kafka_topic
        self.kafka_deadletter_topic = kafka_deadletter_topic
        self.kafka_consumer_group = kafka_consumer_group
        self.kafka_auto_offset_reset = kafka_auto_offset_reset
        self.grpc_port = grpc_port

    def to_dict(self) -> dict[str, str | int]:
        return {
            "db_name": self.db_name,
            "db_path": self.db_path,
            "max_pending_count": self.max_pending_count,
            "kafka_topic": self.kafka_topic,
            "kafka_deadletter_topic": self.kafka_deadletter_topic,
            "kafka_consumer_group": self.kafka_consumer_group,
            "kafka_auto_offset_reset": self.kafka_auto_offset_reset,
            "grpc_port": self.grpc_port,
        }


def create_topic(topic_name: str, num_partitions: int) -> None:
    print(f"Creating topic: {topic_name}, with {num_partitions} partitions")
    create_topic_cmd = [
        "docker",
        "exec",
        "kafka-kafka-1",
        "kafka-topics",
        "--bootstrap-server",
        "localhost:9092",
        "--create",
        "--topic",
        topic_name,
        "--partitions",
        str(num_partitions),
    ]
    res = subprocess.run(create_topic_cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"Got return code: {res.returncode}, when creating topic")
        print(f"Stdout: {res.stdout}")
        print(f"Stderr: {res.stderr}")


def serialize_task_activation(i: int, args: list[Any], kwargs: dict[Any, Any]) -> bytes:
    retry_state = RetryState(
        attempts=0,
        max_attempts=1,
        on_attempts_exceeded=OnAttemptsExceeded.ON_ATTEMPTS_EXCEEDED_DISCARD,
    )
    pending_task_payload = TaskActivation(
        id=uuid4().hex,
        namespace="integration_tests",
        taskname=f"integration_tests.say_hello_{i}",
        parameters=orjson.dumps({"args": args, "kwargs": kwargs}).decode("utf-8"),
        retry_state=retry_state,
        processing_deadline_duration=3000,
        received_at=Timestamp(seconds=int(time.time())),
    ).SerializeToString()

    return pending_task_payload


def send_generic_messages_to_topic(topic_name: str, num_messages: int) -> None:
    """
    Send num_messages to kafka topic using unique task names.
    """
    try:
        producer = Producer(TEST_PRODUCER_CONFIG)

        for i in range(num_messages):
            task_message = serialize_task_activation(i, ["foobar"], {})
            producer.produce(topic_name, task_message)

        producer.flush()
        print(f"Sent {num_messages} generic messages to kafka topic {topic_name}")
    except Exception as e:
        raise Exception(f"Failed to send messages to kafka: {e}")


def send_custom_messages_to_topic(topic_name: str, custom_messages: list[TaskActivation]) -> None:
    """
    Send num_messages to kafka topic using unique task names.
    """
    try:
        producer = Producer(TEST_PRODUCER_CONFIG)

        for message in custom_messages:
            task_message = message.SerializeToString()
            producer.produce(topic_name, task_message)

        producer.flush()
        print(f"Sent {len(custom_messages)} custom messages to kafka topic {topic_name}")
    except Exception as e:
        raise Exception(f"Failed to send messages to kafka: {e}")


def get_topic_size(topic_name: str) -> int:
    """
    Creates a consumer and polls the topic starting at the earliest offset
    attempts are exhausted.
    """
    attempts = 30
    size = 0
    consumer = Consumer(TEST_CONSUMER_CONFIG)
    consumer.subscribe([topic_name])
    while attempts > 0:
        event = consumer.poll(1.0)
        if event is None:
            attempts -= 1
            continue
        if event.error():
            raise KafkaException(event.error())
        else:
            size += 1

    return size


def get_num_tasks_in_sqlite(taskbroker_config: TaskbrokerConfig) -> int:
    attach_db_stmt = (
        f"ATTACH DATABASE '{taskbroker_config.db_path}' " f"AS {taskbroker_config.db_name};\n"
    )
    query = (
        f"SELECT count(*) as count FROM " f"{taskbroker_config.db_name}.inflight_taskactivations;"
    )
    con = sqlite3.connect(taskbroker_config.db_path)
    cur = con.cursor()
    cur.executescript(attach_db_stmt)
    rows = cur.execute(query).fetchall()
    count = rows[0][0]
    return count


def get_num_tasks_in_sqlite_by_status(taskbroker_config: TaskbrokerConfig, status: str) -> int:
    attach_db_stmt = (
        f"ATTACH DATABASE '{taskbroker_config.db_path}' " f"AS {taskbroker_config.db_name};\n"
    )
    query = (
        f"SELECT count(*) as count FROM {taskbroker_config.db_name}."
        f"inflight_taskactivations WHERE status = '{status}';"
    )
    con = sqlite3.connect(taskbroker_config.db_path)
    cur = con.cursor()
    cur.executescript(attach_db_stmt)
    rows = cur.execute(query).fetchall()
    count = rows[0][0]
    return count


def get_num_tasks_group_by_status(
    taskbroker_config: TaskbrokerConfig,
) -> dict[str, int]:
    task_count_in_sqlite = {}
    attach_db_stmt = (
        f"ATTACH DATABASE '{taskbroker_config.db_path}' " f"AS {taskbroker_config.db_name};\n"
    )
    query = (
        f"SELECT status, count(id) as count FROM {taskbroker_config.db_name}."
        f"inflight_taskactivations GROUP BY status;"
    )
    con = sqlite3.connect(taskbroker_config.db_path)
    cur = con.cursor()
    cur.executescript(attach_db_stmt)
    rows = cur.execute(query).fetchall()
    for task_count in rows:
        task_count_in_sqlite[task_count[0]] = task_count[1]
    return task_count_in_sqlite


def get_available_ports(count: int) -> list[int]:
    MIN = 49152
    MAX = 65535
    res = []
    for i in range(count):
        for candidate in range(MIN + i, MAX, count):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(("0.0.0.0", candidate))
                    res.append(candidate)
                    break
            except Exception as e:
                print(f"Tried port: {candidate}: {e}")
    assert len(res) == count, "Not enough free ports"
    return res
