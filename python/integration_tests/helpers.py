import orjson
import subprocess
import sqlite3
import time

from confluent_kafka import Producer
from pathlib import Path
from uuid import uuid4
from sentry_protos.taskbroker.v1.taskbroker_pb2 import (
    OnAttemptsExceeded,
    RetryState,
    TaskActivation,
)
from google.protobuf.timestamp_pb2 import Timestamp

TASKBROKER_ROOT = Path(__file__).parent.parent.parent
TASKBROKER_BIN = TASKBROKER_ROOT / "target/debug/taskbroker"
TESTS_OUTPUT_ROOT = Path(__file__).parent / ".tests_output"


class ConsumerConfig:
    def __init__(
        self,
        db_name: str,
        db_path: str,
        max_pending_count: int,
        kafka_topic: str,
        kafka_consumer_group: str,
        kafka_auto_offset_reset: str,
        grpc_port: int
    ):
        self.db_name = db_name
        self.db_path = db_path
        self.max_pending_count = max_pending_count
        self.kafka_topic = kafka_topic
        self.kafka_consumer_group = kafka_consumer_group
        self.kafka_auto_offset_reset = kafka_auto_offset_reset
        self.grpc_port = grpc_port

    def to_dict(self) -> dict:
        return {
            "db_name": self.db_name,
            "db_path": self.db_path,
            "max_pending_count": self.max_pending_count,
            "kafka_topic": self.kafka_topic,
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


def serialize_task_activation(i, args: list, kwargs: dict) -> bytes:
    retry_state = RetryState(
        attempts=0,
        max_attempts=1,
        on_attempts_exceeded=OnAttemptsExceeded.ON_ATTEMPTS_EXCEEDED_DISCARD,
    )
    pending_task_payload = TaskActivation(
        id=uuid4().hex,
        namespace="integration_tests",
        taskname=f"integration_tests.say_hello_{i}",
        parameters=orjson.dumps({"args": args, "kwargs": kwargs}),
        retry_state=retry_state,
        processing_deadline_duration=3000,
        received_at=Timestamp(seconds=int(time.time())),
    ).SerializeToString()

    return pending_task_payload


def send_messages_to_kafka(topic_name: str, num_messages: int) -> None:
    """
    Send num_messages to kafka topic using unique task names.
    """
    try:
        producer = Producer(
            {
                "bootstrap.servers": "127.0.0.1:9092",
                "broker.address.family": "v4",
            }
        )

        for i in range(num_messages):
            task_message = serialize_task_activation(i, ["foobar"], {})
            producer.produce(topic_name, task_message)

        producer.poll(5)  # trigger delivery reports
        producer.flush()
        print(f"Sent {num_messages} messages to kafka topic {topic_name}")
    except Exception as e:
        raise Exception(f"Failed to send messages to kafka: {e}")


def check_num_tasks_written(consumer_config: ConsumerConfig) -> int:
    attach_db_stmt = f"ATTACH DATABASE '{consumer_config.db_path}' AS {consumer_config.db_name};\n"
    query = f"""SELECT count(*) as count FROM {consumer_config.db_name}.inflight_taskactivations;"""
    con = sqlite3.connect(consumer_config.db_path)
    cur = con.cursor()
    cur.executescript(attach_db_stmt)
    rows = cur.execute(query).fetchall()
    count = rows[0][0]
    return count
