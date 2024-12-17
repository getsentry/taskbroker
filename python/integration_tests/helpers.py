import orjson
import subprocess
import time

from confluent_kafka import Producer
from pathlib import Path
from uuid import uuid4
from sentry_protos.sentry.v1.taskworker_pb2 import RetryState, TaskActivation
from google.protobuf.timestamp_pb2 import Timestamp

TASKBROKER_ROOT = Path(__file__).parent.parent.parent
TASKBROKER_BIN = TASKBROKER_ROOT / "target/debug/taskbroker"


def check_topic_exists(topic_name: str) -> bool:
    try:
        check_topic_cmd = [
            "docker",
            "exec",
            "kafka-kafka-1",
            "kafka-topics",
            "--bootstrap-server",
            "localhost:9092",
            "--list",
        ]
        result = subprocess.run(check_topic_cmd, check=True, capture_output=True, text=True)
        topics = result.stdout.strip().split("\n")

        return topic_name in topics
    except Exception as e:
        raise Exception(f"Failed to check if topic exists: {e}")


def create_topic(topic_name: str, num_partitions: int) -> None:
    try:
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
            str(num_partitions)
        ]
        subprocess.run(create_topic_cmd, check=True)
    except Exception as e:
        raise Exception(f"Failed to create topic: {e}")


def recreate_topic(topic_name: str, num_partitions: int) -> None:
    # Delete and recreate a Kafka topic to ensure a clean state.
    try:
        delete_topic_cmd = [
            "docker",
            "exec",
            "kafka-kafka-1",
            "kafka-topics",
            "--bootstrap-server",
            "localhost:9092",
            "--delete",
            "--topic",
            topic_name
        ]
        subprocess.run(
            delete_topic_cmd,
            check=True
        )

        time.sleep(3)
        create_topic(topic_name, num_partitions)
    except Exception as e:
        raise Exception(f"Failed to recreate topic: {e}")


def serialize_task_activation(args: list, kwargs: dict) -> bytes:
    retry_state = RetryState(
        attempts=0,
        kind="sentry.taskworker.retry.Retry",
        discard_after_attempt=None,
        deadletter_after_attempt=None,
    )
    pending_task_payload = TaskActivation(
        id=uuid4().hex,
        namespace="integration_tests",
        taskname="integration_tests.say_hello",
        parameters=orjson.dumps({"args": args, "kwargs": kwargs}),
        retry_state=retry_state,
        received_at=Timestamp(seconds=int(time.time())),
    ).SerializeToString()

    return pending_task_payload


def send_messages_to_kafka(topic_name: str, num_messages: int) -> None:
    try:
        producer = Producer({
            'bootstrap.servers': '127.0.0.1:9092',
            'broker.address.family': 'v4',
        })

        for _ in range(num_messages):
            task_message = serialize_task_activation(["foobar"], {})
            producer.produce(topic_name, task_message)

        producer.poll(5)  # trigger delivery reports
        producer.flush()
        print(f"Sent {num_messages} messages to kafka topic {topic_name}")
    except Exception as e:
        raise Exception(f"Failed to send messages to kafka: {e}")
