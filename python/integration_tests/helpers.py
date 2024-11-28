import subprocess
from os import environ
from pathlib import Path

TASKBROKER_ROOT = Path(__file__).parent.parent.parent
TASKBROKER_BIN = TASKBROKER_ROOT / "target/debug/taskbroker"
TESTS_OUTPUT_PATH = Path(__file__).parent / ".tests_output"


def check_topic_exists(topic_name: str) -> bool:
    try:
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

        return topic_name in topics
    except Exception as e:
        raise Exception(f"Failed to check if topic exists: {e}")


def create_topic(topic_name: str, num_partitions: int) -> None:
    try:
        create_topic_cmd = [
            "docker",
            "exec",
            "sentry_kafka",
            "kafka-topics",
            "--bootstrap-server",
            "localhost:9092",
            "--create",
            "--topic",
            topic_name,
            "--partitions",
            str(num_partitions),
            "--replication-factor",
            "1",
        ]
        subprocess.run(create_topic_cmd, check=True)
    except Exception as e:
        raise Exception(f"Failed to create topic: {e}")


def update_topic_partitions(topic_name: str, num_partitions: int) -> None:
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
            topic_name,
            "--partitions",
            str(num_partitions),
        ]
        subprocess.run(create_topic_cmd, check=True)
    except Exception:
        # Command fails topic already has the correct number of partitions. Try to continue.
        pass


def send_messages_to_kafka(num_messages: int) -> None:
    path_to_sentry = environ.get("SENTRY_PATH")
    if not path_to_sentry:
        raise Exception("SENTRY_PATH not set in environment. This is required to send messages to kafka")

    try:
        send_tasks_cmd = [
            "cd",
            path_to_sentry,
            "&&",
            "sentry",
            "run",
            "taskbroker-send-tasks",
            "--task-function-path",
            "sentry.taskworker.tasks.examples.say_hello",
            "--args",
            "'[\"foobar\"]'",
            "--repeat",
            str(num_messages),
        ]
        subprocess.run(send_tasks_cmd, check=True)
        print(f"Sent {num_messages} messages to kafka")
    except Exception as e:
        raise Exception(f"Failed to send messages to kafka: {e}")
