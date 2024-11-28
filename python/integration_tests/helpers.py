import subprocess
from pathlib import Path

TASKBROKER_ROOT = Path(__file__).parent.parent.parent
TASKBROKER_BIN = TASKBROKER_ROOT / "target/debug/taskbroker"
TESTS_OUTPUT_PATH = Path(__file__).parent / ".tests_output"


def check_topic_exists(topic_name: str):
    # Checks if a topic exists in sentry_kafka
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


def create_topic(topic_name: str, num_partitions: int):
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


def update_topic_partitions(topic_name: str, num_partitions: int):
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
        pass


def send_messages_to_kafka(num_messages: int):
    # if sentry_path exists in environment, use sentry-cli to send messages to kafka
    pass