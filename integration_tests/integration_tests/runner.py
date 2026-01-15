import os

from integration_tests.worker import ConfigurableTaskWorker, TaskWorkerClient


def main() -> None:
    print("Starting worker")
    grpc_host = os.getenv("SENTRY_TASKWORKER_GRPC_HOST", "127.0.0.1")
    grpc_port = os.getenv("SENTRY_TASKWORKER_GRPC_PORT", "50051")
    namespace = os.getenv("SENTRY_TASKWORKER_NAMESPACE", "test")
    failure_rate = float(os.getenv("SENTRY_TASKWORKER_FAILURE_RATE", "0.0"))
    timeout_rate = float(os.getenv("SENTRY_TASKWORKER_TIMEOUT_RATE", "0.0"))

    if not (0 <= failure_rate <= 1) or not (0 <= timeout_rate <= 1):
        raise ValueError("Failure rate and timeout rate must be between 0 and 1")

    worker = ConfigurableTaskWorker(
        client=TaskWorkerClient(f"{grpc_host}:{grpc_port}"),
        namespace=namespace,
        failure_rate=failure_rate,
        timeout_rate=timeout_rate,
        enable_backoff=True,
    )

    task = None
    while True:
        if task is None:
            task = worker.fetch_task()

        if task:
            task = worker.process_task(task)


if __name__ == "__main__":
    main()
