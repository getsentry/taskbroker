# Taskbroker client example application

This directory contains a sample application that is used by `taskbroker_client`
tests, and can be run locally as a minimal example client for taskbroker.

## Running the example application

First, install the required dependencies:

```bash
# Install the required and development dependencies
uv sync --dev

# Install the optional dependencies for the example application
uv sync --extra examples
```
Before running the examples, make sure you have:

- Kafka running
- Redis running (for the scheduler)
- Taskbroker running. Use `cargo run` in the repository root for this.

With all of those pre-requisites complete, you can run the example application:

```bash
# Generate 5 tasks
python src/examples/cli.py generate --count 5

# Run the scheduler which emits a task every 1m
python src/examples/cli.py scheduler

# Run the worker
python src/examples/cli.py worker
```

## Docker image

The example application also comes with a docker image that will contain the
example application and all required dependencies. This can be useful for end
to end testing changes to taskbroker.

### Building the image

```bash
cd clients/python
docker build -t taskbroker-example -f Dockerfile.example .
```

### Running the image

You'll need to have both Kafka running to produce tasks, and Redis
running to run a scheduler.

```bash
# Spawn one timed_task activation
docker run \
    -e KAFKA_HOST=kafka-host:9092 \
    -e REDIS_HOST=redis-host \
    taskbroker-example spawn --count=1

# Run a worker
docker run \
    -e KAFKA_HOST=kafka-host:9092 \
    -e REDIS_HOST=redis-host \
    taskbroker-example worker --rpc-host=127.0.0.1:50051
```
