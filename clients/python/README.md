# Taskbroker python client

This package provides python client libraries for taskbroker. The client libraries help with:

- Defining tasks
- Spawning tasks
- Running periodic schedulers
- Running workers

## Example application

The `src/examples` directory contains a sample application that is used in tests, and can be run locally. First
install the required dependencies:

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
python src/examples/cli.py scheduler
```
