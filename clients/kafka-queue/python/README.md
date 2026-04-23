# kafka-queue-worker (Python)

- Runs `TaskQueueWorker` → gRPC to the Java `queue-bridge` → `KafkaShareConsumer` on a topic of `TaskActivation` byte payloads.
- Does **not** use `taskbroker_client`.

## Install

```bash
cd clients/kafka-queue/python
python -m venv .venv
source .venv/bin/activate
pip install -e .
kqueue --help
```

`protobuf<6` is required to match `sentry-protos`. The checked-in `kafka_queue_proto/*` was hand-edited to drop a protoc-6+ runtime check; see comments in `queue_bridge_pb2.py` if you regenerate with a newer `grpc_tools` while keeping `protobuf<6`.

`grpcio` is pinned below 1.80. If you regenerate `queue_bridge_pb2_grpc.py` with a newer `grpcio-tools`, either use a plugin version that matches the runtime `grpcio` or set `GRPC_GENERATED_VERSION` in that file to match (the repo pins it to `1.67.0` so 1.67–1.79 runtimes import cleanly).

## Run the demo worker (bridge must be up)

```bash
export QUEUE_BRIDGE_ADDR=127.0.0.1:50060
kqueue worker
```

## Docker

From `clients/kafka-queue` with the package installed in the image: see `../Dockerfile` in this directory (context is the parent `kafka-queue` tree).

## Topic / producer

Produce to `KAFKA_TOPIC` **protobuf** `TaskActivation` serialized to the record **value** (e.g. from your Sentry app or a one-off script). The bridge and worker do not create the topic; configure the cluster for [KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+KAFKA) as needed.
