# Kafka Queues (KIP-932) prototype (clients/kafka-queue)

Rough prototype: a **Java** gRPC service wraps `KafkaShareConsumer` (Kafka 4.2+); a **Python** package runs a small worker and talks to that bridge. **Not** for production: no real security, tests, or long-term support.

## Layout

- `proto/queue_bridge.proto` — gRPC contract
- `java/queue-bridge/` — Netty gRPC + Kafka share consumer
- `java/example/` — two slow manual-test consumers: `KafkaShareConsumer` and classic `KafkaConsumer` (1s between records; separate shadow JARs)
- `python/` — worker + bridge client (does not import `taskbroker_client`)
- `k8s/` — Deployments/Service/ConfigMap for `taskbroker-testing` namespace

## Protobuf + gRPC codegen

```bash
# One-time, from this directory (with grpcio-tools installed)
python3 -m pip install "grpcio-tools>=1.60" "protobuf>=5" --quiet

# Python stubs → clients/kafka-queue/python/src/kafka_queue_proto/
python3 -m grpc_tools.protoc \
  -I proto \
  --python_out=python/src/kafka_queue_proto \
  --pyi_out=python/src/kafka_queue_proto \
  --grpc_python_out=python/src/kafka_queue_proto \
  proto/queue_bridge.proto
```

The Java build runs protoc via Gradle; see `java/queue-bridge/`.

## Run locally (smoke)

1. Start a Kafka 4.2+ broker; create a topic (share-group / queue semantics as per your cluster; see [KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+KAFKA)).
2. Build the Java JAR, run the bridge (see `java/queue-bridge/README.md` env vars).
3. `pip install -e clients/kafka-queue/python/`, set `QUEUE_BRIDGE_ADDR=127.0.0.1:50060`, run the example worker, produce `TaskActivation` bytes to the topic (see `python/examples/`.

## Kubernetes

```bash
kubectl apply -f clients/kafka-queue/k8s/ -n taskbroker-testing
```

Edit `k8s/configmap.yaml` with your `KAFKA_BOOTSTRAP`, topic, and `QUEUE_BRIDGE` listen port. Build/push two images (JAR, Python) and set image names in the Deployments.

## Metrics (demo)

- **Java:** logs + optional Counters in `CheckResponse` / `PollResponse` (simple counters; expose via your APM or log scraper).
- **Python:** `logging` only in the reference-style metrics class.

## Non-goals

- TLS, auth, secrets, unit tests, comprehensive docs, HA bridge.

## Apply Kubernetes

```bash
# Edit k8s/configmap.yaml: KAFKA_BOOTSTRAP, topic, and image names in the Deployments.
kubectl apply -f k8s/ -n taskbroker-testing
```

Java image (context `clients/kafka-queue`):

```bash
docker run --rm -v "$PWD":/w -w /w/java/queue-bridge gradle:8.10.2-jdk17 gradle shadowJar
docker build -f java/queue-bridge/Dockerfile -t <registry>/queue-bridge:dev .
```

Python worker image:

```bash
docker build -f python/Dockerfile -t <registry>/kqueue-worker:dev .
```

## gRPC + protobuf (Python / Java)

- The Python package pins `grpcio<1.80` and `protobuf<6` so the checked-in stubs stay compatible with `sentry-protos` (see [python/README.md](python/README.md)). Regenerate the Python `kafka_queue_proto` from `proto/queue_bridge.proto` after changing the API.
