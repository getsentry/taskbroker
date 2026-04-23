# queue-bridge (Java)

- **Kafka 4.2+** `KafkaShareConsumer`, explicit share acknowledgement.
- gRPC: `QueueBridge` (`Poll`, `Complete`, `Check`).

## Build (Docker, no local Gradle)

From `clients/kafka-queue`:

```bash
docker run --rm -v "$(pwd)":/w -w /w/java/queue-bridge gradle:8.10.2-jdk17 \
  gradle --no-daemon clean shadowJar
ls java/queue-bridge/build/libs/queue-bridge-all.jar
```

## Run locally

```bash
export KAFKA_BOOTSTRAP=localhost:9092
export KAFKA_GROUP_ID=kqueue-demo
export KAFKA_TOPIC=your-kqueue-topic
export GRPC_LISTEN_PORT=50060
java -jar build/libs/queue-bridge-all.jar
```

## Env

| Variable            | Description                          |
| ------------------ | ------------------------------------ |
| `KAFKA_BOOTSTRAP`  | Required, brokers                    |
| `KAFKA_GROUP_ID`   | Required, share group id            |
| `KAFKA_TOPIC`      | Required                            |
| `GRPC_LISTEN_PORT` | Default `50060`                    |
| `KAFKA_MAX_POLL`   | Optional, default `32`             |
| `KQUEUE_BRIDGE_LOG_EMPTY_EVERY` | Optional, default `20`. Throttle for **empty** `consumer.poll()` lines: log the first, then every Nth empty result. Set to `0` to disable, `1` to log every empty poll (very noisy; about one line per Python Poll RPC). |

`BridgeService` and `ShareBridgeCore` also log to `java.util.logging` (INFO) on each gRPC **Poll/Complete** and on each record delivered or acked. The JVM startup adds a `StreamHandler` to **stdout** (when the root logger has no handlers) so the bridge app lines are not mixed with Kafka’s stderr stream—on GKE, logging **stderr** as `ERROR` is a common false signal for INFO lines.

**Share groups:** The broker rejects `auto.offset.reset` and `enable.auto.commit` in the client (Kafka 4.2 `ShareConsumerConfig`); the bridge does **not** set them. Do not add them in your own env or `ConsumerConfig` overrides.

Payload values must be serialized `TaskActivation` (protobuf) bytes (what the Python worker unparses).

See also `../README.md` and the Python package.
