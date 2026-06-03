# Migrating Kafka config to `kafka_clusters` / `kafka_topics`

Taskbroker historically configured Kafka with a set of flat fields describing a
single consumed cluster plus a dead-letter cluster. As of #663 these are
deprecated in favor of two maps:

- `kafka_clusters` — named clusters, each with an address and optional auth.
- `kafka_topics` — named topics, each pointing at a cluster.

The legacy fields still work (they are normalized into the maps at startup, and
emit a deprecation `warn!`), but the two formats are **mutually exclusive** —
you cannot mix a deprecated field with `kafka_clusters`/`kafka_topics` in the
same config.

## Field mapping

| Legacy field | New location |
| --- | --- |
| `kafka_cluster` | `kafka_clusters.<name>.address` |
| `kafka_topic` | a key under `kafka_topics` (the one consumable topic) |
| `kafka_consumer_group` | `kafka_topics.<topic>.consumer_group` |
| `kafka_security_protocol` | `kafka_clusters.<name>.security_protocol` |
| `kafka_sasl_mechanism` | `kafka_clusters.<name>.sasl_mechanism` |
| `kafka_sasl_username` | `kafka_clusters.<name>.sasl_username` |
| `kafka_sasl_password` | `kafka_clusters.<name>.sasl_password` |
| `kafka_ssl_ca_location` | `kafka_clusters.<name>.ssl_ca_location` |
| `kafka_ssl_certificate_location` | `kafka_clusters.<name>.ssl_certificate_location` |
| `kafka_ssl_key_location` | `kafka_clusters.<name>.ssl_key_location` |
| `kafka_deadletter_cluster` | a separate cluster's `address` |
| `kafka_deadletter_security_protocol` (and other `kafka_deadletter_*` auth) | auth on the dead-letter cluster |
| `kafka_deadletter_topic` | **not deprecated** — still a top-level field; must name a `produce_only` topic declared in `kafka_topics` |
| `kafka_retry_topic` | **not deprecated** — still a top-level field; must name a `produce_only` topic declared in `kafka_topics` |

Raw-mode fields move under the consumed topic's `raw:` block:

| Legacy field | New location |
| --- | --- |
| `raw_mode` | presence of a `raw:` block on the topic |
| `raw_namespace` | `kafka_topics.<topic>.raw.namespace` |
| `raw_application` | `kafka_topics.<topic>.raw.application` |
| `raw_taskname` | `kafka_topics.<topic>.raw.taskname` |
| `raw_processing_deadline_duration` | `kafka_topics.<topic>.raw.processing_deadline_duration` |

The top-level `raw_*` fields are deprecated; the legacy global `raw_mode` is
migrated onto the consumed topic's `raw` block during normalization.

### Removed

- `kafka_consume_retry_topic` was removed. Retry topics are declared like any
  other topic in `kafka_topics`; to consume one, declare it as a normal
  (non-`produce_only`) consumed topic rather than toggling a flag.

## Rules enforced at startup

- At least one topic must be consumable (i.e. not `produce_only`).
- `kafka_deadletter_topic` must be declared in `kafka_topics`.
- `kafka_retry_topic` is **mandatory when a consumed topic uses raw mode**: raw
  messages aren't activations, so retries (which are activation-encoded) cannot
  loop back into the raw topic and must go to a separate activation-encoded
  topic. For a single non-raw consumed topic, retries fall back to that topic
  when `kafka_retry_topic` is unset.
- The retry target and the dead-letter topic must resolve to the **same cluster
  address** — they share a single upkeep producer.
- Every topic's `cluster` must reference a cluster defined in `kafka_clusters`.

## Examples

### Before (legacy, single cluster)

```yaml
kafka_cluster: 127.0.0.1:9092
kafka_topic: taskworker
kafka_consumer_group: taskworker
kafka_deadletter_topic: taskworker-dlq
kafka_retry_topic: taskworker-retry
```

### After

```yaml
kafka_deadletter_topic: taskworker-dlq
kafka_retry_topic: taskworker-retry

kafka_clusters:
  default:
    address: 127.0.0.1:9092

kafka_topics:
  taskworker:
    cluster: default
    consumer_group: taskworker
  taskworker-retry:
    cluster: default
    consumer_group: taskworker
    produce_only: true
  taskworker-dlq:
    cluster: default
    consumer_group: taskworker
    produce_only: true
```

### Before (separate dead-letter cluster, with auth)

```yaml
kafka_cluster: main-brokers:9092
kafka_topic: profiles
kafka_consumer_group: taskbroker-profiles
kafka_security_protocol: sasl_ssl
kafka_sasl_mechanism: scram-sha-256
kafka_sasl_username: main-user
kafka_sasl_password: main-pass

kafka_deadletter_cluster: dlq-brokers:9092
kafka_deadletter_topic: profiles-dlq
kafka_retry_topic: profiles-retry
```

### After

The retry and dead-letter topics share the upkeep producer, so they must sit on
the same cluster (`dlq-brokers` here):

```yaml
kafka_deadletter_topic: profiles-dlq
kafka_retry_topic: profiles-retry

kafka_clusters:
  main:
    address: main-brokers:9092
    security_protocol: sasl_ssl
    sasl_mechanism: scram-sha-256
    sasl_username: main-user
    sasl_password: main-pass
  deadletter:
    address: dlq-brokers:9092

kafka_topics:
  profiles:
    cluster: main
    consumer_group: taskbroker-profiles
  profiles-retry:
    cluster: deadletter
    consumer_group: taskbroker-profiles
    produce_only: true
  profiles-dlq:
    cluster: deadletter
    consumer_group: taskbroker-profiles
    produce_only: true
```

### Raw mode (per-topic)

Before — raw mode configured via the global `raw_*` fields:

```yaml
kafka_cluster: 127.0.0.1:9092
kafka_topic: profiles
kafka_consumer_group: ingest-profiles
raw_mode: true
raw_namespace: ingest.profiling.passthrough
raw_application: sentry
raw_taskname: sentry.profiles.task.process_profile_from_kafka
```

After — raw mode moves onto the consumed topic's `raw` block, and a retry topic
becomes mandatory (raw messages aren't activations, so retries can't loop back
into the raw topic). Here retries go to the `taskworker` topic, which another
taskbroker consumes:

```yaml
kafka_deadletter_topic: taskworker-dlq
kafka_retry_topic: taskworker

kafka_clusters:
  default:
    address: 127.0.0.1:9092

kafka_topics:
  profiles:
    cluster: default
    consumer_group: ingest-profiles
    raw:
      namespace: ingest.profiling.passthrough
      application: sentry
      taskname: sentry.profiles.task.process_profile_from_kafka
      processing_deadline_duration: 30
  taskworker:
    cluster: default
    consumer_group: ingest-profiles
    produce_only: true
  taskworker-dlq:
    cluster: default
    consumer_group: ingest-profiles
    produce_only: true
```

## Environment variables

Both formats are also settable via `TASKBROKER_`-prefixed env vars, with `__` as
the nesting separator:

```
TASKBROKER_KAFKA_CLUSTERS__DEFAULT__ADDRESS=127.0.0.1:9092
TASKBROKER_KAFKA_TOPICS__TASKWORKER__CLUSTER=default
TASKBROKER_KAFKA_TOPICS__TASKWORKER__CONSUMER_GROUP=taskworker
```

Note: figment lowercases each key segment after splitting on `__`, so topic and
cluster **names set via env vars can only contain underscores, not hyphens**
(e.g. `taskworker_dlq`, not `taskworker-dlq`). The `kafka_deadletter_topic`
value must then match that lowercased key. For names with hyphens, use a YAML
config file instead.
