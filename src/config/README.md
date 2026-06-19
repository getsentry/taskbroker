# Configuration

This directory contains the code that configures broker behavior. You might work in this directory if you...

- Are building a new feature the user needs to tune
- Need to change the type of a configuration field
- Need to remove unused configuration fields
- Check whether fields satisfy certain properties, like numbers being greater than zero

## Organization

We use nesting to keep components separate. This improves readability and encourages using options that don't cut across the entire application. Each nested configuration is named `ComponentConfig` where `Component` is a short, logical name for the associated component.

| Name               | Location   | Description |
| ------------------ | ---------- | ----------- |
| `BatchConfig`      | `batch.rs` | Settings for anything that uses batches. |
| `FetchConfig`      | `fetch.rs` | Fetch thread settings (count, batch length, and backoff). |
| `TopicConfig`      | `kafka.rs` | Kafka topic settings (cluster, consumer group, raw mode). |
| `ClusterConfig`    | `kafka.rs` | Kafka cluster settings (brokers, SASL, and TLS). |
| `PushConfig`       | `push.rs`  | Push thread settings (count, timeout, and so on). |
| `PushQueueConfig`  | `push.rs`  | Push queue settings (size and timeout). |
| `PushUpdateConfig` | `push.rs`  | Push status update settings (batching and flush behavior). |
| `RawModeConfig`    | `raw.rs`   | Raw mode settings (desired task namespace, application, and more). |
| `StoreConfig`      | `store.rs` | Activation store settings (adapter, batching, limits, retries, and deadlines). |
| `PgConfig`         | `store.rs` | Postgres activation store settings (host, credentials, query parameters). |
| `SqliteConfig`     | `store.rs` | SQLite activation store settings (path, metrics, and vacuuming). |
| `RetryConfig`      | `store.rs` | Database query retry settings (attempt count and delay). |

Most of these are nested inside `Config`, which is provided by the user using YAML and environment variables. If provided, the YAML file is loaded first. It is followed by `TASKBROKER_` environment variables, which take precedence.

Deprecated configuration options are stored in `DeprecatedConfig`, discussed later.

## Adding New Options

Adding new configuration options is straightforward.

First, **determine which component(s) will use your new option**. For example, if your option is related to push threads, it should probably go inside `PushConfig`.

Then, add the field to the appropriate configuration struct and **add a reasonable default value**. Brokers and workers must work on your local machine using only the defaults!

### Serialization and Deserialization

We use the `serde` crate, which allows you to use custom serializers and deserializers as follows.

```rs
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
struct ExampleConfig {
    #[serde(with = "crate::serde::duration")]
    pub period: Duration
}
```

If the option represents a duration of time, use the `Duration` type and the custom serializer / deserializer written in `src/serde.rs`. Right now, it takes a string like "123ms" or "20s" and turns it into a `Duration`.

This attribute expects a _module_ with public `serialize` and `deserialize` functions. Please refer to the `serde` documentation and our duration example when creating new serializers.

### Validation

Finally, **consider whether your option requires validation**. Suppose it is a number. Will the system still work if the user provides...

- Zero?
- A negative number?
- An improperly formatted string?

We use the `validator` crate to easily add validation via attributes. For example, here is how you can make sure the field is between 1 and 100.

```rs
use validator::Validate;

#[derive(Validate)]
struct ExampleConfig {
    #[validate(range(min = 1, max = 100))]
    pub number: i32
}
```

You may need to use or create custom validators. Right now, there are two located in `validate.rs`.

- `validate::power_of_two` ensures the provided number is a power of two
- `validate::nonzero_duration` ensures the provided duration is greater than zero

If you need to write a validator for a field of type `T`, please use one of the above as a template.

## Changing Options

Changing an existing option requires more attention because it must **be backwards compatible**. Rolling out configuration changes cannot break existing deployments! Right now, these guidelines only apply to fields in `Config`. In the future, we need to decide how nested options should be safely deprecated.

First, **move the field** to `DeprecatedConfig` in `deprecated.rs` and wrap the type with an `Option`. If it was already optional, keep it the same. Remove any validators as those will go on the new version of the field.

Then, **add the new version of the field** to the appropriate struct. Here, follow the same steps as for adding new options.

Finally, **map deprecated fields to their newer counterparts** using the `deprecated::map` macro or custom logic if needed. Feel free to modify the macro if it doesn't suit your needs.

Note that there are only two versions of the configuration at any given moment - the current version (which is already deployed or will be deployed soon), and the previous version (which may still be deployed). Once it is guaranteed that nobody is using the old configuration options, they may be deleted.

## Removing Options

Deleting configuration fields is very simple. Just make sure it isn't controlling anything important and remove!

## Adding Nested Configuration

As the codebase evolves, it may become necessary to introduce more nesting and new configuration structures.

First, **determine what module should contain the new structure**. The modules in this directory (configuration modules) should roughly mirror the application modules.

- If the struct configures an existing feature, it should go in the module for that feature. For example, configuration used in `src/push.rs` should go in `config/push.rs`.
- If the struct configures a new feature, create a new configuration module for that feature. For example, if you created `src/foo.rs`, there should be a corresponding `config/foo.rs` and a `FooConfig`.

Please implement `Default` for the new struct **explicitly** with an `impl Default for FooConfig` block. This makes it obvious what the defaults are and causes compiler errors when the configuration is changed in a way that doesn't align with the defaults (which could silently break existing deployments otherwise.)
