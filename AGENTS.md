# Taskbroker Development Guide for Agents

## Overview
Taskbroker is Sentry's distributed asynchronous task execution system. It consists of taskworkers (an application that executes tasks) and taskbrokers (an application that distributes tasks across workers).

## Formatting
Run `make format` before you commit. There are some other conventions you should follow that aren't enforced by that command, described below.

### Rust Imports

#### Flatten Imports
Do not nest braced groups inside other braced groups. Use only one level of nesting per `use` statement. Here is an example.

```rs
use a::{ b::{c, d}, e::{f, g} }
```

Breaking apart this single statement into two improves readability.

```rs
use a::b::{c, d};
use a::e::{f, g};
```

#### Group Imports

Grouping is optional if there are **fewer than four** `use` lines. If there are **four or more** use lines, separate groups with a blank line in this order.

  1. Standard library (`std::`)
  2. External crates
  3. Internal modules (`crate::`, `taskbroker::`)
  4. Super modules (`super::`)

If a single external crate needs **more than three** `use` lines, put that crate in its own group before other external crates.

The first block below is **wrong on purpose** (mixed order and no blank lines).

```rs
use crate::store::adapters::postgres::{PostgresActivationStore, PostgresActivationStoreConfig};
use crate::store::traits::InflightActivationStore;
use std::sync::Arc;
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::FutureProducer;
use anyhow::{Context, Result};
use std::time::Duration;
```

The second block shows the correct layout.

```rs
use std::sync::Arc;
use std::time::Duration;

use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::FutureProducer;

use anyhow::{Context, Result};

use crate::store::adapters::postgres::{PostgresActivationStore, PostgresActivationStoreConfig};
use crate::store::traits::InflightActivationStore;
```
