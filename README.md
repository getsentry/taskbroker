# Taskbroker

taskbroker provides the Kafka consumer, RPC interface, and task storage that are
the future heart of asynchronous task execution at sentry.

Task Producers and Workers can be found in `sentry.taskworker`. The sentry API
server and taskbroker communicate using protobufs. Tasks are serialized into
protobuf messages and appended to Kafka topics by sentry. From there, taskbroker
consumes from the Kafka topic and stores tasks into a small SQLite database. The
SQLite database enables workers to competitively consume from the Kafka topic
enabling out of order message processing and per message awknowledgements.
