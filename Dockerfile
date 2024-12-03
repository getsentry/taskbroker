# Build image
# Note that it is important which debian image is used, because not all of them have a
# recent enough version of protobuf-compiler
FROM rust:1-bookworm as build

RUN apt-get update && apt-get upgrade -y 
RUN apt-get install -y cmake pkg-config libssl-dev librdkafka-dev protobuf-compiler

RUN USER=root cargo new --bin taskbroker
WORKDIR /taskbroker

ARG config_file=config-sentry-dev.yaml

# All these files are required to build or run the application
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
COPY ./migrations ./migrations
COPY ./config/${config_file} ./config.yaml

# This is set by the cloudbuild.yaml file
ARG TASKWORKER_GIT_REVISION=""
ENV TASKWORKER_GIT_REVISION=${TASKWORKER_GIT_REVISION}
RUN echo "${TASKWORKER_GIT_REVISION}" > ./src/.VERSION

# Build dependencies in a way they can be cached
RUN cargo build --release
RUN rm src/*.rs

# Copy source tree
COPY ./src ./src

# Build the main binary
RUN rm ./target/release/deps/taskbroker*
RUN cargo build --release

COPY ./src/.VERSION ./.VERSION

# Runtime image
FROM debian:bookworm-slim

# Necessary fot libssl bindings
RUN apt-get update && apt-get upgrade -y && apt-get install -y libssl-dev

EXPOSE 50051

# Import the built binary and config file and run it
COPY --from=build /taskbroker/.VERSION /opt/.VERSION
COPY --from=build /taskbroker/config.yaml /opt/config.yaml
COPY --from=build /taskbroker/target/release/taskbroker /opt/taskbroker
ENTRYPOINT ["/opt/taskbroker"]
CMD ["--config", "/opt/config.yaml"]

# For devservices:
# docker build -t taskbroker --no-cache . && docker rm taskbroker && docker run --name taskbroker -p 127.0.0.1:50051:50051 -e TASKBROKER_KAFKA_CLUSTER=sentry_kafka:9093 --network sentry  taskbroker
