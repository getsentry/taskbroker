# Build image
# Note that it is important which debian image is used, because not all of them have a
# recent enough version of protobuf-compiler
FROM rust:1-bookworm AS build

RUN apt-get update && apt-get upgrade -y 
RUN apt-get install -y cmake pkg-config libssl-dev librdkafka-dev protobuf-compiler

RUN USER=root cargo new --bin taskbroker
WORKDIR /taskbroker

# This is set by the cloudbuild.yaml file
ARG TASKBROKER_GIT_REVISION
ENV TASKBROKER_VERSION=$TASKBROKER_GIT_REVISION

# All these files are required to build or run the application
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
COPY ./migrations ./migrations
COPY ./benches ./benches

# Build dependencies in a way they can be cached
RUN cargo build --release
RUN rm src/*.rs

# Copy source tree
COPY ./src ./src

# Build the main binary
RUN rm ./target/release/deps/taskbroker*
RUN cargo build --release

RUN echo "${TASKBROKER_VERSION}" > ./VERSION

# Runtime image
FROM rust:1-bookworm

# Necessary for libssl bindings
RUN apt-get update && apt-get upgrade -y && apt-get install -y libssl-dev

EXPOSE 50051

# For the sqlite to be mounted too
RUN mkdir /opt/sqlite

# Import the built binary and config file and run it
COPY --from=build /taskbroker/VERSION /opt/VERSION
COPY --from=build /taskbroker/target/release/taskbroker /opt/taskbroker

WORKDIR /opt

CMD ["/opt/taskbroker"]

# To build and run locally:
# docker build -t taskbroker --no-cache . && docker rm taskbroker && docker run --name taskbroker -p 127.0.0.1:50051:50051 -e TASKBROKER_KAFKA_CLUSTER=sentry_kafka:9093 --network sentry  taskbroker
