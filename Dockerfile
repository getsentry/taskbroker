# Build image
FROM cgr.dev/chainguard/rust:latest-dev AS build

# The -dev variant includes package manager and shell for building
# Switch to root to install packages
USER root
# Use correct package names for Wolfi
RUN apk add cmake pkgconf openssl-dev librdkafka-dev protobuf protobuf-dev

# Find where protoc is located and set the environment variable
RUN which protoc || echo "protoc not found"
ENV PROTOC=/usr/bin/protoc

RUN USER=root cargo new --bin taskbroker
WORKDIR /taskbroker

# This is set by the cloudbuild.yaml file
ARG TASKBROKER_GIT_REVISION
ARG CONFIG_FILE=config-sentry-dev.yaml

ENV TASKBROKER_VERSION=$TASKBROKER_GIT_REVISION

# All these files are required to build or run the application
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
COPY ./migrations ./migrations
COPY ./config/${CONFIG_FILE} ./config.yaml
COPY ./benches ./benches

# Build dependencies in a way they can be cached
RUN cargo build --release
# RUN rm src/*.rs

# Copy source tree
COPY ./src ./src

# Build the main binary
# RUN rm ./target/release/deps/taskbroker*
RUN cargo build --release

RUN echo "${TASKBROKER_VERSION}" > ./VERSION

# Create directory for sqlite
RUN mkdir -p /opt/sqlite

# Runtime image - use minimal image with only required libraries
FROM cgr.dev/chainguard/glibc-dynamic:latest

# Expose the service port
EXPOSE 50051

# Import the built binary and config file
COPY --from=build /taskbroker/VERSION /opt/VERSION
COPY --from=build /taskbroker/config.yaml /opt/config.yaml
COPY --from=build /taskbroker/target/release/taskbroker /opt/taskbroker

WORKDIR /opt

# You can switch back to non-root user for better security if desired
USER nonroot

CMD ["/opt/taskbroker", "--config", "/opt/config.yaml"]

# To build and run locally:
# docker build -t taskbroker --no-cache . && docker rm taskbroker && docker run --name taskbroker -p 127.0.0.1:50051:50051 -e TASKBROKER_KAFKA_CLUSTER=sentry_kafka:9093 --network sentry taskbroker
