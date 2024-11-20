# Build image
# Note that it is important which debian image is used, because not all of them have a
# recent enough version of protobuf-compiler
FROM rust:1-bookworm as build

RUN apt-get update && apt-get upgrade -y 
RUN apt-get install -y cmake pkg-config libssl-dev librdkafka-dev protobuf-compiler

RUN USER=root cargo new --bin taskbroker
WORKDIR /taskbroker

# All these files are required to build or run the application
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
COPY ./migrations ./migrations
COPY ./config/config-dev.yaml ./config.yaml

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
