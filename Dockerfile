# Build image
# Note that it is important which debian image is used, because not all of them have a
# recent enough version of protobuf-compiler
FROM ghcr.io/getsentry/image-mirror-library-rust:1.96.1-trixie AS build

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y cmake pkg-config libssl-dev librdkafka-dev protobuf-compiler

RUN USER=root cargo new --bin taskbroker
WORKDIR /taskbroker

# All these files are required to build or run the application
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
COPY ./migrations ./migrations
COPY ./benches ./benches
COPY ./.cargo ./.cargo

# Build dependencies in a way they can be cached
RUN cargo build --release
RUN rm src/*.rs

# Copy source tree
COPY ./src ./src

# Build the main binary
RUN rm ./target/release/deps/taskbroker*
RUN cargo build --release

# Declared after the dependency build so a new revision on every commit
# doesn't invalidate the cached dependency layers.
ARG TASKBROKER_GIT_REVISION
ENV TASKBROKER_VERSION=$TASKBROKER_GIT_REVISION

RUN echo "${TASKBROKER_VERSION}" > ./VERSION

# /opt for the distroless image. The base has no shell to mkdir or chown, and it
# already has a root-owned /opt, so we prepare the tree here and copy it onto /.
# The directories are owned by nonroot (uid 65532) so the default SQLite path,
# relative to /opt, is writable.
FROM build AS distroless-opt

RUN mkdir -p /distroless/opt/sqlite \
    && cp ./VERSION ./target/release/taskbroker /distroless/opt/ \
    && chown 65532:65532 /distroless/opt /distroless/opt/sqlite

# Distroless runtime: no shell or package manager. OpenSSL and zlib are linked
# statically and SQLite is bundled, so the binary only needs glibc. The base
# runs as nonroot (uid 65532) and ships the CA bundle.
FROM us-docker.pkg.dev/sentryio/dhi-mirror/static:20250419-glibc-debian13 AS taskbroker-distroless

EXPOSE 50051

COPY --from=distroless-opt /distroless/ /

WORKDIR /opt

CMD ["/opt/taskbroker"]

# Debian runtime. Keep it the last stage: builds without --target produce this image.
FROM debian:trixie-slim AS taskbroker

RUN groupadd taskbroker --gid 1000 && useradd --gid taskbroker --uid 1000 taskbroker

# Necessary for libssl bindings
RUN apt-get update && apt-get upgrade -y && apt-get install -y libssl-dev libsqlite3-dev libsqlite3-0 openssl ca-certificates

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
