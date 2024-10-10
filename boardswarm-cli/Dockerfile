## stage 1: builder
FROM rust:slim-bookworm AS builder
ARG BINARY
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install --yes \
      pkg-config \
      protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/boardswarm

COPY . .

RUN cargo build \
      --release \
      --bin boardswarm-cli

## stage 2: runner
FROM debian:bookworm-slim
ARG BINARY
ARG DEBIAN_FRONTEND=noninteractive
ENV RUST_LOG=info

LABEL org.opencontainers.image.title="boardswarm-cli"
LABEL org.opencontainers.image.description="gRPC client for boardswarm used for interacting with development boards"
LABEL org.opencontainers.image.source="https://github.com/boardswarm/boardswarm"

COPY --from=builder /usr/src/boardswarm/target/release/boardswarm-cli \
                    /usr/local/bin/boardswarm-cli

# TODO: add config file
ENTRYPOINT ["/usr/local/bin/boardswarm-cli"]
