# syntax=docker/dockerfile:1

FROM rust:1-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
RUN cargo build --release --bins

FROM debian:bookworm-slim AS runtime

# P0 readiness checks use `nc -z`. Do not replace this with the planned
# `raft-probe` until the GetStatus RPC and that binary have been implemented.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/rust-raft-log /usr/local/bin/rust-raft-log
COPY --from=builder /app/target/release/raft-client /usr/local/bin/raft-client
COPY --from=builder /app/target/release/raft-node-test /usr/local/bin/raft-node-test

# P1 patch, once `raft-probe` exists:
# COPY --from=builder /app/target/release/raft-probe /usr/local/bin/raft-probe

ENTRYPOINT ["rust-raft-log"]
