# syntax=docker/dockerfile:1
# =============================================================================
# usenet-feeder — Usenet feeder sidecar over an nntmux header-scan catalog.
# Multi-stage build.
#
# Build context must be this repo root (packages/plugins/meta-feeder-usenet) so
# the `meta-feeder-sdk` path dependency resolves. Same BuildKit cache-mount
# strategy as the other feeders.
#
# This feeder talks to MariaDB (sqlx, rustls — no system MySQL client needed)
# and reads nntmux's NZB store off a shared volume. It holds no media bytes.
# =============================================================================

FROM rust:1.89-slim-bookworm AS builder
WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        ca-certificates \
        git \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml rust-toolchain.toml ./
COPY crates/ crates/
COPY feeder-plugin/ feeder-plugin/

RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=meta-feeder-usenet-target,target=/build/target \
    cargo build --release --bin usenet-feeder \
    && cp target/release/usenet-feeder /usenet-feeder

# -----------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usenet-feeder /usr/local/bin/usenet-feeder

RUN useradd --system --uid 10001 --no-create-home metafeeder \
    && mkdir -p /data/meta-feeder \
    && chown metafeeder:metafeeder /data/meta-feeder

USER metafeeder
WORKDIR /data/meta-feeder

EXPOSE 8080

ENV META_FEEDER_HTTP_LISTEN=0.0.0.0:8080 \
    META_FEEDER_STATE_DIR=/data/meta-feeder \
    RUST_LOG=info

ENTRYPOINT ["/usr/local/bin/usenet-feeder"]
