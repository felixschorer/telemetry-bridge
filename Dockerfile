FROM rust:1.97-alpine AS build-base

WORKDIR /app

RUN apk add --no-cache musl-dev pkgconfig build-base
RUN cargo install cargo-chef --locked


FROM build-base AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json


FROM build-base AS builder
COPY --from=planner /app/recipe.json recipe.json

# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json

COPY . .
RUN cargo build --release


FROM alpine:3.20
COPY --from=builder --chown=1000:1000 --chmod=755 /app/target/release/telemetry-bridge /app/telemetry-bridge

WORKDIR /app

RUN adduser -D -u 1000 appuser
USER appuser

ENV RUST_LOG=info

RUN ./telemetry-bridge init > config.toml

ENTRYPOINT ["./telemetry-bridge", "-c=config.toml"]