# Build stage with dependency caching
FROM rust:alpine AS builder
WORKDIR /app

RUN apk add --no-cache \
    musl-dev \
    pkgconfig \
    openssl-dev \
    openssl-libs-static \
    gcc \
    g++ \
    make \
    cmake \
    perl \
    linux-headers \
    git

COPY prism-cartographer/Cargo.toml prism-cartographer/Cargo.toml
RUN mkdir -p prism-cartographer/src && \
    echo 'fn main() {}' > prism-cartographer/src/main.rs
COPY Cargo.toml Cargo.lock ./
COPY relay-bindings/Cargo.toml relay-bindings/Cargo.toml
COPY prism/Cargo.toml prism/Cargo.toml
COPY relay-bindings/src/ relay-bindings/src/
COPY prism/src/ prism/src/
RUN cargo build --release --target x86_64-unknown-linux-musl -p prism && \
    strip target/x86_64-unknown-linux-musl/release/prism

# --- Runtime stage ---
FROM alpine:3.22 AS runner

LABEL org.opencontainers.image.source="https://github.com/BitCraftToolBox/prism"

RUN apk add --no-cache ca-certificates && \
    addgroup -g 1000 prism && \
    adduser -D -u 1000 -G prism prism

WORKDIR /app
COPY --from=builder --chown=prism:prism /app/target/x86_64-unknown-linux-musl/release/prism /app/

USER prism
ENTRYPOINT ["/app/prism"]
