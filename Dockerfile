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

COPY Cargo.toml Cargo.lock ./
COPY relay-bindings/Cargo.toml relay-bindings/Cargo.toml
COPY prism/Cargo.toml prism/Cargo.toml

# Build dependency graph with placeholder sources so this layer is reused when
# only application source files change.
RUN mkdir -p relay-bindings/src prism/src prism-cartographer/src && \
    printf '[package]\nname = "prism-cartographer"\nversion = "0.0.0"\nedition = "2024"\n' > prism-cartographer/Cargo.toml && \
    echo '// placeholder for dependency caching' > relay-bindings/src/lib.rs && \
    echo 'fn main() {}' > prism/src/main.rs && \
    echo 'fn main() {}' > prism-cartographer/src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl -p prism

# Replace placeholders with real sources and rebuild only the workspace crates.
COPY relay-bindings/src/ relay-bindings/src/
COPY prism/src/ prism/src/
RUN ! grep -qx 'fn main() {}' prism/src/main.rs && \
    cargo clean -p prism -p relay_bindings && \
    touch relay-bindings/src/lib.rs && touch prism/src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl -p prism && \
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
