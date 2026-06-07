# Build stage
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

# Copy workspace manifest and lock file first for layer caching.
COPY Cargo.toml Cargo.lock ./

# Stub out the other workspace members so Cargo can resolve the workspace
# without needing their full source trees.
RUN mkdir -p prism/src relay-bindings/src && \
    echo 'fn main() {}' > prism/src/main.rs && \
    echo '' > relay-bindings/src/lib.rs
COPY prism/Cargo.toml prism/Cargo.toml
COPY relay-bindings/Cargo.toml relay-bindings/Cargo.toml

# Copy cartographer manifest, then source so dependency layers are cached
# independently of source changes.
COPY prism-cartographer/Cargo.toml prism-cartographer/Cargo.toml
RUN mkdir -p prism-cartographer/src && echo 'fn main() {}' > prism-cartographer/src/main.rs
RUN cargo build --release --target x86_64-unknown-linux-musl -p prism-cartographer || true

# Now copy real source and rebuild only the cartographer binary.
COPY prism-cartographer/src/ prism-cartographer/src/
RUN touch prism-cartographer/src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl -p prism-cartographer && \
    strip target/x86_64-unknown-linux-musl/release/prism-cartographer

# --- Runtime stage ---
FROM alpine:3.22 AS runner

LABEL org.opencontainers.image.source="https://github.com/BitCraftToolBox/prism"

RUN apk add --no-cache ca-certificates && \
    addgroup -g 1000 prism && \
    adduser -D -u 1000 -G prism prism

WORKDIR /app
COPY --from=builder --chown=prism:prism \
    /app/target/x86_64-unknown-linux-musl/release/prism-cartographer /app/

USER prism
ENTRYPOINT ["/app/prism-cartographer"]
