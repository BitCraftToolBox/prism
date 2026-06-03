# Prism task runner. Requires `just` (https://github.com/casey/just) and
# `spacetime` CLI on PATH for the module/binding tasks.

default:
    @just --list

# --- relay module ---

# Publish the relay module to a SpacetimeDB server.
publish-dev module="prism-relay" host="http://127.0.0.1:3000":
    cd relay-module && spacetime publish --module-path . --server {{host}} {{module}}

publish-prod module="prism-relay" host="https://maincloud.spacetimedb.com":
    cd relay-module && spacetime publish --module-path . --server {{host}} {{module}}


# Regenerate Rust client bindings into relay-bindings/src.
generate-bindings:
    spacetime generate -y --lang rust --out-dir relay-bindings/src --module-path relay-module
    mv relay-bindings/src/mod.rs relay-bindings/src/lib.rs

generate-map path="../bitcraftmap/src/relay-bindings":
    spacetime generate -y --lang ts --out-dir {{path}} --module-path relay-module

# --- prism workspace ---

build:
    cargo build --workspace

run:
    cargo run -p prism

fmt:
    cargo fmt -p prism

clippy:
    cargo clippy --workspace --all-targets -- -D warnings

# --- infra ---

docker-up:
    docker compose up -d

docker-down:
    docker compose down

docker-logs:
    docker compose logs -f prism


