# syntax=docker/dockerfile:1

FROM rust:1.98-bookworm AS builder

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        build-essential \
        cmake \
        pkg-config \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cache dependencies separately from application source.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin \
    && printf 'fn main() {}\n' > src/main.rs \
    && printf 'fn main() {}\n' > src/bin/benchmark.rs \
    && printf 'fn main() {}\n' > src/bin/kafka_load.rs \
    && printf '// dependency cache placeholder\n' > src/lib.rs \
    && cargo build --locked --release --bin rustper \
    && rm -rf src

COPY src ./src
RUN touch src/lib.rs src/main.rs \
    && cargo build --locked --release --bin rustper

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 rustper \
    && useradd --system --uid 10001 --gid rustper --home-dir /nonexistent rustper

COPY --from=builder /build/target/release/rustper /usr/local/bin/rustper
COPY config/local.toml /etc/rustper/config.toml

USER 10001:10001

ENTRYPOINT ["/usr/local/bin/rustper"]
CMD ["/etc/rustper/config.toml"]
