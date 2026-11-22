FROM rust:1.99.1-slim-bookworm AS builder

WORKDIR /build
RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential \
    && rm -rf /var/lib/apt/lists/*

ARG CARGO_BUILD_JOBS=2
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/
COPY keeper.example.toml keeper.secrets.example.toml ./
RUN cargo build --release --locked -j "${CARGO_BUILD_JOBS}"

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --home-dir /home/keeper --shell /usr/sbin/nologin keeper \
    && mkdir /config /state \
    && chown keeper:keeper /config /state

COPY --from=builder /build/target/release/keeper /usr/local/bin/keeper
COPY LICENSE /usr/share/doc/keeper/copyright

ENV HOME=/home/keeper \
    KEEPER_KEYPAIR_PATH=/run/keeper/id.json \
    KEEPER_STATE_DIR=/state
WORKDIR /config
USER keeper
ENTRYPOINT ["keeper"]
CMD ["--help"]
