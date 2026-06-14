FROM rust:1.94 AS build
WORKDIR /app
COPY Cargo.toml .
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src target/release/httparena-mqbridge-h2c* target/release/deps/httparena_mqbridge_h2c*
COPY src ./src
RUN RUSTFLAGS="-C target-cpu=native" cargo build --release

FROM debian:bookworm-slim
RUN groupadd --system mqbridge \
    && useradd --system --gid mqbridge --home-dir /nonexistent --shell /usr/sbin/nologin mqbridge
COPY --from=build --chown=mqbridge:mqbridge /app/target/release/httparena-mqbridge-h2c /server
EXPOSE 8082
USER mqbridge:mqbridge
CMD ["/server"]
