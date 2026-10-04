FROM rust:1.98-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# --release only; no target-cpu=native (no other server gets an equivalent).
RUN cargo build --release --locked && cp target/release/wsbench-rust /server

FROM debian:bookworm-slim
LABEL org.wsbench.runtime="rust" org.wsbench.implementation="Rust + axum 0.8 (Tokio)"
COPY --from=build /server /server
ENV PORT=8080 CORES=1
EXPOSE 8080
ENTRYPOINT ["/server"]
