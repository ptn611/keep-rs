# ---- Build Stage ----
FROM rust:1.91.1 AS builder
WORKDIR /app

# 1. Cache dependencies first
RUN apt-get update && apt-get install jq -y && rustup component add rustfmt

# TODO: docker layer cache
# COPY Cargo.toml Cargo.lock ./
# RUN mkdir src && echo "fn main() {}" > src/main.rs

# 2. Copy actual code and build.
# protocol-v2 (the `drift` crate), drift-rs and drift-ffi-sys are consumed as
# path-deps / build deps siblings of keep-rs (see Cargo.toml and drift-rs
# build.rs). The build context is the parent dir holding all of them; preserve
# the relative layout so Cargo's `../drift-rs`, `../drift-ffi-sys` and
# `../protocol-v2/programs/drift` resolve.
COPY protocol-v2 ./protocol-v2
COPY drift-ffi-sys ./drift-ffi-sys
COPY drift-rs ./drift-rs
COPY keep-rs ./keep-rs

WORKDIR /app/keep-rs
RUN cargo build --release

# ---- Runtime Stage ----
FROM debian:bookworm-slim
# RUN apt-get update && apt-get install -y ca-certificates lldb
RUN apt-get update && apt-get install -y ca-certificates
COPY --from=builder /app/drift-ffi-sys/target/release/libdrift_ffi_sys.so /lib/
# COPY --from=builder /app/keep-rs/target/release/keeprs /usr/local/bin/keeprs-real
COPY --from=builder /app/keep-rs/target/release/keeprs /usr/local/bin/keeprs

# RUN echo '#!/bin/bash\nexec lldb -o "run" -o "bt all" -o "quit" -- /usr/local/bin/keeprs-real "$@"' > /usr/local/bin/keeprs && \
#     chmod +x /usr/local/bin/keeprs

EXPOSE 9898
ENV METRICS_PORT=9898

ENTRYPOINT ["/usr/local/bin/keeprs"]
