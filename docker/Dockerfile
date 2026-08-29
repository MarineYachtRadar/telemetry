FROM rust:1.97-alpine AS builder
# `build-base` is needed to compile the SQLite amalgamation that rusqlite's
# `bundled` feature builds, so the image carries no libsqlite3 of its own.
RUN apk add --no-cache build-base

WORKDIR /usr/src/telemetry

# Cache the dependency build: cargo compiles dependencies before the crate
# itself, so a manifest and a stub main are enough to fill this layer.
COPY Cargo.lock Cargo.toml ./
RUN mkdir src && echo "fn main() {}" > src/main.rs \
    && cargo build --release \
    && rm -rf src target/release/telemetry target/release/deps/telemetry-*

COPY src src/
COPY static static/
RUN cargo build --release

FROM alpine:3.21
LABEL org.opencontainers.image.source="https://github.com/keesverruijt/telemetry"
LABEL org.opencontainers.image.title="telemetry"
LABEL org.opencontainers.image.description="Collector for anonymous mayara usage reports"
LABEL org.opencontainers.image.license="Apache-2.0"

# The database is the only state; it lives on a volume owned by the run user
# so the rest of the filesystem can stay read-only.
RUN apk add --no-cache tini \
    && adduser -D -h /var/lib/telemetry telemetry
COPY --from=builder /usr/src/telemetry/target/release/telemetry /usr/local/bin/telemetry

USER telemetry
VOLUME /var/lib/telemetry
EXPOSE 8099
ENV RUST_LOG=info

# The collector binds every interface inside the container -- nginx reaches it
# from outside the namespace -- and the port publish is what keeps it on the
# host's loopback.
CMD ["telemetry", "--listen", "0.0.0.0:8099", "--database", "/var/lib/telemetry/telemetry.db"]

# Probes the collector's own health endpoint, which reports on the database it
# writes to. BusyBox wget is already in the base image; there is no curl.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:8099/health || exit 1
ENTRYPOINT ["tini", "--"]
