# ollamux container image — static musl binary on scratch.
#
# TLS uses rustls + bundled webpki-roots, so no ca-certificates is
# needed at runtime. Keys come in via the OLLAMUX_KEYS environment
# variable or a mounted file (see below). The proxy is unauthenticated:
# only publish/expose it on trusted networks.
#
# Build:  docker build -t ollamux .
# Run (keys via env):
#   docker run -p 11435:11435 -e OLLAMUX_KEYS="key-one key-two" ollamux
# Run (keys via mounted file):
#   docker run -p 11435:11435 -v ./keys:/keys:ro -e OLLAMUX_KEYS_FILE=/keys ollamux
#
# OLLAMUX_KEYS_FILE is a convenience wrapper below; the binary itself only
# reads OLLAMUX_KEYS, or a keys file path.

FROM rust:1-alpine AS build
WORKDIR /src
RUN apk add --no-cache musl-dev
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
# --locked: fail the image build if Cargo.lock is out of sync.
RUN cargo build --release --locked && strip target/release/ollamux

FROM scratch
COPY --from=build /src/target/release/ollamux /ollamux
# GPL-2.0-or-later requires offering source; the image label points at
# the repo which is the corresponding source for every pushed tag.
LABEL org.opencontainers.image.title="ollamux" \
      org.opencontainers.image.description="Key-rotating reverse proxy for the Ollama Cloud API" \
      org.opencontainers.image.licenses="GPL-2.0-or-later" \
      org.opencontainers.image.source="https://github.com/j-stechmann/ollamux" \
      org.opencontainers.image.url="https://ghcr.io/j-stechmann/ollamux"
USER 65532:65532
EXPOSE 11435
# Loopback default is meaningless inside a container (published ports
# DNAT to the container IP), so default to all-interfaces here; the
# container boundary is the trust boundary. Override with:
#   docker run ... ollamux --addr 127.0.0.1:11435
ENTRYPOINT ["/ollamux"]
CMD ["--addr", "0.0.0.0:11435"]