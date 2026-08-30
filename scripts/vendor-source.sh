#!/usr/bin/env bash
# Produce the release source tarball + vendored-crates archive:
#   omlx-<ver>.tar.gz        — git-archive (tagged commit), no junk
#   omlx-<ver>-vendor.tar.xz — cargo vendor output + .cargo/config.toml
#   SHA256SUMS               — checksums for both
#
# Used by release CI for the deb/rpm/COPR offline builds. Debian
# additionally rebuilds its orig tarball from these.
#
# Usage: scripts/vendor-source.sh [version]
#        (version defaults to CARGO_PKG_VERSION via cargo read-manifest)
set -euo pipefail
cd "$(dirname "$0")/.."

VER="${1:-$(cargo pkgid | sed 's/.*#//')}"
NAME="omlx-$VER"
OUT="dist"
mkdir -p "$OUT"

echo "==> source tarball (git archive)"
git archive --format=tar.gz --prefix="$NAME/" -o "$OUT/$NAME.tar.gz" "HEAD"

echo "==> vendored crates"
rm -rf "$OUT/vendor"
cargo vendor "$OUT/vendor" > "$OUT/vendor-config.toml"

# .cargo/config.toml must ship inside the omlx source tree so cargo
# replaces crates.io with ./vendor in deb/rpm builds.
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
git archive --format=tar --prefix="$NAME/" HEAD > "$WORK/base.tar"
mkdir -p "$WORK/$NAME/.cargo"
cp "$OUT/vendor-config.toml" "$WORK/$NAME/.cargo/config.toml"
tar -rf "$WORK/base.tar.add" -C "$WORK" "$NAME/.cargo/config.toml" 2>/dev/null ||
  tar -rf "$WORK/base.tar" -C "$WORK" "$NAME/.cargo/config.toml"
rm -f "$WORK/base.tar.add"
gzip -c "$WORK/base.tar" > "$OUT/$NAME.tar.gz"

echo "==> vendor archive (same .cargo/config.toml at its root)"
tar -cJf "$OUT/$NAME-vendor.tar.xz" -C "$OUT" \
  --transform "s|^vendor|$NAME/vendor|" vendor
cp "$OUT/vendor-config.toml" "$OUT/$NAME-cargo-config.toml"

echo "==> checksums"
( cd "$OUT" && sha256sum "$NAME.tar.gz" "$NAME-vendor.tar.xz" > SHA256SUMS )
cat "$OUT/SHA256SUMS"
echo "==> done: $OUT/"