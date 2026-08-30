#!/usr/bin/env bash
# Produce the release source tarball + vendored-crates archive:
#   ollamux-<ver>.tar.gz        — git-archive (HEAD) + .cargo/config.toml
#   ollamux-<ver>-vendor.tar.xz — cargo vendor output, flat "vendor/" root
#   SHA256SUMS               — checksums for both
#
# Consumers:
#   deb (CI): extract both; vendor/ lands inside the source tree; the
#             in-tree .cargo/config.toml makes the build offline.
#   rpm     : %prep extracts the vendor tarball next to the source;
#             %cargo_prep -v vendor wires crates.io -> ./vendor
#
# Usage: scripts/vendor-source.sh [version]
set -euo pipefail
cd "$(dirname "$0")/.."

VER="${1:-$(cargo pkgid | sed 's/.*#//')}"
NAME="ollamux-$VER"
OUT="dist"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$OUT"

echo "==> vendored crates"
rm -rf vendor
cargo vendor vendor > "$OUT/vendor-config.toml"

echo "==> source tarball (git archive HEAD + vendored .cargo/config.toml)"
git archive --format=tar --prefix="$NAME/" HEAD | tar -x -C "$WORK"
mkdir -p "$WORK/$NAME/.cargo"
cp "$OUT/vendor-config.toml" "$WORK/$NAME/.cargo/config.toml"
tar -czf "$OUT/$NAME.tar.gz" -C "$WORK" "$NAME"

echo "==> vendor archive (flat vendor/ at root, as %cargo_prep -v expects)"
tar -cJf "$OUT/$NAME-vendor.tar.xz" -C . vendor
rm -rf vendor

echo "==> checksums"
( cd "$OUT" && sha256sum "$NAME.tar.gz" "$NAME-vendor.tar.xz" > SHA256SUMS )
cat "$OUT/SHA256SUMS"
echo "==> done: $OUT/"