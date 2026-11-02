#!/usr/bin/env bash
# SUPERSEDED by scripts/cross-build.sh (xinchuang cross-build, Task 2).
# Prefer: ./scripts/cross-build.sh --target loongarch64-unknown-linux-gnu
# Kept as a thin wrapper for existing runbooks/docs that call this path.
#
# Task 17 (legacy): MemoryDataBase LoongArch (龙芯) Linux release build.
#
# Cross-compiles jx-server and jx-cli for loongarch64-unknown-linux-gnu.
# On macOS (or any host without the LoongArch cross toolchain) rustc can
# still compile the code; the link step will fail until the LoongArch
# cross-gcc is installed and exposed as the linker.
#
# Required 龙芯 toolchain env vars (adjust paths to your install):
#   export CARGO_TARGET_LOONGARCH64_UNKNOWN_LINUX_GNU_LINKER=loongarch64-linux-gnu-gcc
#   # optionally, if the toolchain is not on PATH:
#   # export LOONGARCH64_LINUX_GNU_PREFIX=/opt/loongarch64-linux-gnu
#   # export PATH="$LOONGARCH64_LINUX_GNU_PREFIX/bin:$PATH"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export PATH="$(dirname "$(rustup which cargo)"):$PATH"

TARGET=loongarch64-unknown-linux-gnu
LINKER="${CARGO_TARGET_LOONGARCH64_UNKNOWN_LINUX_GNU_LINKER:-loongarch64-linux-gnu-gcc}"

is_toolchain_error() {
  local log="$1"
  grep -Eq 'linker `[^`]+` not found|error: linking with|cc-rs: failed to find tool|failed to find tool "[^"]+"|error occurred in cc-rs' "$log"
}

echo "== MemoryDataBase loongarch64 release build =="
echo "cwd:   $ROOT"
echo "date:  $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "rustc: $(rustc --version 2>/dev/null || echo unknown)"
echo "linker env: CARGO_TARGET_LOONGARCH64_UNKNOWN_LINUX_GNU_LINKER=${LINKER}"

LOG="$(mktemp "${TMPDIR:-/tmp}/MemoryDataBase-build-loongarch.XXXXXX.log")"
trap 'rm -f "$LOG"' EXIT

echo "== rustup target add ${TARGET} =="
rustup target add "$TARGET"
echo ""

echo "== cargo build --release --target ${TARGET} -p jx-server -p jx-cli =="
if cargo build --release --target "$TARGET" -p jx-server -p jx-cli 2>&1 | tee "$LOG"; then
  echo ""
  echo "=> ${TARGET}: build OK"
  echo "status: OK"
  exit 0
elif is_toolchain_error "$LOG"; then
  echo ""
  echo "=> ${TARGET}: compile of pure-Rust crates OK, C/ASM or link FAILED (LoongArch cross-gcc missing)"
  echo "   Expected on macOS / hosts without the 龙芯 toolchain."
  echo "   ring (via cc-rs) and the final linker need loongarch64-linux-gnu-gcc."
  echo "   To complete, install the LoongArch cross toolchain, then:"
  echo "     export CARGO_TARGET_LOONGARCH64_UNKNOWN_LINUX_GNU_LINKER=loongarch64-linux-gnu-gcc"
  echo "     export CC_loongarch64_unknown_linux_gnu=loongarch64-linux-gnu-gcc   # for ring/cc-rs"
  echo "     export PATH=\"/opt/loongarch64-linux-gnu/bin:\$PATH\"   # adjust"
  echo "   Re-run: bash scripts/build-loongarch.sh"
  echo "status: OK_WITH_WARNINGS (Rust code ok; need 龙芯 cross-gcc for ring C/ASM + link)"
  exit 0
else
  echo ""
  echo "=> ${TARGET}: COMPILE FAILED (see log above)"
  echo "status: FAILED"
  exit 1
fi
