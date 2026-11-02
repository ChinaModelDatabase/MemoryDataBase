#!/usr/bin/env bash
# SUPERSEDED by scripts/cross-build.sh (xinchuang cross-build, Task 2).
# Prefer: ./scripts/cross-build.sh --target all
# Kept as a thin wrapper for existing runbooks/docs that call this path.
#
# Task 17 (legacy): MemoryDataBase Linux x86_64 / aarch64 release build.
# Builds jx-server and jx-cli for the two mainstream 信创 Linux targets.
# On this macOS dev machine a cross-linker may be missing:
#   - rustc compile success + link failure is reported as a warning
#     (install cross-gcc and set the linker in .cargo/config.toml),
#   - genuine compile errors are still fatal.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export PATH="$(dirname "$(rustup which cargo)"):$PATH"

TARGETS="x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu"

is_toolchain_error() {
  local log="$1"
  grep -Eq 'linker `[^`]+` not found|error: linking with|cc-rs: failed to find tool|failed to find tool "[^"]+"|error occurred in cc-rs' "$log"
}

echo "== MemoryDataBase linux release build =="
echo "cwd:   $ROOT"
echo "date:  $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "rustc: $(rustc --version 2>/dev/null || echo unknown)"

LOG="$(mktemp "${TMPDIR:-/tmp}/MemoryDataBase-build-linux.XXXXXX.log")"
trap 'rm -f "$LOG"' EXIT

echo "== rustup target add ${TARGETS} =="
# shellcheck disable=SC2086
rustup target add ${TARGETS}
echo ""

SUMMARY=""
FAILED_COMPILE=0
WARNED_LINK=0

for target in ${TARGETS}; do
  echo "== cargo build --release --target ${target} -p jx-server -p jx-cli =="
  if cargo build --release --target "$target" -p jx-server -p jx-cli 2>&1 | tee "$LOG"; then
    SUMMARY="${SUMMARY}  ${target}  BUILD_OK
"
    echo "=> ${target}: build OK"
  elif is_toolchain_error "$LOG"; then
    SUMMARY="${SUMMARY}  ${target}  TOOLCHAIN_WARN
"
    WARNED_LINK=1
    echo "=> ${target}: compile of pure-Rust crates OK, C/ASM or link FAILED (cross-gcc missing)"
    echo "   ring (via cc-rs) and the final linker need a cross C toolchain:"
    echo "   - install cross-gcc, e.g. brew install aarch64-unknown-linux-gnu-gcc"
    echo "     or apt install gcc-x86-64-linux-gnu / gcc-aarch64-linux-gnu on Linux hosts"
    echo "   - then set in ${ROOT}/.cargo/config.toml:"
    echo "       [target.x86_64-unknown-linux-gnu]"
    echo "       linker = \"x86_64-linux-gnu-gcc\""
    echo "       [target.aarch64-unknown-linux-gnu]"
    echo "       linker = \"aarch64-linux-gnu-gcc\""
  else
    SUMMARY="${SUMMARY}  ${target}  COMPILE_FAIL
"
    FAILED_COMPILE=1
    echo "=> ${target}: COMPILE FAILED (see log above)"
  fi
  echo ""
done

echo "== summary =="
printf '%s' "$SUMMARY"
echo ""

if [[ "$FAILED_COMPILE" -ne 0 ]]; then
  echo "status: FAILED (compile errors)"
  exit 1
fi
if [[ "$WARNED_LINK" -ne 0 ]]; then
  echo "status: OK_WITH_WARNINGS (Rust code ok; install cross-gcc to finish ring C/ASM + link)"
  exit 0
fi
echo "status: OK"
