#!/usr/bin/env bash
# Task 17: MemoryDataBase license check (cargo-deny).
#
# Runs cargo-deny licenses check from the workspace root. Installs
# cargo-deny via cargo if the binary is not already available.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export PATH="$(dirname "$(rustup which cargo)"):$PATH"

echo "== MemoryDataBase license check =="
echo "cwd:   $ROOT"
echo "date:  $(date -u '+%Y-%m-%dT%H:%M:%SZ')"

if ! command -v cargo-deny >/dev/null 2>&1 && ! cargo deny --version >/dev/null 2>&1; then
  echo "cargo-deny not found; installing via cargo install cargo-deny ..."
  cargo install cargo-deny --locked
fi
echo "cargo-deny: $(cargo deny --version 2>/dev/null || echo unknown)"

cargo deny check licenses
echo ""
echo "license check: OK"
