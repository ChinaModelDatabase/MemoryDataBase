#!/usr/bin/env bash
# Task 16: MemoryDataBase MVP performance acceptance baseline.
#
# Compatibility wrapper: delegates to scripts/perf-matrix.sh, which is the
# full acceptance matrix (pipeline=1 primary vs 500k, pipeline=32, client
# scaling, GET-heavy proxy). Kept so docs/commands using perf-report.sh
# still work.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec bash "$ROOT/scripts/perf-matrix.sh"
