#!/usr/bin/env bash
# MemoryDataBase perf acceptance matrix (shard-ownership branch).
#
# Builds release jx-server+jx-cli, starts a temp plain RESP server
# (shards=nproc, maxmemory 4gb, noeviction, appendonly no), runs the
# jx-cli bench matrix, and prints PASS/FAIL for pipeline=1 vs 500000 OPS/s.
#
# jx-cli bench today is SET-then-GET phases only (no mixed workload mode).
# The GET phase is therefore the closest GET-heavy (GET-after-preload) proxy;
# an 80/20 mixed run is noted as unsupported by the current bench tool.
set -euo pipefail
export PATH="$(dirname "$(rustup which cargo)"):$PATH"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PORT="${PERF_PORT:-6399}"
HOST="${PERF_HOST:-127.0.0.1}"
ACCEPT_OPS="${PERF_ACCEPT_OPS:-500000}"
VALUE_SIZE="${PERF_VALUE_SIZE:-64}"
TARGET_OPS="${PERF_TARGET_OPS:-100000}"
CLIENTS="${PERF_CLIENTS:-64}"
PIPELINE_PRIMARY_CLIENTS="${PERF_PRIMARY_CLIENTS:-64}"
PIPELINE_PRIMARY="${PERF_PRIMARY_PIPELINE:-1}"
PIPELINE_SECONDARY="${PERF_SECONDARY_PIPELINE:-32}"
EXTRA_CLIENTS=(16 256)

echo "== MemoryDataBase perf acceptance matrix =="
echo "date:      $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "uname:     $(uname -srm)"
echo "host:      $(hostname 2>/dev/null || echo unknown)"
echo "cpu:       $(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)"
CPU_COUNT="$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo unknown)"
echo "cpu count: ${CPU_COUNT}"
echo "rustc:     $(rustc --version 2>/dev/null || echo 'rustc not found')"
echo "cargo:     $(cargo --version 2>/dev/null || echo 'cargo not found')"
echo "cwd:       $ROOT"
echo "accept:    pipeline=1 SET/GET ops/s >= ${ACCEPT_OPS}"
echo ""

echo "== build release jx-server jx-cli =="
cargo build --release -p jx-server -p jx-cli
echo ""

DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/MemoryDataBase-perf.XXXXXX")"
CONF="$(mktemp "${TMPDIR:-/tmp}/MemoryDataBase-perf.conf.XXXXXX")"
SERVER_PID=""
SERVER_LOG="${DATA_DIR}/server.log"

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill -TERM "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -rf "${DATA_DIR}" "${CONF}"
}
trap cleanup EXIT

cat > "$CONF" <<EOF
bind ${HOST}
port ${PORT}
tls-mode plain
dir ${DATA_DIR}
shards ${CPU_COUNT}
appendonly no
maxmemory 4gb
maxmemory-policy noeviction
maxclients 10000
timeout 0
loglevel warn
EOF

echo "== server config (${CONF}) =="
cat "$CONF"
echo ""

echo "== start jx-server on ${HOST}:${PORT} =="
./target/release/jx-server "$CONF" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
sleep 2
if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
  echo "ERROR: jx-server failed to start; log:"
  cat "$SERVER_LOG" || true
  exit 1
fi
echo "server pid: ${SERVER_PID}"
echo ""

run_bench() {
  local clients="$1" pipeline="$2" label="$3"
  echo "== bench: ${label} (clients=${clients} pipeline=${pipeline} requests=${TARGET_OPS} value=${VALUE_SIZE}) =="
  echo "jx-cli bench --host ${HOST} --port ${PORT} --clients ${clients} --requests ${TARGET_OPS} --value-size ${VALUE_SIZE} --pipeline ${pipeline}"
  ./target/release/jx-cli bench \
    --host "$HOST" --port "$PORT" \
    --clients "$clients" --requests "$TARGET_OPS" \
    --value-size "$VALUE_SIZE" --pipeline "$pipeline"
  echo ""
}

extract_ops() {
  # extract_ops <SET|GET> ; reads stdin (bench output) and prints ops/s or empty
  local op="$1"
  local line
  line="$(grep -E "^${op} ops=" || true)"
  if [[ -z "$line" ]]; then
    return 0
  fi
  echo "$line" | sed -E 's/.*ops\/s=([0-9]+).*/\1/'
}

PRIMARY_OUT="$(mktemp "${TMPDIR:-/tmp}/MemoryDataBase-matrix-primary.XXXXXX")"
run_bench "$PIPELINE_PRIMARY_CLIENTS" "$PIPELINE_PRIMARY" "primary acceptance" | tee "$PRIMARY_OUT"

SET_OPS="$(extract_ops SET < "$PRIMARY_OUT" || true)"
GET_OPS="$(extract_ops GET < "$PRIMARY_OUT" || true)"
PRIMARY_STATUS="UNKNOWN"
if [[ -n "${SET_OPS:-}" && -n "${GET_OPS:-}" ]]; then
  if (( SET_OPS >= ACCEPT_OPS && GET_OPS >= ACCEPT_OPS )); then
    PRIMARY_STATUS="PASS"
  else
    PRIMARY_STATUS="FAIL"
  fi
fi
echo "== primary acceptance vs ${ACCEPT_OPS} OPS/s =="
echo "SET ops/s=${SET_OPS:-n/a}  GET ops/s=${GET_OPS:-n/a}  => ${PRIMARY_STATUS}"
echo ""

echo "== bench: clients=${CLIENTS} pipeline=${PIPELINE_SECONDARY} =="
run_bench "$CLIENTS" "$PIPELINE_SECONDARY" "pipeline=${PIPELINE_SECONDARY} (secondary)"

for c in "${EXTRA_CLIENTS[@]}"; do
  run_bench "$c" "$PIPELINE_PRIMARY" "pipeline=${PIPELINE_PRIMARY} clients=${c}"
done

echo "== mixed workload note =="
echo "jx-cli bench has no --mixed/--ratio flag; workload is fixed SET phase then GET phase."
echo "Current bench has no mixed-workload mode; running GET-heavy (GET-after-preload) as the proxy (bench GET phase after SET preload)."
echo ""

rm -f "$PRIMARY_OUT"
echo "== teardown =="
kill -TERM "${SERVER_PID}" 2>/dev/null || true
wait "${SERVER_PID}" 2>/dev/null || true
SERVER_PID=""
echo "done."
