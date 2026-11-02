#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export PATH="$(dirname "$(rustup which cargo)"):$PATH"

PORT="${PORT:-6390}"
HOST="${HOST:-127.0.0.1}"
SERVER_BIN="${ROOT}/target/release/jx-server"
CLI_BIN="${ROOT}/target/release/jx-cli"
COMPAT_DIR="${ROOT}/tests/compat"

if [[ ! -x "${SERVER_BIN}" || ! -x "${CLI_BIN}" ]]; then
  echo "building release jx-server + jx-cli ..."
  (cd "${ROOT}" && cargo build --release -p jx-server -p jx-cli)
fi

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/jx-compat.XXXXXX")"
SERVER_PID=""
cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -rf "${WORKDIR}"
}
trap cleanup EXIT

CONF="${WORKDIR}/jx.conf"
mkdir -p "${WORKDIR}/data"
cat >"${CONF}" <<EOF
bind ${HOST}
port ${PORT}
tls-mode plain
dir ${WORKDIR}/data
appendonly no
shards 2
maxmemory 0
loglevel warn
EOF

echo "starting jx-server on ${HOST}:${PORT} (dir=${WORKDIR}/data)"
"${SERVER_BIN}" "${CONF}" >"${WORKDIR}/server.log" 2>&1 &
SERVER_PID=$!

ready=0
for _ in $(seq 1 100); do
  if (echo >"/dev/tcp/${HOST}/${PORT}") >/dev/null 2>&1; then
    ready=1
    break
  fi
  if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
    echo "server failed to start; log:" >&2
    cat "${WORKDIR}/server.log" >&2 || true
    exit 1
  fi
  sleep 0.05
done
if [[ "${ready}" -ne 1 ]]; then
  echo "server did not accept connections on ${HOST}:${PORT}; log:" >&2
  cat "${WORKDIR}/server.log" >&2 || true
  exit 1
fi

export HOST PORT
STATUS=0

echo "== test_redis_py.sh =="
if ! bash "${COMPAT_DIR}/test_redis_py.sh"; then
  STATUS=1
fi

echo "== test_go_redis.sh =="
if ! bash "${COMPAT_DIR}/test_go_redis.sh"; then
  STATUS=1
fi

if [[ "${STATUS}" -ne 0 ]]; then
  echo "compat smoke FAILED" >&2
  exit 1
fi
echo "compat smoke OK"
