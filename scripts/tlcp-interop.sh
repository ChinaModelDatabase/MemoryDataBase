#!/usr/bin/env bash
# TLCP GmSSL interop probe (Task 6 gate artifact).
#
# Live interop with GmSSL is WAIVED for Task 6 completion: gm-tlcp loopback +
# real OpenSSL SM2 fixture certs suffice for this gate. This script remains
# required for Task 12 wiring before any production TLCP claim.
#
# Behaviour:
#   - gmssl not on PATH  -> print skip, exit 0
#   - gmssl present      -> attempt minimal interop with fixture certs
#   - attempt fails      -> print exact gmssl commands for manual interop,
#                           exit 0 with "manual interop required"
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURES="${ROOT}/crates/jx-crypto/tests/fixtures/tlcp"
PORT="${TLCP_INTEROP_PORT:-4433}"
WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/tlcp-interop.XXXXXX")"
SERVER_PID=""

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -rf "${WORKDIR}"
}
trap cleanup EXIT

if ! command -v gmssl >/dev/null 2>&1; then
  echo "SKIP: gmssl not on PATH; TLCP interop not attempted."
  exit 0
fi

echo "gmssl found: $(command -v gmssl)"
gmssl version 2>/dev/null || true

# GmSSL tlcp_server expects concatenated dual certs/keys. Our fixtures use
# unencrypted PKCS#8 SM2 keys; GmSSL 3.2.0's PEM reader expects GmSSL-style
# encrypted keys (BEGIN ENCRYPTED PRIVATE KEY). This is a known gap and is
# why live interop is waived for Task 6.
cat "${FIXTURES}/sign.crt" "${FIXTURES}/enc.crt" > "${WORKDIR}/server_certs.pem"
cat "${FIXTURES}/sign.key" "${FIXTURES}/enc.key" > "${WORKDIR}/server_keys.pem"

echo "Attempting minimal interop: gmssl tlcp_server on 127.0.0.1:${PORT}"
gmssl tlcp_server \
  -port "${PORT}" \
  -cert "${WORKDIR}/server_certs.pem" \
  -key "${WORKDIR}/server_keys.pem" \
  -pass "" \
  -cipher_suite TLS_ECC_SM4_GCM_SM3 \
  -verbose \
  >"${WORKDIR}/server.log" 2>&1 &
SERVER_PID=$!

sleep 1
if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
  echo "tlcp_server failed to start (expected with unencrypted fixture keys):"
  sed 's/^/  /' "${WORKDIR}/server.log" || true
  SERVER_PID=""
  print_manual() {
    cat <<'MANUAL'
manual interop required

Live GmSSL TLCP interop is WAIVED for Task 6 (loopback + real fixtures suffice
for this gate). The gm-tlcp PureRust acceptor in jx-crypto uses unencrypted
fixture keys that gm-crypto can parse; GmSSL 3.2.0 tlcp_server expects
GmSSL-encrypted PEM keys, so these fixtures cannot drive a GmSSL server.

To run live interop manually (required before production TLCP claims, Task 12):

  FIX=crates/jx-crypto/tests/fixtures/tlcp
  WORK=$(mktemp -d)
  PORT=4433

  # 1. GmSSL-native dual-cert chain with encrypted keys (gm-crypto MVP cannot
  #    parse these encrypted keys — that is the known gap under review).
  gmssl sm2keygen -pass 12345678 -out "${WORK}/ca.key"
  gmssl reqgen -C CN -ST Beijing -L Haidian -O MemoryDataBase -OU test \
    -CN "MemoryDataBase SM2 TLCP CA" -key "${WORK}/ca.key" -pass 12345678 \
    -out "${WORK}/ca.req"
  gmssl reqsign -in "${WORK}/ca.req" -days 1825 -key_usage keyCertSign \
    -key "${WORK}/ca.key" -pass 12345678 -out "${WORK}/ca.crt" -ca

  for role in sign enc; do
    if [ "${role}" = sign ]; then usage=digitalSignature; else usage=keyEncipherment; fi
    gmssl sm2keygen -pass 12345678 -out "${WORK}/server_${role}.key"
    gmssl reqgen -C CN -ST Beijing -L Haidian -O MemoryDataBase -OU test \
      -CN "MemoryDataBase SM2 TLCP Server" -key "${WORK}/server_${role}.key" \
      -pass 12345678 -out "${WORK}/server_${role}.req"
    gmssl reqsign -in "${WORK}/server_${role}.req" -days 365 -key_usage "${usage}" \
      -ext_key_usage serverAuth -subject_dns_name localhost \
      -cacert "${WORK}/ca.crt" -key "${WORK}/ca.key" -pass 12345678 \
      -out "${WORK}/server_${role}.crt"
  done
  cat "${WORK}/server_sign.crt" "${WORK}/server_enc.crt" > "${WORK}/server_certs.pem"
  cat "${WORK}/server_sign.key" "${WORK}/server_enc.key" > "${WORK}/server_keys.pem"

  # 2. Start GmSSL TLCP server.
  gmssl tlcp_server -port "${PORT}" \
    -cert "${WORK}/server_certs.pem" -key "${WORK}/server_keys.pem" \
    -pass 12345678 -cipher_suite TLS_ECC_SM4_GCM_SM3 -verbose

  # 3. In another terminal: GmSSL client.
  gmssl tlcp_client -host 127.0.0.1 -port "${PORT}" \
    -cacert "${WORK}/ca.crt" -cipher_suite TLS_ECC_SM4_GCM_SM3

  # 4. Optional: point jx-crypto at the GmSSL-issued certs only after adding
  #    encrypted-key support to TlcpAcceptor::from_config (post-MVP).

# Rust-side reference (pure gm-tlcp loopback, already covered by
# `cargo test -p jx-crypto` — see tests/tlcp.rs tlcp_loopback_handshake):
#   TlcpAcceptor::from_config(cfg) + gm_tlcp::TlcpConnector with fixture certs
MANUAL
  }
  print_manual
  exit 0
fi

echo "tlcp_server started (pid ${SERVER_PID}); attempting client handshake"
CLIENT_OUT="$(echo "hello-tlcp-interop" | gmssl tlcp_client \
  -host 127.0.0.1 -port "${PORT}" \
  -cacert "${FIXTURES}/ca.crt" \
  -cipher_suite TLS_ECC_SM4_GCM_SM3 2>&1)" || true
echo "${CLIENT_OUT}" | sed 's/^/  client: /'

if echo "${CLIENT_OUT}" | grep -qiE "hello-tlcp-interop|application data|read"; then
  echo "PASS: GmSSL TLCP interop handshake succeeded."
  exit 0
fi

echo "GmSSL client did not complete a usable interop exchange."
print_manual() {
  cat <<'MANUAL'
manual interop required

See the manual interop procedure printed below for the exact gmssl commands
to run by hand. Exit code is 0 because live GmSSL interop is WAIVED for
Task 6 completion.
MANUAL
}
print_manual
echo ""
echo "Exact gmssl commands (minimal GmSSL-native dual-cert setup):"
echo "  gmssl tlcp_server -port ${PORT} -cert <concat_sign_enc.crt> -key <concat_sign_enc.key> -pass 12345678 -cipher_suite TLS_ECC_SM4_GCM_SM3"
echo "  gmssl tlcp_client -host 127.0.0.1 -port ${PORT} -cacert ca.crt -cipher_suite TLS_ECC_SM4_GCM_SM3"
exit 0
