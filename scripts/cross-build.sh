#!/usr/bin/env bash
# MemoryDataBase xinchuang cross-build entry point.
#
# Builds (or check-only smoke-tests) jx-server and jx-cli for the 信创
# Linux targets. Replaces scripts/build-linux.sh and build-loongarch.sh.
#
# Usage:
#   ./scripts/cross-build.sh [--target <triple>|all] [--check-only] [--docker]
#
#   --target      Triple or "all" (default: all)
#                 x86_64-unknown-linux-gnu
#                 aarch64-unknown-linux-gnu
#                 loongarch64-unknown-linux-gnu
#   --check-only  cargo check --target only (no release link)
#   --docker      Prefer docker/cross image builds; falls back to local
#                 only when the MemoryDataBase-cross image is missing.
#                 A docker cargo failure is BUILD_FAIL/CHECK_FAIL (no fallback).
#
# Env:
#   ALLOW_LINK_FAIL=1   Exit 0 on missing linker/cc-rs after printing guidance
#
# Exit codes:
#   0  all targets OK (or only toolchain gaps with ALLOW_LINK_FAIL=1)
#   1  compile/check failure, or toolchain gap without ALLOW_LINK_FAIL=1
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export PATH="$(dirname "$(rustup which cargo)"):$PATH"

TARGETS_ALL="x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu loongarch64-unknown-linux-gnu"
TARGET="${TARGET:-all}"
CHECK_ONLY="${CHECK_ONLY:-0}"
DOCKER="${DOCKER:-0}"
ALLOW_LINK_FAIL="${ALLOW_LINK_FAIL:-0}"

usage() {
  cat <<'EOF'
Usage: scripts/cross-build.sh [--target <triple>|all] [--check-only] [--docker]

Targets:
  x86_64-unknown-linux-gnu
  aarch64-unknown-linux-gnu
  loongarch64-unknown-linux-gnu
  all (default)

Flags:
  --target <triple>|all  Target triple(s) to build (default: all)
  --check-only           cargo check only; skip release link
  --docker               Prefer docker/cross builds when available
                         (fallback to local only if image is missing;
                          docker cargo failure = BUILD_FAIL/CHECK_FAIL)

Env:
  ALLOW_LINK_FAIL=1      Exit 0 on missing linker/cc-rs after printing guidance
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      [[ $# -ge 2 ]] || { echo "error: --target requires a value" >&2; exit 2; }
      TARGET="$2"
      shift 2
      ;;
    --target=*)
      TARGET="${1#*=}"
      shift
      ;;
    --check-only)
      CHECK_ONLY=1
      shift
      ;;
    --docker)
      DOCKER=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

resolve_targets() {
  if [[ "$TARGET" == "all" ]]; then
    echo "$TARGETS_ALL"
    return
  fi
  local known=0 t
  for t in $TARGETS_ALL; do
    if [[ "$t" == "$TARGET" ]]; then
      known=1
      break
    fi
  done
  if [[ "$known" -ne 1 ]]; then
    echo "error: unknown target: $TARGET" >&2
    echo "known targets: $TARGETS_ALL (or: all)" >&2
    exit 2
  fi
  echo "$TARGET"
}

is_toolchain_error() {
  local log="$1"
  grep -Eq 'linker `[^`]+` not found|error: linking with|cc-rs: failed to find tool|failed to find tool "[^"]+"|error occurred in cc-rs|Could not find `[^`]+` \(linker\)|unable to find linker' "$log"
}

linker_name_for() {
  case "$1" in
    x86_64-unknown-linux-gnu) echo "x86_64-linux-gnu-gcc" ;;
    aarch64-unknown-linux-gnu) echo "aarch64-linux-gnu-gcc" ;;
    loongarch64-unknown-linux-gnu) echo "loongarch64-linux-gnu-gcc" ;;
    *) echo "cross-gcc" ;;
  esac
}

apt_pkg_for() {
  case "$1" in
    x86_64-unknown-linux-gnu) echo "gcc-x86-64-linux-gnu g++-x86-64-linux-gnu" ;;
    aarch64-unknown-linux-gnu) echo "gcc-aarch64-linux-gnu g++-aarch64-linux-gnu" ;;
    loongarch64-unknown-linux-gnu) echo "loongarch64-linux-gnu-gcc (LoongArch toolchain / vendor package)" ;;
  esac
}

cc_env_for() {
  case "$1" in
    x86_64-unknown-linux-gnu) echo "CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc" ;;
    aarch64-unknown-linux-gnu) echo "CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc" ;;
    loongarch64-unknown-linux-gnu) echo "CC_loongarch64_unknown_linux_gnu=loongarch64-linux-gnu-gcc" ;;
  esac
}

print_toolchain_guidance() {
  local triple="$1"
  local cc cc_env linker
  cc="$(linker_name_for "$triple")"
  cc_env="$(cc_env_for "$triple")"
  echo "   Missing cross C toolchain for ${triple} (aws-lc-rs/cc-rs + final linker)."
  echo "   Install (Debian/Ubuntu): apt install $(apt_pkg_for "$triple")"
  echo "   On macOS: brew install <triple>-gcc or provide a vendor toolchain."
  echo "   Then set linker (e.g. ${ROOT}/.cargo/config.toml) and ${cc_env} for cc-rs."
  echo "   Example:"
  echo "     [target.${triple}]"
  echo "     linker = \"${cc}\""
  echo "     export ${cc_env}"
  echo "   Or re-run with --docker once docker/cross image exists (Task 3)."
  echo "   Or use --check-only / ALLOW_LINK_FAIL=1 on hosts without the linker."
}

docker_image_ready() {
  [[ -f "${ROOT}/docker/cross/Dockerfile" ]] && command -v docker >/dev/null 2>&1
}

# Returns:
#   0  docker cargo OK
#   1  image missing (caller may fall back to local)
#   2  docker cargo failed (caller must NOT fall back)
run_docker_build() {
  local triple="$1" mode="$2"
  local log="$3"
  local image="MemoryDataBase-cross"
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "   docker image not ready (no ${image} image; build docker/cross/Dockerfile first)"
    echo "   falling back to local cargo ${mode} --target ${triple}"
    return 1
  fi
  local cargo_args=(build --release --target "$triple" -p jx-server -p jx-cli)
  if [[ "$mode" == "check" ]]; then
    cargo_args=(check --target "$triple" -p jx-server -p jx-cli)
  fi
  if docker run --rm -v "${ROOT}:/src" -w /src "$image" cargo "${cargo_args[@]}"; then
    return 0
  fi
  return 2
}

TARGETS="$(resolve_targets)"

MODE="build"
if [[ "$CHECK_ONLY" -eq 1 ]]; then
  MODE="check"
fi

echo "== MemoryDataBase xinchuang cross-build =="
echo "cwd:     $ROOT"
echo "date:    $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "rustc:   $(rustc --version 2>/dev/null || echo unknown)"
echo "mode:    ${MODE}"
echo "docker:  ${DOCKER}"
echo "targets: ${TARGETS}"
echo "ALLOW_LINK_FAIL=${ALLOW_LINK_FAIL}"
echo ""

if [[ "$DOCKER" -eq 1 ]]; then
  if docker_image_ready; then
    echo "docker:  docker/cross/Dockerfile present"
  else
    echo "docker image not ready, falling back to local"
  fi
  echo ""
fi
# Summary columns: target | mode | result
SUMMARY_LINES=()
COMPILE_FAIL=0
TOOLCHAIN_FAIL=0
CHECK_FAIL=0

LOG="$(mktemp "${TMPDIR:-/tmp}/MemoryDataBase-cross-build.XXXXXX.log")"
trap 'rm -f "$LOG"' EXIT

for target in $TARGETS; do
  result="OK"
  mode_used="$MODE"

  echo "== rustup target add ${target} =="
  rustup target add "$target" 2>&1 | tee "$LOG" || {
    echo "=> ${target}: rustup target add FAILED"
    SUMMARY_LINES+=("  ${target}  ${mode_used}  TARGET_ADD_FAIL")
    COMPILE_FAIL=1
    echo ""
    continue
  }
  echo ""

  if [[ "$DOCKER" -eq 1 ]]; then
    echo "== docker cargo ${MODE} --target ${target} -p jx-server -p jx-cli =="
    docker_rc=0
    set +e
    run_docker_build "$target" "$MODE" "$LOG" 2>&1 | tee "$LOG"
    docker_rc=${PIPESTATUS[0]}
    set -e
    if [[ "$docker_rc" -eq 0 ]]; then
      echo "=> ${target}: docker ${MODE} OK"
      SUMMARY_LINES+=("  ${target}  docker-${MODE}  OK")
      echo ""
      continue
    elif [[ "$docker_rc" -eq 1 ]]; then
      # image missing — only allowed fallback to local
      echo "docker image not ready, falling back to local"
      mode_used="$MODE"
      echo ""
    else
      # docker cargo failed — no local fallback
      if [[ "$MODE" == "check" ]]; then
        result="CHECK_FAIL"
        CHECK_FAIL=1
        SUMMARY_LINES+=("  ${target}  docker-${MODE}  CHECK_FAIL")
        echo "=> ${target}: docker CHECK FAILED (no local fallback)"
      else
        result="BUILD_FAIL"
        COMPILE_FAIL=1
        SUMMARY_LINES+=("  ${target}  docker-${MODE}  BUILD_FAIL")
        echo "=> ${target}: docker BUILD FAILED (no local fallback)"
      fi
      echo ""
      continue
    fi
  fi

  local_cargo_cmd=()
  if [[ "$MODE" == "check" ]]; then
    local_cargo_cmd=(cargo check --target "$target" -p jx-server -p jx-cli)
  else
    local_cargo_cmd=(cargo build --release --target "$target" -p jx-server -p jx-cli)
  fi

  echo "== ${local_cargo_cmd[*]} =="
  if "${local_cargo_cmd[@]}" 2>&1 | tee "$LOG"; then
    echo "=> ${target}: ${MODE} OK"
    SUMMARY_LINES+=("  ${target}  ${mode_used}  OK")
  elif is_toolchain_error "$LOG"; then
    result="TOOLCHAIN_MISS"
    TOOLCHAIN_FAIL=1
    echo "=> ${target}: ${MODE} failed — missing cross toolchain (linker/cc-rs)"
    print_toolchain_guidance "$target"
    SUMMARY_LINES+=("  ${target}  ${mode_used}  TOOLCHAIN_MISS")
  else
    if [[ "$MODE" == "check" ]]; then
      result="CHECK_FAIL"
      CHECK_FAIL=1
      SUMMARY_LINES+=("  ${target}  ${mode_used}  CHECK_FAIL")
      echo "=> ${target}: CHECK FAILED (see log above)"
    else
      result="BUILD_FAIL"
      COMPILE_FAIL=1
      SUMMARY_LINES+=("  ${target}  ${mode_used}  BUILD_FAIL")
      echo "=> ${target}: BUILD FAILED (see log above)"
    fi
  fi
  echo ""
done

echo "== summary =="
printf '  %-32s  %-14s  %s\n' "TARGET" "MODE" "RESULT"
for line in "${SUMMARY_LINES[@]}"; do
  # lines are "  <target>  <mode>  <result>"
  # shellcheck disable=SC2086
  set -- $line
  printf '  %-32s  %-14s  %s\n' "$1" "$2" "$3"
done
echo ""

if [[ "$COMPILE_FAIL" -ne 0 || "$CHECK_FAIL" -ne 0 ]]; then
  echo "status: FAILED (compile/check errors)"
  exit 1
fi
if [[ "$TOOLCHAIN_FAIL" -ne 0 ]]; then
  if [[ "$ALLOW_LINK_FAIL" -eq 1 ]]; then
    echo "status: OK_WITH_TOOLCHAIN_GAP (ALLOW_LINK_FAIL=1; install cross gcc to finish release builds)"
    exit 0
  fi
  echo "status: FAILED (missing cross linker/cc-rs; guidance above)"
  echo "hint: ALLOW_LINK_FAIL=1 for check-only CI smoke without toolchain"
  exit 1
fi
echo "status: OK"
exit 0
