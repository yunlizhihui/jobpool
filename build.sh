#!/usr/bin/env bash

set -euo pipefail

source ./scripts/test_lib.sh

GIT_SHA=$(git rev-parse --short HEAD || echo "GitNotFound")
if [[ -n "${FAILPOINTS:-}" ]]; then
  GIT_SHA="$GIT_SHA"-FAILPOINTS
fi

VERSION_SYMBOL="${ROOT_MODULE}/v2/version.GitSHA"

# use go env if noset
GOOS=${GOOS:-$(go env GOOS)}
GOARCH=${GOARCH:-$(go env GOARCH)}
# -------------配置项（打正式镜像的时候写死）-------------
# 注意：下面的配置可以强制要求按照linux和amd64去打包
GOOS=linux
GOARCH=amd64
#GOARCH=arm64

# Set GO_LDFLAGS="-s" for building without symbols for debugging.
# shellcheck disable=SC2206
GO_LDFLAGS=(${GO_LDFLAGS:-} "-X=${VERSION_SYMBOL}=${GIT_SHA}")
GO_BUILD_ENV=("CGO_ENABLED=0" "GO_BUILD_FLAGS=${GO_BUILD_FLAGS:-}" "GOOS=${GOOS}" "GOARCH=${GOARCH}")

# enable/disable failpoints
toggle_failpoints() {
  mode="$1"
  if command -v gofail >/dev/null 2>&1; then
    run gofail "$mode" server/etcdserver/ server/mvcc/backend/ server/wal/
  elif [[ "$mode" != "disable" ]]; then
    log_error "FAILPOINTS set but gofail not found"
    exit 1
  fi
}

toggle_failpoints_default() {
  mode="disable"
  if [[ -n "${FAILPOINTS:-}" ]]; then mode="enable"; fi
  toggle_failpoints "$mode"
}

jobpool_build() {
  out="bin"
  if [[ -n "${BINDIR:-}" ]]; then out="${BINDIR}"; fi
  toggle_failpoints_default

  run rm -f "${out}/jobpool"
  (
    cd ./server
    # Static compilation is useful when jobpool is run in a container. $GO_BUILD_FLAGS is OK
    # shellcheck disable=SC2086
    run env "${GO_BUILD_ENV[@]}" go build ${GO_BUILD_FLAGS:-} \
      -trimpath \
      -installsuffix=cgo \
      "-ldflags=${GO_LDFLAGS[*]}" \
      -o="../${out}/jobpool" . || return 2
  ) || return 2

  run rm -f "${out}/jobpoolctl"
  # shellcheck disable=SC2086
  (
    cd ./jobpoolctl
    run env GO_BUILD_FLAGS="${GO_BUILD_FLAGS:-}" "${GO_BUILD_ENV[@]}" go build ${GO_BUILD_FLAGS:-} \
      -trimpath \
      -installsuffix=cgo \
      "-ldflags=${GO_LDFLAGS[*]}" \
      -o="../${out}/jobpoolctl" . || return 2
  ) || return 2
  # Verify whether symbol we overriden exists
  run rm -f "${out}/jobpool_agent"
  (
      cd ./agent
      run env GO_BUILD_FLAGS="${GO_BUILD_FLAGS:-}" "${GO_BUILD_ENV[@]}" go build ${GO_BUILD_FLAGS:-} \
        -trimpath \
        -installsuffix=cgo \
        "-ldflags=${GO_LDFLAGS[*]}" \
        -o="../${out}/jobpool_agent" . || return 2
    ) || return 2

  # For cross-compiling we cannot run: ${out}/jobpool --version | grep -q "Git SHA: ${GIT_SHA}"

  # We need symbols to do this check:
  if [[ "${GO_LDFLAGS[*]}" != *"-s"* ]]; then
    go tool nm "${out}/jobpool" | grep "${VERSION_SYMBOL}" > /dev/null
    if [[ "${PIPESTATUS[*]}" != "0 0" ]]; then
      log_error "FAIL: Symbol ${VERSION_SYMBOL} not found in binary: ${out}/jobpool"
      return 2
    fi
  fi
}

tools_build() {
  out="bin"
  if [[ -n "${BINDIR:-}" ]]; then out="${BINDIR}"; fi
  tools_path="tools/benchmark
    tools/etcd-dump-db
    tools/etcd-dump-logs
    tools/local-tester/bridge"
  for tool in ${tools_path}
  do
    echo "Building" "'${tool}'"...
    run rm -f "${out}/${tool}"
    # shellcheck disable=SC2086
    run env GO_BUILD_FLAGS="${GO_BUILD_FLAGS:-}" CGO_ENABLED=0 go build ${GO_BUILD_FLAGS:-} \
      -trimpath \
      -installsuffix=cgo \
      "-ldflags=${GO_LDFLAGS[*]}" \
      -o="${out}/${tool}" "./${tool}" || return 2
  done
  tests_build "${@}"
}

tests_build() {
  out="bin"
  if [[ -n "${BINDIR:-}" ]]; then out="${BINDIR}"; fi
  tools_path="
    functional/cmd/etcd-agent
    functional/cmd/etcd-proxy
    functional/cmd/etcd-runner
    functional/cmd/etcd-tester"
  (
    cd tests || exit 2
    for tool in ${tools_path}; do
      echo "Building" "'${tool}'"...
      run rm -f "../${out}/${tool}"

      # shellcheck disable=SC2086
      run env CGO_ENABLED=0 GO_BUILD_FLAGS="${GO_BUILD_FLAGS:-}" go build ${GO_BUILD_FLAGS:-} \
        -installsuffix=cgo \
        "-ldflags=${GO_LDFLAGS[*]}" \
        -o="../${out}/${tool}" "./${tool}" || return 2
    done
  ) || return 2
}

toggle_failpoints_default

# only build when called directly, not sourced
if echo "$0" | grep -E "build(.sh)?$" >/dev/null; then
  if jobpool_build; then
    log_success "SUCCESS: jobpool_build (GOARCH=${GOARCH})"
  else
    log_error "FAIL: jobpool_build (GOARCH=${GOARCH})"
    exit 2
  fi
fi
