#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TARGET="${1:-debian12}"
RUN_ID="pfapi-int-$$-$(date +%s)"
TMP_DIR="$(mktemp -d)"

HOST_OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$(uname -m)" in
  x86_64|amd64) HOST_ARCH="amd64" ;;
  arm64|aarch64) HOST_ARCH="arm64" ;;
  *)
    echo "ERROR: unsupported host architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

cleanup() {
  local rc=$?
  if command -v docker >/dev/null 2>&1; then
    local cids
    cids="$(docker ps -aq --filter "label=postfix-api-test-run=${RUN_ID}" 2>/dev/null || true)"
    if [ -n "${cids}" ]; then
      while IFS= read -r cid; do
        if [ -n "${cid}" ]; then
          docker rm -f "${cid}" >/dev/null 2>&1 || true
        fi
      done <<< "${cids}"
    fi
  fi
  if [ -n "${TMP_DIR:-}" ] && [ -d "${TMP_DIR}" ]; then
    rm -rf "${TMP_DIR}"
  fi
  exit "${rc}"
}
trap cleanup EXIT INT TERM

usage() {
  echo "Usage: $0 [debian12|alpine|latest|boky|all]" >&2
}

use_host_go() {
  command -v go >/dev/null 2>&1 || return 1
  local ver major minor
  ver="$(go env GOVERSION 2>/dev/null || true)"
  [ -n "${ver}" ] || return 1
  major="$(printf '%s' "${ver}" | sed -nE 's/^go([0-9]+)\.([0-9]+).*/\1/p')"
  minor="$(printf '%s' "${ver}" | sed -nE 's/^go([0-9]+)\.([0-9]+).*/\2/p')"
  [ -n "${major}" ] && [ -n "${minor}" ] || return 1
  if [ "${major}" -gt 1 ] || { [ "${major}" -eq 1 ] && [ "${minor}" -ge 27 ]; }; then
    return 0
  fi
  return 1
}

build_image() {
  local target="$1"
  local image="$2"
  local dockerfile=""
  case "${target}" in
    debian12) dockerfile="${REPO_ROOT}/docker/test/Dockerfile.debian12" ;;
    alpine)   dockerfile="${REPO_ROOT}/docker/test/Dockerfile.alpine" ;;
    latest)   dockerfile="${REPO_ROOT}/docker/test/Dockerfile.latest" ;;
    boky)     dockerfile="${REPO_ROOT}/docker/boky/Dockerfile" ;;
    *)
      echo "ERROR: unknown target '${target}'" >&2
      usage
      exit 2
      ;;
  esac

  echo "==> Building Docker image ${image} from ${dockerfile} (platform linux/${HOST_ARCH})..."
  if docker buildx version >/dev/null 2>&1; then
    docker buildx build --load --platform "linux/${HOST_ARCH}" \
      --build-arg "TARGETOS=linux" --build-arg "TARGETARCH=${HOST_ARCH}" \
      -f "${dockerfile}" -t "${image}" "${REPO_ROOT}"
  else
    DOCKER_BUILDKIT=1 docker build --platform "linux/${HOST_ARCH}" \
      --build-arg "TARGETOS=linux" --build-arg "TARGETARCH=${HOST_ARCH}" \
      -f "${dockerfile}" -t "${image}" "${REPO_ROOT}"
  fi
}

run_target() {
  local target="$1"
  local image="postfix-http-api-test:${target}"

  build_image "${target}" "${image}"

  local container_arch
  container_arch="$(docker image inspect --format '{{.Architecture}}' "${image}" 2>/dev/null || echo "${HOST_ARCH}")"
  case "${container_arch}" in
    amd64|x86_64) container_arch="amd64" ;;
    arm64|aarch64) container_arch="arm64" ;;
    *) container_arch="${HOST_ARCH}" ;;
  esac

  local out_dir="${TMP_DIR}/bin-${target}"
  mkdir -p "${out_dir}"
  local helper_bin="${out_dir}/testhelper"

  cd "${REPO_ROOT}"

  if use_host_go; then
    echo "==> Running integration suite for target=${target} using host Go ($(go version))..."
    CGO_ENABLED=0 GOOS=linux GOARCH="${container_arch}" \
      go build -o "${helper_bin}" ./tests/integration/cmd/testhelper
    POSTFIX_TEST_TARGET="${target}" \
    POSTFIX_TEST_IMAGE="${image}" \
    POSTFIX_TEST_RUN_ID="${RUN_ID}" \
    POSTFIX_TEST_HELPER="${helper_bin}" \
      go test -v -count=1 -timeout 20m -tags=integration ./tests/integration/...
  else
    echo "==> Compiling integration suite via golang:1.27.4 container for ${HOST_OS}/${HOST_ARCH} (target=${target})..."
    docker run --rm \
      -v "${REPO_ROOT}:/src:ro" \
      -v "${out_dir}:/out" \
      -w /src \
      golang:1.27.4 \
      sh -c "set -eu; \
        CGO_ENABLED=0 GOOS=linux GOARCH=${container_arch} go build -o /out/testhelper ./tests/integration/cmd/testhelper; \
        CGO_ENABLED=0 GOOS=${HOST_OS} GOARCH=${HOST_ARCH} go test -c -tags=integration -o /out/integration.test ./tests/integration"

    echo "==> Running integration suite for target=${target}..."
    POSTFIX_TEST_TARGET="${target}" \
    POSTFIX_TEST_IMAGE="${image}" \
    POSTFIX_TEST_RUN_ID="${RUN_ID}" \
    POSTFIX_TEST_HELPER="${helper_bin}" \
      "${out_dir}/integration.test" -test.v -test.timeout=20m
  fi
}

case "${TARGET}" in
  debian12|alpine|latest|boky)
    run_target "${TARGET}"
    ;;
  all)
    for t in debian12 alpine boky latest; do
      run_target "${t}"
    done
    ;;
  -h|--help|help)
    usage
    exit 0
    ;;
  *)
    echo "ERROR: invalid target '${TARGET}'" >&2
    usage
    exit 2
    ;;
esac
