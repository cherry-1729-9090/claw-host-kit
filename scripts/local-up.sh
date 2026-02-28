#!/usr/bin/env bash
set -euo pipefail

# One-command local demo:
# - Starts a local Traefik HTTPS entrypoint (self-signed cert) + forward-auth (token checks for /terminal)
# - Creates N OpenClaw instances behind Traefik (each on its own localtest.me subdomain)
#
# Usage:
#   ./scripts/local-up.sh            # 2 instances (demo1, demo2)
#   ./scripts/local-up.sh 4          # 4 instances (demo1..demo4)
#
# Environment overrides (optional):
#   OPENCLAW_INSTANCE_PREFIX=demo
#   OPENCLAW_LOCAL_HTTP_PORT=18090
#   OPENCLAW_LOCAL_DOMAIN=localtest.me
#   OPENCLAW_DOCKER_NETWORK=traefik_default
#   OPENCLAW_FORWARD_AUTH_IMAGE=openclaw-forward-auth:test
#   OPENCLAW_RUNTIME_IMAGE=openclaw-ttyd:local
#   OPENCLAW_BUILD_RUNTIME_IMAGE=1   # force rebuild runtime image
#
# Notes:
# - If OPENCLAW_TTYD_SECRET is not set, we generate one and store it at .data/ttyd-secret
#   so terminal URLs keep working across restarts.

cd "$(dirname "$0")/.."

COUNT="${1:-2}"
if ! [[ "${COUNT}" =~ ^[0-9]+$ ]] || [ "${COUNT}" -lt 1 ] || [ "${COUNT}" -gt 20 ]; then
  echo "Usage: $0 [count]" >&2
  echo "count must be an integer between 1 and 20" >&2
  exit 1
fi

if [ -f .env ]; then
  # shellcheck disable=SC1091
  source .env
fi

PREFIX="${OPENCLAW_INSTANCE_PREFIX:-demo}"
export OPENCLAW_LOCAL_HTTP_PORT="${OPENCLAW_LOCAL_HTTP_PORT:-18090}"
export OPENCLAW_LOCAL_DOMAIN="${OPENCLAW_LOCAL_DOMAIN:-localtest.me}"
export OPENCLAW_DOCKER_NETWORK="${OPENCLAW_DOCKER_NETWORK:-traefik_default}"

export OPENCLAW_FORWARD_AUTH_IMAGE="${OPENCLAW_FORWARD_AUTH_IMAGE:-openclaw-forward-auth:test}"
export OPENCLAW_RUNTIME_IMAGE="${OPENCLAW_RUNTIME_IMAGE:-openclaw-ttyd:local}"

SECRET_FILE="${OPENCLAW_TTYD_SECRET_FILE:-.data/ttyd-secret}"
if [ -z "${OPENCLAW_TTYD_SECRET:-}" ]; then
  if [ -f "${SECRET_FILE}" ]; then
    OPENCLAW_TTYD_SECRET="$(cat "${SECRET_FILE}")"
  else
    mkdir -p "$(dirname "${SECRET_FILE}")"
    if command -v openssl >/dev/null 2>&1; then
      OPENCLAW_TTYD_SECRET="$(openssl rand -hex 32)"
    else
      OPENCLAW_TTYD_SECRET="$(node -e "console.log(require('node:crypto').randomBytes(32).toString('hex'))")"
    fi
    printf '%s\n' "${OPENCLAW_TTYD_SECRET}" > "${SECRET_FILE}"
    chmod 0600 "${SECRET_FILE}" 2>/dev/null || true
  fi
  export OPENCLAW_TTYD_SECRET
fi

if ! docker info >/dev/null 2>&1; then
  echo "Docker does not seem to be running. Start Docker Desktop (macOS) or dockerd (Linux) and try again." >&2
  exit 1
fi

if ! docker image inspect "${OPENCLAW_FORWARD_AUTH_IMAGE}" >/dev/null 2>&1; then
  echo "Building forward-auth image: ${OPENCLAW_FORWARD_AUTH_IMAGE}"
  docker build -t "${OPENCLAW_FORWARD_AUTH_IMAGE}" docker/forward-auth >/dev/null
fi

if [ "${OPENCLAW_BUILD_RUNTIME_IMAGE:-0}" = "1" ]; then
  echo "Building runtime image: ${OPENCLAW_RUNTIME_IMAGE} (this can take a bit)"
  docker build -t "${OPENCLAW_RUNTIME_IMAGE}" docker/openclaw-ttyd
else
  if ! docker image inspect "${OPENCLAW_RUNTIME_IMAGE}" >/dev/null 2>&1; then
    echo "Runtime image ${OPENCLAW_RUNTIME_IMAGE} not found locally; building it now."
    docker build -t "${OPENCLAW_RUNTIME_IMAGE}" docker/openclaw-ttyd
  fi
fi

./scripts/local-frontdoor-up.sh

PORT="${OPENCLAW_LOCAL_HTTP_PORT}"
DOMAIN="${OPENCLAW_LOCAL_DOMAIN}"

IDS=()
for i in $(seq 1 "${COUNT}"); do
  id="${PREFIX}${i}"
  IDS+=("${id}")
  ./scripts/local-create-instance.sh "${id}"
done

echo
for id in "${IDS[@]}"; do
  host="openclaw-${id}.${DOMAIN}"
  token="$(OPENCLAW_TTYD_SECRET="${OPENCLAW_TTYD_SECRET}" ./scripts/terminal-token.sh "${id}")"
  echo "${id}"
  echo "  dashboard: https://${host}:${PORT}/overview"
  echo "  terminal:  https://${host}:${PORT}/terminal?token=${token}"
done
echo
echo "To connect the Dashboard UI:"
echo "  1. Open a terminal URL above"
echo "  2. Run: openclaw onboard"
echo "  3. Grab your dashboard token:"
echo "     sed -n 's/.*\"token\"[^\"]*\"\\([^\"]*\\)\".*/\\1/p' ~/.openclaw/openclaw.json"
echo "  4. Open the dashboard URL, paste the token into Gateway Token, and click Connect"

