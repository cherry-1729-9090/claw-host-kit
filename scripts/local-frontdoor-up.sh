#!/usr/bin/env bash
set -euo pipefail

# Local dev helper: run Traefik (HTTPS, self-signed cert) + openclaw-forward-auth on a shared Docker network.
# This is meant for quick smoke tests on your laptop (no DNS setup needed).
#
# Most people should run:
#   ./scripts/local-up.sh 2

TRAEFIK_CONTAINER="${OPENCLAW_TRAEFIK_CONTAINER:-openclaw-traefik}"
FORWARD_AUTH_CONTAINER="${OPENCLAW_FORWARD_AUTH_CONTAINER:-openclaw-forward-auth}"
FORWARD_AUTH_IMAGE="${OPENCLAW_FORWARD_AUTH_IMAGE:-openclaw-forward-auth:test}"
NETWORK="${OPENCLAW_DOCKER_NETWORK:-traefik_default}"
PORT="${OPENCLAW_LOCAL_HTTP_PORT:-18090}"

: "${OPENCLAW_TTYD_SECRET:?Missing OPENCLAW_TTYD_SECRET (set it in your shell or in .env)}"

docker network inspect "${NETWORK}" >/dev/null 2>&1 || docker network create "${NETWORK}" >/dev/null

docker rm -f "${TRAEFIK_CONTAINER}" >/dev/null 2>&1 || true
docker run -d \
  --name "${TRAEFIK_CONTAINER}" \
  --network "${NETWORK}" \
  -p "${PORT}:8080" \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
  traefik:v3.1 \
  --providers.docker=true \
  --providers.docker.exposedbydefault=false \
  --entrypoints.websecure.address=:8080 \
  --entrypoints.websecure.http.tls=true \
  --log.level=INFO >/dev/null

docker rm -f "${FORWARD_AUTH_CONTAINER}" >/dev/null 2>&1 || true
docker run -d \
  --name "${FORWARD_AUTH_CONTAINER}" \
  --network "${NETWORK}" \
  -e OPENCLAW_TTYD_SECRET="${OPENCLAW_TTYD_SECRET}" \
  -e OPENCLAW_TTYD_TTL_SECONDS="${OPENCLAW_TTYD_TTL_SECONDS:-86400}" \
  "${FORWARD_AUTH_IMAGE}" >/dev/null

