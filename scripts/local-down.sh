#!/usr/bin/env bash
set -euo pipefail

TRAEFIK_CONTAINER="${OPENCLAW_TRAEFIK_CONTAINER:-openclaw-traefik}"
FORWARD_AUTH_CONTAINER="${OPENCLAW_FORWARD_AUTH_CONTAINER:-openclaw-forward-auth}"

docker rm -f "${TRAEFIK_CONTAINER}" >/dev/null 2>&1 || true
docker rm -f "${FORWARD_AUTH_CONTAINER}" >/dev/null 2>&1 || true

echo "Local stack removed (instances are left running)."

