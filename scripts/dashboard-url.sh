#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

INSTANCE_ID="${1:-}"
if [ -z "${INSTANCE_ID}" ]; then
  echo "Usage: $0 <instanceId>" >&2
  exit 1
fi

if [ -f .env ]; then
  # shellcheck disable=SC1091
  source .env
fi

: "${OPENCLAW_BASE_DOMAIN:?Missing OPENCLAW_BASE_DOMAIN}"
: "${OPENCLAW_HOST_SHARD:?Missing OPENCLAW_HOST_SHARD}"

OPENCLAW_SUBDOMAIN="${OPENCLAW_SUBDOMAIN:-openclaw}"
WILDCARD_DOMAIN="${OPENCLAW_HOST_SHARD}.${OPENCLAW_SUBDOMAIN}.${OPENCLAW_BASE_DOMAIN}"

CONTAINER="openclaw-${INSTANCE_ID}"
GATEWAY_TOKEN="$(docker exec "${CONTAINER}" openclaw config get gateway.auth.token 2>/dev/null | tr -d '[:space:]\"')"

if [ -z "${GATEWAY_TOKEN}" ] || [ "${GATEWAY_TOKEN}" = "null" ]; then
  echo "Failed to read gateway token from ${CONTAINER}. Is it running?" >&2
  exit 1
fi

echo "https://openclaw-${INSTANCE_ID}.${WILDCARD_DOMAIN}/overview?token=${GATEWAY_TOKEN}"

