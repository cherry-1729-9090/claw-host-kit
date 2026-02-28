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

TOKEN="$(./scripts/terminal-token.sh "${INSTANCE_ID}")"

echo "https://openclaw-${INSTANCE_ID}.${WILDCARD_DOMAIN}/terminal?token=${TOKEN}"

