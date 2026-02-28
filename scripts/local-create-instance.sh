#!/usr/bin/env bash
set -euo pipefail

# Local dev helper: starts one OpenClaw instance behind local Traefik+forward-auth (HTTPS, self-signed cert).
#
# Usage:
#   ./scripts/local-up.sh
#   ./scripts/local-create-instance.sh <instanceId>
#
# Then open:
#   https://openclaw-<instanceId>.localtest.me:<PORT>/
#   https://openclaw-<instanceId>.localtest.me:<PORT>/terminal?token=...

cd "$(dirname "$0")/.."

INSTANCE_ID="${1:-}"
if [ -z "${INSTANCE_ID}" ]; then
  echo "Usage: $0 <instanceId>" >&2
  exit 1
fi

SECRET_FILE="${OPENCLAW_TTYD_SECRET_FILE:-.data/ttyd-secret}"
if [ -z "${OPENCLAW_TTYD_SECRET:-}" ] && [ -f "${SECRET_FILE}" ]; then
  OPENCLAW_TTYD_SECRET="$(cat "${SECRET_FILE}")"
  export OPENCLAW_TTYD_SECRET
fi

: "${OPENCLAW_TTYD_SECRET:?Missing OPENCLAW_TTYD_SECRET (run ./scripts/local-up.sh first, or set it in your shell/.env)}"

RUNTIME_IMAGE="${OPENCLAW_RUNTIME_IMAGE:-openclaw-ttyd:local}"
NETWORK="${OPENCLAW_DOCKER_NETWORK:-traefik_default}"
PORT="${OPENCLAW_LOCAL_HTTP_PORT:-18090}"
DOMAIN="${OPENCLAW_LOCAL_DOMAIN:-localtest.me}"
FORWARD_AUTH_CONTAINER="${OPENCLAW_FORWARD_AUTH_CONTAINER:-openclaw-forward-auth}"
FORWARD_AUTH_URL="${OPENCLAW_AUTH_URL:-http://${FORWARD_AUTH_CONTAINER}:8080/}"

CONTAINER="openclaw-${INSTANCE_ID}"
HOSTNAME="openclaw-${INSTANCE_ID}.${DOMAIN}"
DATA_DIR="${OPENCLAW_DATA_DIR_BASE:-$(pwd)/.data/instances}/${INSTANCE_ID}"

mkdir -p "${DATA_DIR}"

docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true

docker run -d \
  --name "${CONTAINER}" \
  --restart unless-stopped \
  --network "${NETWORK}" \
  -v "${DATA_DIR}:/home/node/.openclaw" \
  --label 'traefik.enable=true' \
  --label "traefik.http.routers.${CONTAINER}.rule=Host(\`${HOSTNAME}\`)" \
  --label "traefik.http.routers.${CONTAINER}.service=${CONTAINER}" \
  --label "traefik.http.routers.${CONTAINER}.entrypoints=websecure" \
  --label "traefik.http.services.${CONTAINER}.loadbalancer.server.port=18789" \
  --label "traefik.http.routers.${CONTAINER}-terminal.rule=Host(\`${HOSTNAME}\`) && PathPrefix(\`/terminal\`)" \
  --label "traefik.http.routers.${CONTAINER}-terminal.service=${CONTAINER}-terminal" \
  --label "traefik.http.routers.${CONTAINER}-terminal.entrypoints=websecure" \
  --label "traefik.http.routers.${CONTAINER}-terminal.priority=100" \
  --label "traefik.http.middlewares.${CONTAINER}-terminal-strip.stripprefix.prefixes=/terminal" \
  --label "traefik.http.middlewares.${CONTAINER}-terminal-strip.stripprefix.forceSlash=true" \
  --label "traefik.http.middlewares.${CONTAINER}-inject-id.headers.customrequestheaders.X-Openclaw-Instance-Id=${INSTANCE_ID}" \
  --label "traefik.http.middlewares.${CONTAINER}-auth.forwardauth.address=${FORWARD_AUTH_URL}" \
  --label "traefik.http.middlewares.${CONTAINER}-auth.forwardauth.trustForwardHeader=true" \
  --label "traefik.http.routers.${CONTAINER}-terminal.middlewares=${CONTAINER}-inject-id,${CONTAINER}-auth,${CONTAINER}-terminal-strip" \
  --label "traefik.http.services.${CONTAINER}-terminal.loadbalancer.server.port=7681" \
  "${RUNTIME_IMAGE}" >/dev/null

TOKEN="$(OPENCLAW_TTYD_SECRET="${OPENCLAW_TTYD_SECRET}" ./scripts/terminal-token.sh "${INSTANCE_ID}")"

ready=0
for _ in $(seq 1 120); do
  code="$(curl -sk -o /dev/null -w "%{http_code}" -H "Host: ${HOSTNAME}" "https://localhost:${PORT}/" || true)"
  case "${code}" in
    200)
      ready=1
      break
      ;;
    000|404|502|503|504)
      sleep 0.5
      ;;
    *)
      ready=1
      break
      ;;
  esac
done

if [ "${ready}" != "1" ]; then
  echo "Warning: ${INSTANCE_ID} did not become ready in time (last status=${code})." >&2
fi
