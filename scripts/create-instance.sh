#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

INSTANCE_ID="${1:-}"
if [ -z "${INSTANCE_ID}" ]; then
  echo "Usage: $0 <instanceId>" >&2
  exit 1
fi

if [ "$(id -u)" -ne 0 ]; then
  echo "Run as root (or via sudo) because we write to /var/lib/openclaw and chown volumes." >&2
  exit 1
fi

if [ -f .env ]; then
  # shellcheck disable=SC1091
  source .env
fi

: "${OPENCLAW_BASE_DOMAIN:?Missing OPENCLAW_BASE_DOMAIN}"
: "${OPENCLAW_HOST_SHARD:?Missing OPENCLAW_HOST_SHARD}"

OPENCLAW_SUBDOMAIN="${OPENCLAW_SUBDOMAIN:-openclaw}"
OPENCLAW_RUNTIME_IMAGE="${OPENCLAW_RUNTIME_IMAGE:-openclaw-ttyd:local}"

WILDCARD_DOMAIN="${OPENCLAW_HOST_SHARD}.${OPENCLAW_SUBDOMAIN}.${OPENCLAW_BASE_DOMAIN}"
HOSTNAME="openclaw-${INSTANCE_ID}.${WILDCARD_DOMAIN}"
CONTAINER="openclaw-${INSTANCE_ID}"
DATA_DIR="/var/lib/openclaw/instances/${INSTANCE_ID}"

AUTH_URL="${OPENCLAW_AUTH_URL:-http://openclaw-forward-auth:8080/}"
NETWORK="${OPENCLAW_DOCKER_NETWORK:-traefik_default}"

CPU_LIMIT="${OPENCLAW_CPU_LIMIT:-2}"
MEMORY_LIMIT="${OPENCLAW_CONTAINER_MEMORY:-${OPENCLAW_MEMORY_LIMIT:-5120m}}"
MEMORY_RESERVATION="${OPENCLAW_MEMORY_RESERVATION:-4096m}"
PIDS_LIMIT="${OPENCLAW_PIDS_LIMIT:-512}"


mkdir -p "${DATA_DIR}"
chown 1000:1000 "${DATA_DIR}"

docker pull "${OPENCLAW_RUNTIME_IMAGE}" >/dev/null 2>&1 || true

docker run -d \
  --name "${CONTAINER}" \
  --restart unless-stopped \
  --network "${NETWORK}" \
  --cpus="${CPU_LIMIT}" \
  --memory-reservation="${MEMORY_RESERVATION}" \
  --memory="${MEMORY_LIMIT}" \
  --memory-swap="${MEMORY_LIMIT}" \
  --pids-limit="${PIDS_LIMIT}" \
  -v "${DATA_DIR}:/home/node/.openclaw" \
  --label 'traefik.enable=true' \
  --label "traefik.docker.network=${NETWORK}" \
  --label "traefik.http.routers.${CONTAINER}.rule=Host(\`${HOSTNAME}\`)" \
  --label "traefik.http.routers.${CONTAINER}.service=${CONTAINER}" \
  --label "traefik.http.routers.${CONTAINER}.entrypoints=websecure" \
  --label "traefik.http.routers.${CONTAINER}.tls=true" \
  --label "traefik.http.routers.${CONTAINER}.tls.certresolver=le" \
  --label "traefik.http.routers.${CONTAINER}.tls.domains[0].main=${WILDCARD_DOMAIN}" \
  --label "traefik.http.routers.${CONTAINER}.tls.domains[0].sans=*.${WILDCARD_DOMAIN}" \
  --label "traefik.http.services.${CONTAINER}.loadbalancer.server.port=18789" \
  --label "traefik.http.routers.${CONTAINER}-terminal.rule=Host(\`${HOSTNAME}\`) && PathPrefix(\`/terminal\`)" \
  --label "traefik.http.routers.${CONTAINER}-terminal.service=${CONTAINER}-terminal" \
  --label "traefik.http.routers.${CONTAINER}-terminal.priority=100" \
  --label "traefik.http.routers.${CONTAINER}-terminal.entrypoints=websecure" \
  --label "traefik.http.routers.${CONTAINER}-terminal.tls=true" \
  --label "traefik.http.routers.${CONTAINER}-terminal.tls.certresolver=le" \
  --label "traefik.http.routers.${CONTAINER}-terminal.tls.domains[0].main=${WILDCARD_DOMAIN}" \
  --label "traefik.http.routers.${CONTAINER}-terminal.tls.domains[0].sans=*.${WILDCARD_DOMAIN}" \
  --label "traefik.http.middlewares.${CONTAINER}-terminal-strip.stripprefix.prefixes=/terminal" \
  --label "traefik.http.middlewares.${CONTAINER}-terminal-strip.stripprefix.forceSlash=true" \
  --label "traefik.http.middlewares.${CONTAINER}-inject-id.headers.customrequestheaders.X-Openclaw-Instance-Id=${INSTANCE_ID}" \
  --label "traefik.http.middlewares.${CONTAINER}-auth.forwardauth.address=${AUTH_URL}" \
  --label "traefik.http.middlewares.${CONTAINER}-auth.forwardauth.trustForwardHeader=true" \
  --label "traefik.http.routers.${CONTAINER}-terminal.middlewares=${CONTAINER}-inject-id,${CONTAINER}-auth,${CONTAINER}-terminal-strip" \
  --label "traefik.http.services.${CONTAINER}-terminal.loadbalancer.server.port=7681" \
  "${OPENCLAW_RUNTIME_IMAGE}"

echo
echo "Instance created: ${INSTANCE_ID}"
echo "Terminal URL:"
OPENCLAW_BASE_DOMAIN="${OPENCLAW_BASE_DOMAIN}" OPENCLAW_HOST_SHARD="${OPENCLAW_HOST_SHARD}" OPENCLAW_SUBDOMAIN="${OPENCLAW_SUBDOMAIN}" OPENCLAW_TTYD_SECRET="${OPENCLAW_TTYD_SECRET:-}" ./scripts/terminal-url.sh "${INSTANCE_ID}" || true
