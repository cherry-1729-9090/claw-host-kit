#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [ "$(id -u)" -ne 0 ]; then
  echo "Run as root (or via sudo)." >&2
  exit 1
fi

if [ -f .env ]; then
  # shellcheck disable=SC1091
  source .env
fi

: "${OPENCLAW_BASE_DOMAIN:?Missing OPENCLAW_BASE_DOMAIN}"
: "${OPENCLAW_HOST_SHARD:?Missing OPENCLAW_HOST_SHARD}"
: "${OPENCLAW_ACME_EMAIL:?Missing OPENCLAW_ACME_EMAIL}"
: "${OPENCLAW_CF_DNS_API_TOKEN:?Missing OPENCLAW_CF_DNS_API_TOKEN}"
: "${OPENCLAW_TTYD_SECRET:?Missing OPENCLAW_TTYD_SECRET}"

OPENCLAW_SUBDOMAIN="${OPENCLAW_SUBDOMAIN:-openclaw}"
export OPENCLAW_WILDCARD_DOMAIN="${OPENCLAW_HOST_SHARD}.${OPENCLAW_SUBDOMAIN}.${OPENCLAW_BASE_DOMAIN}"

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y ca-certificates curl gnupg lsb-release

if ! command -v docker > /dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sh
fi

systemctl enable --now docker

if ! docker compose version > /dev/null 2>&1; then
  apt-get install -y docker-compose-plugin
fi

if ! command -v node > /dev/null 2>&1; then
  curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
  apt-get install -y nodejs
fi

npm install --prefix "$(pwd)" --omit=dev

mkdir -p /opt/traefik
touch /opt/traefik/acme.json
chmod 600 /opt/traefik/acme.json

docker compose -p traefik -f deploy/traefik/docker-compose.yml up -d --build

HOST_KIT_DIR="$(pwd)"
cat > /etc/systemd/system/vps-agent.service << EOF
[Unit]
Description=OpenClaw VPS Agent
After=network.target docker.service
Requires=docker.service

[Service]
Type=simple
WorkingDirectory=${HOST_KIT_DIR}
EnvironmentFile=${HOST_KIT_DIR}/.env
ExecStart=/usr/bin/node ${HOST_KIT_DIR}/vps-agent.js
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now vps-agent

echo
echo "Traefik is up."
echo "vps-agent is up on port ${VPS_AGENT_PORT:-4444}."
echo "Expected wildcard DNS (A record): *.${OPENCLAW_WILDCARD_DOMAIN} -> <this host IP>"

if [ -n "${OPENCLAW_CONTROL_PLANE_URL:-}" ] && [ -n "${OPENCLAW_INTERNAL_SECRET:-}" ]; then
  VPS_IP="$(curl -sf https://api.ipify.org || curl -sf https://ifconfig.me || echo '')"
  if [ -n "${VPS_IP}" ]; then
    curl -sf -X POST "${OPENCLAW_CONTROL_PLANE_URL}/api/webhooks/node-register" \
      -H "Content-Type: application/json" \
      -H "X-Internal-Secret: ${OPENCLAW_INTERNAL_SECRET}" \
      -d "{\"ip\":\"${VPS_IP}\",\"shard\":\"${OPENCLAW_HOST_SHARD}\",\"baseDomain\":\"${OPENCLAW_BASE_DOMAIN}\",\"ttydSecret\":\"${OPENCLAW_TTYD_SECRET}\"}" \
      && echo "Registered with control plane (IP: ${VPS_IP})" \
      || echo "Warning: Could not reach control plane."
  fi
fi
