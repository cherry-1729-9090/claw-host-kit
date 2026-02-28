import { buildTraefikComposeYaml } from './traefik';

export interface ProvisionScriptInput {
  traefikCompose: Parameters<typeof buildTraefikComposeYaml>[0];
  openclawRuntimeImage?: string;
  composePath?: string;
}

export function buildProvisionScript(input: ProvisionScriptInput): string {
  const composePath = input.composePath || '/opt/traefik/docker-compose.yml';
  const composeYaml = buildTraefikComposeYaml(input.traefikCompose);
  const sudoLiteral = '${SUDO}';
  const dockerPull = input.openclawRuntimeImage ? `${sudoLiteral} docker pull ${input.openclawRuntimeImage}` : '';

  return [
    'set -euo pipefail',
    'export DEBIAN_FRONTEND=noninteractive',
    'if [ "$(id -u)" -ne 0 ]; then SUDO="sudo -n"; else SUDO=""; fi',
    '${SUDO} apt-get update -y',
    '${SUDO} apt-get install -y ca-certificates curl gnupg lsb-release',
    'if ! command -v docker >/dev/null 2>&1; then curl -fsSL https://get.docker.com | ${SUDO} sh; fi',
    '${SUDO} systemctl enable --now docker',
    'if ! docker compose version >/dev/null 2>&1; then ${SUDO} apt-get install -y docker-compose-plugin; fi',
    'DAEMON_JSON="/etc/docker/daemon.json"',
    'if [[ ! -s "${DAEMON_JSON}" ]] || [[ "$(tr -d \' \\n\\t\' < "${DAEMON_JSON}" 2>/dev/null || echo \'\')" == "{}" ]]; then',
    '  ${SUDO} tee "${DAEMON_JSON}" >/dev/null <<\'JSON\'',
    '{',
    '  "min-api-version": "1.24"',
    '}',
    'JSON',
    '  ${SUDO} systemctl restart docker',
    'fi',
    '${SUDO} mkdir -p /opt/traefik',
    '${SUDO} touch /opt/traefik/acme.json',
    '${SUDO} chmod 600 /opt/traefik/acme.json',
    `${sudoLiteral} tee ${composePath} >/dev/null <<'YAML'`,
    composeYaml,
    'YAML',
    `${sudoLiteral} docker compose -f ${composePath} up -d`,
    dockerPull,
  ].filter(Boolean).join('\n');
}
