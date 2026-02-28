export interface TraefikComposeInput {
  acmeEmail: string;
  wildcardDomain: string;
  cfDnsApiToken: string;
  enableDashboard?: boolean;
  traefikImage?: string;
  certResolverName?: string;
  entrypointName?: string;
  entrypointPort?: number;
  dnsCheckDelay?: number;
}

export function buildTraefikComposeYaml(input: TraefikComposeInput): string {
  const traefikImage = input.traefikImage || 'traefik:v3.1';
  const certResolverName = input.certResolverName || 'le';
  const entrypointName = input.entrypointName || 'websecure';
  const entrypointPort = input.entrypointPort ?? 443;
  const dnsCheckDelay = input.dnsCheckDelay ?? 10;

  const enableDashboard = !!input.enableDashboard;
  const dashboardCmd = enableDashboard ? '      - "--api.dashboard=true"' : '';
  const dashboardPort = enableDashboard ? '      - "8080:8080"' : '';

  return [
    'version: "3.9"',
    'services:',
    '  traefik:',
    `    image: ${traefikImage}`,
    '    container_name: traefik',
    '    restart: unless-stopped',
    '    command:',
    '      - "--providers.docker=true"',
    '      - "--providers.docker.exposedbydefault=false"',
    `      - "--entrypoints.${entrypointName}.address=:${entrypointPort}"`,
    `      - "--certificatesresolvers.${certResolverName}.acme.email=${input.acmeEmail}"`,
    `      - "--certificatesresolvers.${certResolverName}.acme.storage=/acme.json"`,
    `      - "--certificatesresolvers.${certResolverName}.acme.dnschallenge=true"`,
    `      - "--certificatesresolvers.${certResolverName}.acme.dnschallenge.provider=cloudflare"`,
    `      - "--certificatesresolvers.${certResolverName}.acme.dnschallenge.resolvers=1.1.1.1:53,8.8.8.8:53"`,
    `      - "--certificatesresolvers.${certResolverName}.acme.dnschallenge.delaybeforecheck=${dnsCheckDelay}"`,
    `      - "--entrypoints.${entrypointName}.http.tls.certresolver=${certResolverName}"`,
    `      - "--entrypoints.${entrypointName}.http.tls.domains[0].main=${input.wildcardDomain}"`,
    `      - "--entrypoints.${entrypointName}.http.tls.domains[0].sans=*.${input.wildcardDomain}"`,
    dashboardCmd,
    '    environment:',
    `      - CF_DNS_API_TOKEN=${input.cfDnsApiToken}`,
    '    ports:',
    `      - "${entrypointPort}:${entrypointPort}"`,
    dashboardPort,
    '    volumes:',
    '      - /var/run/docker.sock:/var/run/docker.sock:ro',
    '      - /opt/traefik/acme.json:/acme.json',
  ].filter(Boolean).join('\n');
}
