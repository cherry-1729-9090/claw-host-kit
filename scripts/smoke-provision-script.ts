import { buildProvisionScript } from '../src/index';

const script = buildProvisionScript({
  traefikCompose: {
    acmeEmail: 'you@example.com',
    wildcardDomain: 'h1.openclaw.example.com',
    vercelApiToken: 'token',
    vercelTeamId: 'team',
    enableDashboard: false,
    traefikImage: 'traefik:v3.1',
    certResolverName: 'le',
    entrypointName: 'websecure',
    entrypointPort: 443,
  },
  openclawRuntimeImage: 'openclaw-ttyd:local',
  composePath: '/opt/traefik/docker-compose.yml',
});

if (!script.includes('docker compose')) {
  throw new Error('Expected docker compose commands in provision script');
}

console.log('OK');

