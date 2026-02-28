import { buildInstanceUrls, InstanceUrls } from './urls';

export interface DockerResourceLimits {
  cpuLimit?: string;
  memoryReservation?: string;
  memoryLimit?: string;
  pidsLimit?: number;
}

export interface BuildDockerRunCommandInput extends DockerResourceLimits {
  instanceId: string;
  runtimeImage: string;
  hostShard: string;
  baseDomain: string;
  subdomain?: string;
  terminalToken: string;
  authUrl: string;

  dockerNetwork?: string;
  entrypoint?: string;
  certResolver?: string;
  containerNamePrefix?: string;
  dataDirBase?: string;
  gatewayPort?: number;
  ttydPort?: number;
}

export interface DockerRunResult {
  runCommand: string;
  containerName: string;
  dataDir: string;
  openclawUrl: string;
  ttydUrl: string;
  hostName: string;
  wildcardDomain: string;
  labels: string[];
}

const DEFAULTS = {
  dockerNetwork: 'traefik_default',
  entrypoint: 'websecure',
  certResolver: 'le',
  containerNamePrefix: 'openclaw-',
  dataDirBase: '/var/lib/openclaw/instances',
  gatewayPort: 18789,
  ttydPort: 7681,
  cpuLimit: '2',
  memoryReservation: '4g',
  memoryLimit: '6g',
  pidsLimit: 512,
} as const;

export function buildDockerRunCommand(input: BuildDockerRunCommandInput): DockerRunResult {
  const dockerNetwork = input.dockerNetwork || DEFAULTS.dockerNetwork;
  const entrypoint = input.entrypoint || DEFAULTS.entrypoint;
  const certResolver = input.certResolver || DEFAULTS.certResolver;
  const containerNamePrefix = input.containerNamePrefix || DEFAULTS.containerNamePrefix;
  const dataDirBase = input.dataDirBase || DEFAULTS.dataDirBase;
  const gatewayPort = input.gatewayPort ?? DEFAULTS.gatewayPort;
  const ttydPort = input.ttydPort ?? DEFAULTS.ttydPort;

  const cpuLimit = input.cpuLimit || DEFAULTS.cpuLimit;
  const memoryReservation = input.memoryReservation || DEFAULTS.memoryReservation;
  const memoryLimit = input.memoryLimit || DEFAULTS.memoryLimit;
  const pidsLimit = input.pidsLimit ?? DEFAULTS.pidsLimit;

  const urls: InstanceUrls = buildInstanceUrls({
    instanceId: input.instanceId,
    hostShard: input.hostShard,
    baseDomain: input.baseDomain,
    subdomain: input.subdomain,
    terminalToken: input.terminalToken,
    instancePrefix: containerNamePrefix,
  });

  const containerName = `${containerNamePrefix}${input.instanceId}`;
  const dataDir = `${dataDirBase}/${input.instanceId}`;

  const labels = [
    'traefik.enable=true',
    `traefik.docker.network=${dockerNetwork}`,
    `traefik.http.routers.${containerName}.rule=Host(\`${urls.hostName}\`)`,
    `traefik.http.routers.${containerName}.service=${containerName}`,
    `traefik.http.routers.${containerName}.entrypoints=${entrypoint}`,
    `traefik.http.routers.${containerName}.tls=true`,
    `traefik.http.routers.${containerName}.tls.certresolver=${certResolver}`,
    `traefik.http.routers.${containerName}.tls.domains[0].main=${urls.wildcardDomain}`,
    `traefik.http.routers.${containerName}.tls.domains[0].sans=*.${urls.wildcardDomain}`,
    `traefik.http.services.${containerName}.loadbalancer.server.port=${gatewayPort}`,
    `traefik.http.routers.${containerName}-terminal.rule=Host(\`${urls.hostName}\`) && PathPrefix(\`/terminal\`)`,
    `traefik.http.routers.${containerName}-terminal.service=${containerName}-terminal`,
    `traefik.http.routers.${containerName}-terminal.priority=100`,
    `traefik.http.routers.${containerName}-terminal.entrypoints=${entrypoint}`,
    `traefik.http.routers.${containerName}-terminal.tls=true`,
    `traefik.http.routers.${containerName}-terminal.tls.certresolver=${certResolver}`,
    `traefik.http.routers.${containerName}-terminal.tls.domains[0].main=${urls.wildcardDomain}`,
    `traefik.http.routers.${containerName}-terminal.tls.domains[0].sans=*.${urls.wildcardDomain}`,
    `traefik.http.middlewares.${containerName}-terminal-strip.stripprefix.prefixes=/terminal`,
    `traefik.http.middlewares.${containerName}-terminal-strip.stripprefix.forceSlash=true`,
    `traefik.http.middlewares.${containerName}-inject-id.headers.customrequestheaders.X-Openclaw-Instance-Id=${input.instanceId}`,
    `traefik.http.middlewares.${containerName}-auth.forwardauth.address=${input.authUrl}`,
    `traefik.http.middlewares.${containerName}-auth.forwardauth.trustForwardHeader=true`,
    `traefik.http.routers.${containerName}-terminal.middlewares=${containerName}-inject-id,${containerName}-auth,${containerName}-terminal-strip`,
    `traefik.http.services.${containerName}-terminal.loadbalancer.server.port=${ttydPort}`,
  ];

  const labelArgs = labels.map((label) => `--label '${label}'`).join(' ');

  const runCommand = [
    'docker run -d',
    `--name ${containerName}`,
    '--restart unless-stopped',
    `--network ${dockerNetwork}`,
    `--cpus=${cpuLimit}`,
    `--memory-reservation=${memoryReservation}`,
    `--memory=${memoryLimit}`,
    `--memory-swap=${memoryLimit}`,
    `--pids-limit=${pidsLimit}`,
    `-v ${dataDir}:/home/node/.openclaw`,
    labelArgs,
    input.runtimeImage,
  ].join(' ');

  return {
    runCommand,
    containerName,
    dataDir,
    openclawUrl: urls.openclawUrl,
    ttydUrl: urls.ttydUrl,
    hostName: urls.hostName,
    wildcardDomain: urls.wildcardDomain,
    labels,
  };
}
