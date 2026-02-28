export interface BuildInstanceUrlsInput {
  instanceId: string;
  hostShard: string;
  baseDomain: string;
  subdomain?: string;
  terminalToken: string;
  instancePrefix?: string;
}

export interface InstanceUrls {
  openclawUrl: string;
  ttydUrl: string;
  hostName: string;
  wildcardDomain: string;
}

export function buildInstanceUrls(input: BuildInstanceUrlsInput): InstanceUrls {
  const subdomain = input.subdomain || 'openclaw';
  const instancePrefix = input.instancePrefix || 'openclaw-';

  const wildcardDomain = `${input.hostShard}.${subdomain}.${input.baseDomain}`;
  const hostName = `${instancePrefix}${input.instanceId}.${wildcardDomain}`;

  return {
    wildcardDomain,
    hostName,
    openclawUrl: `https://${hostName}/`,
    ttydUrl: `https://${hostName}/terminal?token=${input.terminalToken}`,
  };
}

