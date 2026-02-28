import { createHmac, timingSafeEqual } from 'crypto';

const DEFAULT_TOKEN_TTL_SECONDS = 24 * 60 * 60;

export interface TerminalTokenOptions {
  secret: string;
  ttlSeconds?: number;
  nowSeconds?: number;
}

export function generateTerminalToken(instanceId: string, options: TerminalTokenOptions): string {
  const secret = options.secret;
  const nowSeconds = options.nowSeconds ?? Math.floor(Date.now() / 1000);
  const timestamp = nowSeconds.toString(16);
  const signature = createHmac('sha256', secret).update(`${instanceId}:${timestamp}`).digest('hex');
  return `${timestamp}.${signature}`;
}

export function validateTerminalToken(instanceId: string, token: string, options: TerminalTokenOptions): boolean {
  const secret = options.secret;
  const ttlSeconds = options.ttlSeconds ?? DEFAULT_TOKEN_TTL_SECONDS;
  const nowSeconds = options.nowSeconds ?? Math.floor(Date.now() / 1000);

  const dotIndex = token.indexOf('.');
  if (dotIndex === -1) return false;
  const timestamp = token.substring(0, dotIndex);
  const signature = token.substring(dotIndex + 1);

  const issuedAt = parseInt(timestamp, 16);
  if (!Number.isFinite(issuedAt)) return false;
  if (nowSeconds - issuedAt > ttlSeconds) return false;

  const full = createHmac('sha256', secret).update(`${instanceId}:${timestamp}`).digest('hex');
  const expected = full.substring(0, signature.length);
  if (expected.length !== signature.length) return false;
  return timingSafeEqual(Buffer.from(expected), Buffer.from(signature));
}

