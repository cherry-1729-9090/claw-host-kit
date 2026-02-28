const http = require('node:http');
const crypto = require('node:crypto');

const DEFAULT_TTL_SECONDS = 24 * 60 * 60;

function readEnv(name, fallback = '') {
  const v = process.env[name];
  if (typeof v !== 'string') return fallback;
  return v;
}

function toInt(value, fallback) {
  const n = Number.parseInt(String(value || ''), 10);
  return Number.isFinite(n) ? n : fallback;
}

function validateTerminalToken(instanceId, token, options) {
  const secret = options.secret;
  const ttlSeconds = options.ttlSeconds ?? DEFAULT_TTL_SECONDS;
  const nowSeconds = options.nowSeconds ?? Math.floor(Date.now() / 1000);

  const dotIndex = token.indexOf('.');
  if (dotIndex === -1) return false;
  const timestamp = token.substring(0, dotIndex);
  const signature = token.substring(dotIndex + 1);

  const issuedAt = Number.parseInt(timestamp, 16);
  if (!Number.isFinite(issuedAt)) return false;
  if (nowSeconds - issuedAt > ttlSeconds) return false;

  const full = crypto.createHmac('sha256', secret).update(`${instanceId}:${timestamp}`).digest('hex');
  const expected = full.substring(0, signature.length);
  if (expected.length !== signature.length) return false;

  try {
    return crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(signature));
  } catch {
    return false;
  }
}

function respond(res, status, body) {
  res.statusCode = status;
  res.setHeader('content-type', 'text/plain; charset=utf-8');
  res.end(body);
}

const secret = readEnv('OPENCLAW_TTYD_SECRET');
const ttlSeconds = toInt(readEnv('OPENCLAW_TTYD_TTL_SECONDS'), DEFAULT_TTL_SECONDS);
const port = toInt(readEnv('PORT'), 8080);

if (!secret) {
  // Fail fast: if this container runs without a secret, terminal auth is meaningless.
  // Exiting makes misconfig obvious.
  console.error('Missing OPENCLAW_TTYD_SECRET');
  process.exit(1);
}

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') {
    return respond(res, 200, 'ok');
  }

  const instanceId = String(req.headers['x-openclaw-instance-id'] || '').trim();
  const forwardedUri = String(req.headers['x-forwarded-uri'] || '').trim();

  if (!instanceId || !forwardedUri) {
    return respond(res, 401, 'Unauthorized');
  }

  let token = '';
  try {
    const url = new URL(forwardedUri, 'http://localhost');
    token = url.searchParams.get('token') || '';
  } catch {
    token = '';
  }

  if (!token) {
    return respond(res, 401, 'Unauthorized');
  }

  const ok = validateTerminalToken(instanceId, token, { secret, ttlSeconds });
  if (!ok) {
    return respond(res, 401, 'Unauthorized');
  }

  return respond(res, 200, 'OK');
});

server.listen(port, '0.0.0.0', () => {
  console.log(`openclaw-forward-auth listening on :${port}`);
});

