#!/usr/bin/env bash
set -euo pipefail

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

: "${OPENCLAW_TTYD_SECRET:?Missing OPENCLAW_TTYD_SECRET}"

NOW_SECONDS="${NOW_SECONDS:-$(date +%s)}"
TS_HEX="$(printf '%x' "${NOW_SECONDS}")"

if command -v openssl >/dev/null 2>&1; then
  SIG="$(printf '%s' "${INSTANCE_ID}:${TS_HEX}" | openssl dgst -sha256 -hmac "${OPENCLAW_TTYD_SECRET}" | awk '{print $NF}')"
else
  # Fallback for environments without openssl (e.g. minimal containers).
  SIG="$(node -e "const crypto=require('node:crypto'); const secret=process.env.OPENCLAW_TTYD_SECRET; const msg=process.argv[1]; process.stdout.write(crypto.createHmac('sha256', secret).update(msg).digest('hex'))" "${INSTANCE_ID}:${TS_HEX}")"
fi

TOKEN_CHARS="${OPENCLAW_TTYD_TOKEN_CHARS:-16}"
SIG="${SIG:0:${TOKEN_CHARS}}"

printf '%s.%s\n' "${TS_HEX}" "${SIG}"
