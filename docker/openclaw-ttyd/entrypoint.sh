#!/usr/bin/env bash
set -euo pipefail

GATEWAY_PORT="${OPENCLAW_GATEWAY_PORT:-18789}"
TTYD_PORT="${OPENCLAW_TTYD_PORT:-7681}"
AUTO_APPROVE_CONTROL_UI_PAIRING="${OPENCLAW_AUTO_APPROVE_CONTROL_UI_PAIRING:-true}"
AUTO_APPROVE_POLL_SECONDS="${OPENCLAW_AUTO_APPROVE_POLL_SECONDS:-2}"
MAX_RETRIES=2
RETRY_DELAY=3
CONTROL_UI_CLIENT_ID="openclaw-control-ui"

gateway_pid=""
ttyd_pid=""
auto_approve_pid=""

is_truthy() {
  local value
  value="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
  case "${value}" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

read_gateway_token() {
  local token
  token="$(openclaw config get gateway.auth.token 2>/dev/null || true)"
  token="$(printf '%s' "${token}" | tr -d '[:space:]')"
  token="${token#\"}"
  token="${token%\"}"
  printf '%s' "${token}"
}

ensure_gateway_token() {
  local token
  token="$(read_gateway_token)"

  if [ -z "${token}" ] || [ "${token}" = "null" ]; then
    openclaw config set gateway.auth.mode token >/dev/null 2>&1 || true
    openclaw config set gateway.auth.token "$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')" >/dev/null 2>&1 || true
    token="$(read_gateway_token)"
  fi

  if [ -z "${token}" ] || [ "${token}" = "null" ]; then
    echo "Warning: could not initialize gateway auth token. Gateway will start without pre-set token." >&2
  fi
}

ensure_silent_pairing() {
  local pending="${OPENCLAW_STATE_DIR}/devices/pending.json"
  mkdir -p "$(dirname "${pending}")"

  if [ ! -f "${pending}" ]; then
    printf '{"silent":true}\n' > "${pending}"
  else
    node -e "
      const fs = require('fs');
      const f = '${pending}';
      const d = JSON.parse(fs.readFileSync(f, 'utf8'));
      if (d.silent !== true) { d.silent = true; fs.writeFileSync(f, JSON.stringify(d) + '\n'); }
    " 2>/dev/null || true
  fi
}

start_gateway() {
  openclaw gateway --bind lan --port "${GATEWAY_PORT}" --allow-unconfigured &
  gateway_pid=$!
}

list_pending_control_ui_request_ids() {
  node - "${1}" "${CONTROL_UI_CLIENT_ID}" <<'NODE'
const fs = require('node:fs');
const [,, pendingPath, controlUiClientId] = process.argv;

let entries;
try {
  entries = JSON.parse(fs.readFileSync(pendingPath, 'utf8'));
} catch {
  process.exit(0);
}

if (!entries || typeof entries !== 'object') process.exit(0);

for (const r of Object.values(entries)) {
  if (r?.role === 'operator' && r?.clientId === controlUiClientId && r?.requestId) {
    console.log(r.requestId);
  }
}
NODE
}

auto_approve_pending_pairings_loop() {
  local pending_file request_id
  pending_file="${OPENCLAW_STATE_DIR:-/home/node/.openclaw}/devices/pending.json"

  while true; do
    if [ -f "${pending_file}" ]; then
      while IFS= read -r request_id; do
        [ -n "${request_id}" ] || continue
        if openclaw devices approve "${request_id}" >/dev/null 2>&1; then
          echo "Auto-approved Control UI pairing request: ${request_id}" >&2
        fi
      done < <(list_pending_control_ui_request_ids "${pending_file}")
    fi
    sleep "${AUTO_APPROVE_POLL_SECONDS}"
  done
}

start_auto_pairing_approver() {
  if ! is_truthy "${AUTO_APPROVE_CONTROL_UI_PAIRING}"; then
    return
  fi

  auto_approve_pending_pairings_loop &
  auto_approve_pid=$!
}

kill_and_wait() {
  local pid="${1:-}"
  [ -n "${pid}" ] || return 0
  kill "${pid}" 2>/dev/null || true
  wait "${pid}" 2>/dev/null || true
}

cleanup() {
  local pid
  for pid in ${auto_approve_pid} ${gateway_pid} ${ttyd_pid}; do
    kill "${pid}" 2>/dev/null || true
  done
  for pid in ${auto_approve_pid} ${gateway_pid} ${ttyd_pid}; do
    wait "${pid}" 2>/dev/null || true
  done
}

ensure_gateway_token
ensure_silent_pairing

ttyd -p "${TTYD_PORT}" -W bash &
ttyd_pid=$!

start_gateway
start_auto_pairing_approver
retries=0

trap cleanup SIGINT SIGTERM

while true; do
  wait -n "${gateway_pid}" "${ttyd_pid}" || true
  status=$?

  if ! kill -0 "${ttyd_pid}" 2>/dev/null; then
    echo "ttyd exited (status=${status}). Shutting down." >&2
    kill_and_wait "${gateway_pid}"
    exit "${status}"
  fi

  if ! kill -0 "${gateway_pid}" 2>/dev/null; then
    retries=$((retries + 1))
    if [ "${retries}" -gt "${MAX_RETRIES}" ]; then
      echo "openclaw gateway crashed ${MAX_RETRIES} times consecutively. Giving up but keeping ttyd alive." >&2
      wait "${ttyd_pid}" 2>/dev/null || true
      exit 1
    fi
    echo "openclaw gateway exited (status=${status}). Restarting in ${RETRY_DELAY}s (attempt ${retries}/${MAX_RETRIES})..." >&2
    sleep "${RETRY_DELAY}"
    start_gateway
  else
    retries=0
  fi
done
