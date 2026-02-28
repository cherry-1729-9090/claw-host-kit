#!/usr/bin/env bash
set -euo pipefail

# Backwards compatible alias.
exec "$(dirname "$0")/local-up.sh" "$@"

