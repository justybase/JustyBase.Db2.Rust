#!/usr/bin/env bash
# Public cross-driver qualification against one disposable Db2 instance.
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [ "${1:-}" = "--full" ]; then
  shift
  exec "$ROOT/scripts/test-db2-integration.sh" --parity-full "$@"
fi
exec "$ROOT/scripts/test-db2-integration.sh" --parity "$@"
