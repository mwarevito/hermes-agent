#!/usr/bin/env bash
# prod-approvals — rollback. Two documented modes; default is the SAFE one.
#
#   ./plugins/prod_approvals/deploy/rollback.sh          # safe: keep the floor
#   ./plugins/prod_approvals/deploy/rollback.sh --full   # full revert to pre-install
#
# SAFE (default): disable the plugin but KEEP the backstop hook installed. With
#   the plugin down and the marker going stale, the backstop hard-blocks every
#   production write (fail closed) — nothing runs unapproved during/after the
#   rollback. Use this to take the structured path out of service safely.
#
# FULL (--full): also remove the backstop hook, returning to the pre-install
#   state (production writes gated only by the native host-safety gate). Only
#   use when intentionally removing the feature entirely.
#
# Durable state: pending grants live in HERMES_HOME/prod_approvals/approvals.db.
# They are NOT auto-executed by a rollback (no live claim happens without the
# middleware), and any schema change on a later reinstall invalidates every
# non-terminal grant. Pass --purge to delete the store outright.
set -euo pipefail

HERMES_HOME="${HERMES_HOME:-$HOME/.hermes}"
HOOK_DST="$HERMES_HOME/agent-hooks/gate-prod-writes.sh"
STORE_DIR="$HERMES_HOME/prod_approvals"

FULL=0; PURGE=0
for arg in "$@"; do
  case "$arg" in
    --full) FULL=1 ;;
    --purge) PURGE=1 ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

if command -v hermes >/dev/null 2>&1; then
  hermes plugins disable prod-approvals || true
  echo "[prod-approvals] disabled plugin"
else
  echo "[prod-approvals] 'hermes' not on PATH — run: hermes plugins disable prod-approvals"
fi

if [ "$FULL" -eq 1 ]; then
  rm -f "$HOOK_DST"
  echo "[prod-approvals] removed backstop hook (FULL revert — writes no longer floor-blocked)"
else
  echo "[prod-approvals] kept backstop hook: production writes are hard-blocked (fail closed) until re-enabled"
fi

if [ "$PURGE" -eq 1 ]; then
  rm -rf "$STORE_DIR"
  echo "[prod-approvals] purged durable store at $STORE_DIR"
fi

echo "[prod-approvals] Restart the gateway to apply."
