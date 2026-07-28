#!/usr/bin/env bash
# prod-approvals — live activation. Repo-resident and idempotent: no live source
# is hand-edited. Run once per profile/host after the plugin source is deployed.
#
#   HERMES_HOME=~/.hermes ./plugins/prod_approvals/deploy/install.sh
#
# Steps:
#   1. Install the fail-closed backstop shell hook (the floor that hard-blocks
#      production writes whenever the plugin is not loaded).
#   2. Enable the plugin in config (opt-in, per hermes plugin policy).
# Then restart the gateway so the tool_execution middleware + heartbeat load.
set -euo pipefail

HERMES_HOME="${HERMES_HOME:-$HOME/.hermes}"
PLUGIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"   # plugins/prod_approvals
HOOK_SRC="$PLUGIN_DIR/backstop/gate-prod-writes.sh"
HOOK_DST="$HERMES_HOME/agent-hooks/gate-prod-writes.sh"

echo "[prod-approvals] HERMES_HOME=$HERMES_HOME"

# 1. Fail-closed backstop hook.
mkdir -p "$HERMES_HOME/agent-hooks"
cp "$HOOK_SRC" "$HOOK_DST"
chmod +x "$HOOK_DST"
echo "[prod-approvals] installed backstop hook -> $HOOK_DST"

# 2. Enable the plugin (opt-in). Falls back to a printed instruction if the CLI
#    is not on PATH in this shell.
if command -v hermes >/dev/null 2>&1; then
  hermes plugins enable prod-approvals || true
  echo "[prod-approvals] enabled via 'hermes plugins enable prod-approvals'"
else
  echo "[prod-approvals] 'hermes' not on PATH — run: hermes plugins enable prod-approvals"
fi

echo "[prod-approvals] DONE. Restart the gateway to load the middleware + heartbeat."
echo "[prod-approvals] Verify: hermes plugins list | grep prod-approvals"
