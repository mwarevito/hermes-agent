#!/usr/bin/env bash
# prod-approvals fail-closed backstop (pre_tool_call shell hook).
#
# Installable template — DO NOT edit the live copy by hand. Install with:
#   cp plugins/prod_approvals/backstop/gate-prod-writes.sh \
#      "${HERMES_HOME:-$HOME/.hermes}/agent-hooks/gate-prod-writes.sh"
#   chmod +x "${HERMES_HOME:-$HOME/.hermes}/agent-hooks/gate-prod-writes.sh"
#
# Contract (see agent/shell_hooks.py): for pre_tool_call a hook may print
#   {"decision":"block","message":"..."}   -> hard block
# Any other/empty output -> pass through. The hook is fail-OPEN on error by the
# host contract, so this script keeps its logic tiny and deterministic and never
# errors out on the block path.
#
# Behaviour: if the command touches a production-write CLI class AND the
# prod-approvals middleware plugin is NOT currently loaded (no fresh activation
# marker), hard-block — this is the floor that prevents an ambient "yes" from
# executing a prod write while the structured approval path is unavailable. When
# the plugin IS loaded (fresh marker), defer: emit nothing so the in-process
# middleware gate handles structured approval.
#
# The tool payload arrives on THIS script's stdin and is passed straight through
# to python as stdin; the python program itself is supplied via `-c` so it never
# competes with the payload for the stdin fd.

HERMES_HOME="${HERMES_HOME:-$HOME/.hermes}"
MARKER="${HERMES_HOME}/prod_approvals/plugin_active"
MARKER_FRESH_SECONDS=300

exec python3 -c '
import json, re, sys, time

marker_path = sys.argv[1]
fresh_seconds = int(sys.argv[2])

try:
    payload = json.load(sys.stdin)
except Exception:
    # Can not parse input -> emit nothing (host fails open; the middleware gate
    # and native gate remain in force). We only *add* blocks, never weaken.
    sys.exit(0)

tool_name = payload.get("tool_name") or ""
tool_input = payload.get("tool_input") or {}
command = ""
if isinstance(tool_input, dict):
    command = tool_input.get("command") or tool_input.get("code") or ""
command = str(command)

if tool_name not in ("terminal", "execute_code") or not command:
    sys.exit(0)

PROD_CLIS = ("railway", "vercel", "supabase", "flyctl", "fly")
def is_prod_class(cmd):
    for cli in PROD_CLIS:
        if re.search(r"(?<![\w/-])" + re.escape(cli) + r"(?![\w-])", cmd):
            return True
    return False

if not is_prod_class(command):
    sys.exit(0)

plugin_fresh = False
try:
    # Marker is "<unix_ts> <pid>"; the plugin refreshes it every ~60s while
    # alive (plugins/prod_approvals/heartbeat.py). Read the first token so the
    # format stays compatible with the legacy timestamp-only marker.
    raw = open(marker_path).read().strip()
    ts = float(raw.split()[0])
    plugin_fresh = (time.time() - ts) <= fresh_seconds
except Exception:
    plugin_fresh = False

if plugin_fresh:
    sys.exit(0)  # defer to the in-process middleware gate

print(json.dumps({
    "decision": "block",
    "message": (
        "Production-write blocked by prod-approvals backstop: the structured "
        "approval plugin is not loaded, so there is no safe one-tap approval "
        "path. Enable it with `hermes plugins enable prod-approvals` (and keep "
        "this backstop as the fail-closed floor)."
    ),
}))
' "$MARKER" "$MARKER_FRESH_SECONDS"
