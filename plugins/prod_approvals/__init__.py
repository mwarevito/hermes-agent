"""prod-approvals plugin — task-scoped one-tap / bundle approval for custom
production-write gates.

Wiring (zero core edits by the plugin itself; the generic gateway card/action
surface it uses lives in ``gateway.approval_cards`` +
``PluginContext.register_gateway_action_handler``):

* ``tool_execution`` middleware  → :func:`plugins.prod_approvals.gate.middleware`
  intercepts terminal/execute_code calls, classifies production-write actions,
  enforces exactly-once approved execution against a durable fenced store, and
  delivers a one-tap approval card to the authoritative bound chat.
* gateway action handler ``pa:`` → :func:`plugins.prod_approvals.callbacks.handle_action`
  resolves a card button click by the exact nonce/bundle id it carries.
* ``prod_bundle_request`` tool → :func:`plugins.prod_approvals.bundle.request_bundle`
  builds an ordered bounded bundle and delivers a single approval card.
* ``/approve-prod`` slash command → :func:`plugins.prod_approvals.resolve.handle`
  is the non-one-tap fallback (CLI / no-button transports).

Activation / backstop contract: the fail-closed floor is the shell hook
``backstop/gate-prod-writes.sh`` (installed into ``~/.hermes/agent-hooks/``),
which hard-blocks production-write classes whenever this plugin is *not* loaded.
While loaded, a background heartbeat keeps a fresh activation marker
(``HERMES_HOME/prod_approvals/plugin_active``) so the backstop defers to the
middleware gate. So: plugin down / marker stale → hard block (safe); plugin up →
structured approval path — indefinitely, not just for the first few minutes.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


def _base_dir() -> Path:
    try:
        from hermes_constants import get_hermes_home
        return Path(get_hermes_home()) / "prod_approvals"
    except Exception:
        return Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))) / "prod_approvals"


def register(ctx) -> None:
    from plugins.prod_approvals.gate import middleware as gate_middleware
    from plugins.prod_approvals.resolve import handle as approve_handler
    from plugins.prod_approvals.callbacks import handle_action as pa_action_handler
    from plugins.prod_approvals.cards import CB_PREFIX
    from plugins.prod_approvals import bundle as _bundle
    from plugins.prod_approvals import heartbeat as _heartbeat

    ctx.register_middleware("tool_execution", gate_middleware)

    # One-tap card button resolution (the real proof: no model relay).
    ctx.register_gateway_action_handler(CB_PREFIX, pa_action_handler)

    # Slash fallback for CLI / button-less transports.
    ctx.register_command(
        "approve-prod",
        handler=lambda raw_args="": approve_handler(raw_args),
        description="Approve/deny/revoke a pending production-write by nonce "
                    "(/approve-prod <nonce> | deny <nonce> | revoke <nonce> | "
                    "bundle <id> | list). One-tap buttons are preferred.",
        args_hint="<nonce>",
    )

    # User-facing bundle creation path (agent-invokable).
    try:
        ctx.register_tool(
            name="prod_bundle_request",
            toolset="prod_approvals",
            schema=_bundle.TOOL_SCHEMA,
            handler=_bundle.request_bundle,
            is_async=False,
            override=True,  # idempotent across live reloads / redeploys
            description=(
                "Request approval for an ordered, bounded bundle of production "
                "actions (e.g. variable-set → restart → read-only verify). "
                "Delivers ONE approval card; after one tap the steps run in "
                "order through the normal terminal path, each exactly once."
            ),
            emoji="📦",
        )
    except Exception:
        logger.warning("prod-approvals: prod_bundle_request tool registration failed", exc_info=True)

    # Robust liveness: refresh the activation marker while alive so the
    # backstop keeps deferring to this gate (fixes the write-once staleness).
    try:
        _heartbeat.start_singleton(_base_dir())
    except Exception:
        logger.warning("prod-approvals: heartbeat start failed", exc_info=True)

    logger.info(
        "prod-approvals plugin registered "
        "(tool_execution gate + pa: one-tap + prod_bundle_request + /approve-prod + heartbeat)"
    )
