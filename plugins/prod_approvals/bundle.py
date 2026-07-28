"""User-facing bundle creation: the ``prod_bundle_request`` model tool.

The agent proposes an ordered, bounded sequence of production actions (e.g.
variable-set → restart → read-only verify). This builds a durable bounded
bundle bound to the authoritative context, delivers **one** approval card to
the bound chat, and returns a block telling the agent to wait. On one tap the
whole bundle is approved; the agent then runs each command through the normal
``terminal`` path and the gate consumes the steps *in order* (a step blocks
until its dependency has provably SUCCEEDED). This replaces the previous
direct-store-only (untriggerable) bundle code.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List

from plugins.prod_approvals import actions as _actions
from plugins.prod_approvals import cards as _cards
from plugins.prod_approvals import context as _context
from plugins.prod_approvals import fingerprint as _fp
from plugins.prod_approvals.gate import _open_store
from plugins.prod_approvals.store import StoreUnavailable

MAX_BUNDLE_STEPS = 8

TOOL_SCHEMA = {
    "type": "object",
    "properties": {
        "commands": {
            "type": "array",
            "items": {"type": "string"},
            "description": (
                "Ordered list of production commands to run as one approved "
                "bundle (each must be a structurally-verifiable production "
                "action, e.g. 'railway variables --set K=V --service <uuid>', "
                "'railway redeploy --service <uuid> --yes', "
                "'railway status'). Max 8 steps, run strictly in order."
            ),
        },
        "kind": {
            "type": "string",
            "enum": ["forward", "rollback"],
            "description": "forward deploy bundle or a separate rollback bundle.",
        },
        "cwd": {"type": "string", "description": "Working dir the terminal calls will use."},
    },
    "required": ["commands"],
}


def _err(msg: str, **extra) -> str:
    payload = {"ok": False, "blocked": True, "gate": "prod-approvals", "error": msg}
    payload.update(extra)
    return json.dumps(payload)


def request_bundle(args: Dict[str, Any], **kwargs: Any) -> str:
    """Handler for the ``prod_bundle_request`` tool."""
    if not isinstance(args, dict):
        return _err("invalid arguments")
    commands = args.get("commands")
    if not isinstance(commands, list) or not commands or not all(
        isinstance(c, str) and c.strip() for c in commands
    ):
        return _err("`commands` must be a non-empty list of command strings")
    if len(commands) > MAX_BUNDLE_STEPS:
        return _err(f"bundle exceeds max cardinality {MAX_BUNDLE_STEPS}")
    kind = args.get("kind", "forward")
    if kind not in ("forward", "rollback"):
        return _err("kind must be 'forward' or 'rollback'")
    cwd = str(args.get("cwd") or os.getcwd())

    # Every step must classify to a structurally-proven production action.
    classified = []
    for i, cmd in enumerate(commands):
        c = _actions.classify(cmd, cwd=cwd)
        if c is None:
            return _err(f"step {i + 1} is not a production action: {cmd!r}")
        if not getattr(c, "is_action", False):
            return _err(f"step {i + 1} is not structurally safe: {c.reason} ({c.detail})")
        classified.append(c)

    try:
        ctx = _context.bind_context()
    except _context.ContextError as exc:
        return _err(f"context could not be bound authoritatively: {exc}")
    except Exception as exc:  # pragma: no cover - defensive
        return _err(f"context error: {exc}")

    try:
        store = _open_store()
    except StoreUnavailable as exc:
        return _err(f"approval store unavailable: {exc}")
    except Exception as exc:
        return _err(f"approval store error: {exc}")

    try:
        key = getattr(store, "_fp_key")
        steps: List[Dict[str, Any]] = []
        card_steps: List[Dict[str, Any]] = []
        for act in classified:
            redacted = _fp.redact_argv(act, key)
            steps.append({
                "grant_fp": _fp.grant_fingerprint(act, ctx, key),
                "action_fp": _fp.action_fingerprint(act, key),
                "action_class": act.action_class,
                "redacted_argv": redacted,
                "targets": act.targets,
            })
            card_steps.append({"action_class": act.action_class, "redacted_argv": redacted})
        try:
            bundle_id, grants = store.create_bundle(
                steps=steps, ctx=ctx, kind=kind, max_steps=MAX_BUNDLE_STEPS,
            )
        except ValueError as exc:
            return _err(str(exc))
    except StoreUnavailable as exc:
        return _err(f"approval store unavailable: {exc}")
    except Exception as exc:
        return _err(f"bundle creation error: {exc}")
    finally:
        try:
            store.close()
        except Exception:
            pass

    card = _cards.build_bundle_card(ctx, bundle_id=bundle_id, steps=card_steps, kind=kind)
    delivered = _cards.deliver(card)
    payload = {
        "ok": True,
        "blocked": True,
        "gate": "prod-approvals",
        "bundle_id": bundle_id,
        "kind": kind,
        "steps": [{"class": s["action_class"], "argv": list(s["redacted_argv"])} for s in steps],
        "state": "requested",
        "next": (
            "A single approval card was delivered to this chat. After the user "
            "approves it, run each command in order via the terminal tool; each "
            "step runs exactly once and blocks until the previous step succeeds. "
            "Do NOT run any step before approval."
        ),
    }
    if not delivered:
        payload["card_delivered"] = False
        payload["approve_with"] = f"/approve-prod bundle {bundle_id}"
    else:
        payload["card_delivered"] = True
    return json.dumps(payload)
