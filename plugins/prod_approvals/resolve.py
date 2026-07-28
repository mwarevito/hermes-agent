"""``/approve-prod`` slash command — nonce-scoped resolution.

Runs in a CLI or gateway session, so it sees the *resolver's* authoritative
``session_context``. Resolution is always by an explicit nonce (or bundle id):
with two pending requests, ``/approve-prod <nonce-B>`` resolves exactly B and
never the oldest-FIFO. Clicking/approving is authorised only from the same
authoritative channel the request is bound to — approval from another
chat/thread/user/profile is rejected — and a nonce can be resolved once
(replay of an already-resolved nonce is a no-op error).
"""

from __future__ import annotations

import json
import shlex
from pathlib import Path
from typing import Callable, Dict, List, Optional

from plugins.prod_approvals import context as _context
from plugins.prod_approvals import fingerprint as _fp
from plugins.prod_approvals.store import (
    ApprovalStore,
    Grant,
    StoreUnavailable,
    APPROVED,
    REQUESTED,
)

# Fields that authorise a resolver to act on a grant: approval must come from
# the exact channel/identity the execution is bound to.
_AUTH_FIELDS = ("platform", "chat_id", "thread_id", "user_id", "profile")


def _base_dir() -> Path:
    import os
    try:
        from hermes_constants import get_hermes_home
        return Path(get_hermes_home()) / "prod_approvals"
    except Exception:
        return Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))) / "prod_approvals"


def _open_store() -> ApprovalStore:
    store = ApprovalStore(_base_dir())
    return store


def _authorised(grant_ctx: Dict[str, str], resolver_ctx: Dict[str, str]) -> bool:
    return all(str(grant_ctx.get(f, "")) == str(resolver_ctx.get(f, "")) for f in _AUTH_FIELDS)


def _fmt(grant: Grant) -> Dict[str, object]:
    return {
        "nonce": grant.nonce,
        "class": grant.action_class,
        "state": grant.state,
        "argv": list(grant.redacted_argv),
        "targets": [list(t) for t in grant.targets],
        "bundle_id": grant.bundle_id,
        "step_index": grant.step_index,
    }


def handle(
    raw_args: str,
    *,
    open_store: Callable[[], ApprovalStore] = _open_store,
    bind_context: Callable[..., Dict[str, str]] = _context.bind_context,
) -> str:
    """Handle ``/approve-prod ...``. Returns a JSON string (shown to the user)."""
    try:
        argv = shlex.split(raw_args or "")
    except ValueError:
        argv = (raw_args or "").split()

    try:
        resolver_ctx = bind_context()
    except _context.ContextError as exc:
        return json.dumps({"ok": False, "error": f"cannot bind approver context: {exc}"})

    try:
        store = open_store()
    except StoreUnavailable as exc:
        return json.dumps({"ok": False, "error": f"approval store unavailable: {exc}"})

    try:
        if not argv or argv[0] in ("list", "ls"):
            pending = store.list_pending()
            visible = [_fmt(g) for g in pending if _authorised(g.ctx, resolver_ctx)]
            return json.dumps({"ok": True, "pending": visible})

        sub = argv[0]
        # `/approve-prod <nonce>` shorthand == approve.
        if sub not in ("approve", "deny", "revoke", "bundle"):
            return _approve(store, resolver_ctx, sub)

        if sub == "approve":
            if len(argv) < 2:
                return json.dumps({"ok": False, "error": "usage: /approve-prod <nonce>"})
            return _approve(store, resolver_ctx, argv[1])
        if sub == "deny":
            if len(argv) < 2:
                return json.dumps({"ok": False, "error": "usage: /approve-prod deny <nonce> [reason]"})
            return _resolve(store, resolver_ctx, argv[1], "deny", " ".join(argv[2:]))
        if sub == "revoke":
            if len(argv) < 2:
                return json.dumps({"ok": False, "error": "usage: /approve-prod revoke <nonce> [reason]"})
            return _resolve(store, resolver_ctx, argv[1], "revoke", " ".join(argv[2:]))
        if sub == "bundle":
            if len(argv) < 2:
                return json.dumps({"ok": False, "error": "usage: /approve-prod bundle <bundle_id>"})
            return _approve_bundle(store, resolver_ctx, argv[1])
        return json.dumps({"ok": False, "error": "unknown subcommand"})
    finally:
        store.close()


def _approve(store: ApprovalStore, resolver_ctx: Dict[str, str], nonce: str) -> str:
    grant = store.get(nonce)
    if grant is None:
        return json.dumps({"ok": False, "error": "no such request", "nonce": nonce})
    if not _authorised(grant.ctx, resolver_ctx):
        return json.dumps({
            "ok": False,
            "error": "not authorised: approval must come from the same channel/user "
                     "the request is bound to",
            "nonce": nonce,
        })
    if grant.state != REQUESTED:
        return json.dumps({
            "ok": False,
            "error": f"request is {grant.state}, not resolvable",
            "nonce": nonce,
        })
    approver = resolver_ctx.get("user_id", "")
    if grant.bundle_id is not None:
        return json.dumps({
            "ok": False,
            "error": "this request is a bundle step; approve the whole bundle with "
                     "/approve-prod bundle <bundle_id>",
            "bundle_id": grant.bundle_id,
        })
    ok = store.approve(nonce, approver=approver)
    return json.dumps({
        "ok": bool(ok),
        "action": "approved" if ok else "not-approved",
        "nonce": nonce,
        "class": grant.action_class,
        "argv": list(grant.redacted_argv),
    })


def _resolve(store: ApprovalStore, resolver_ctx: Dict[str, str], nonce: str,
             kind: str, reason: str) -> str:
    grant = store.get(nonce)
    if grant is None:
        return json.dumps({"ok": False, "error": "no such request", "nonce": nonce})
    if not _authorised(grant.ctx, resolver_ctx):
        return json.dumps({"ok": False, "error": "not authorised", "nonce": nonce})
    if kind == "deny":
        ok = store.deny(nonce, reason=reason)
    else:
        ok = store.revoke(nonce, reason=reason)
    return json.dumps({"ok": bool(ok), "action": kind, "nonce": nonce})


def _approve_bundle(store: ApprovalStore, resolver_ctx: Dict[str, str], bundle_id: str) -> str:
    # Authorise against the bundle's step contexts (all share one ctx).
    steps = [g for g in store.list_pending() if g.bundle_id == bundle_id]
    if not steps:
        return json.dumps({"ok": False, "error": "no such pending bundle", "bundle_id": bundle_id})
    if not _authorised(steps[0].ctx, resolver_ctx):
        return json.dumps({"ok": False, "error": "not authorised", "bundle_id": bundle_id})
    ok = store.approve_bundle(bundle_id, approver=resolver_ctx.get("user_id", ""))
    return json.dumps({
        "ok": bool(ok),
        "action": "bundle-approved" if ok else "not-approved",
        "bundle_id": bundle_id,
        "steps": len(steps),
    })
