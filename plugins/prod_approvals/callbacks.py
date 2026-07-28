"""Inline-button (one-tap) resolution for prod-approvals.

Registered via ``ctx.register_gateway_action_handler("pa:", handle_action)``.
A tap on an approval card's button arrives here with the full ``callback_data``
and the authoritative identity of whoever clicked. Resolution is strictly by
the exact nonce/bundle id embedded in the button — two pending cards resolve
independently, never oldest-FIFO. Approval is accepted only from the same
channel/identity the request is bound to.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from plugins.prod_approvals import cards as _cards
from plugins.prod_approvals import context as _context
from plugins.prod_approvals import resolve as _resolve
from plugins.prod_approvals.store import APPROVED, REQUESTED, StoreUnavailable

logger = logging.getLogger(__name__)


def _result(answer: str, *, edit: Optional[str] = None, remove_buttons: bool = True):
    from gateway.approval_cards import GatewayActionResult
    return GatewayActionResult(
        handled=True, answer_text=answer, edit_text=edit, remove_buttons=remove_buttons,
    )


def _resolver_ctx(clicker: Dict[str, str]) -> Dict[str, str]:
    """Authoritative approver context: the clicker's channel identity plus the
    process-fixed profile (same process that created the grant)."""
    ctx = {
        "platform": str(clicker.get("platform", "")),
        "chat_id": str(clicker.get("chat_id", "")),
        "thread_id": str(clicker.get("thread_id", "")),
        "user_id": str(clicker.get("user_id", "")),
    }
    try:
        ctx["profile"] = _context._profile()
    except Exception:
        ctx["profile"] = ""
    return ctx


def handle_action(data: str, clicker: Dict[str, str]) -> Optional[Any]:
    """Route a ``pa:*`` button click to the exact grant/bundle it names."""
    resolver = _resolver_ctx(clicker)
    try:
        store = _resolve._open_store()
    except StoreUnavailable as exc:
        return _result(f"Approval store unavailable: {exc}", remove_buttons=False)
    except Exception as exc:  # pragma: no cover - defensive
        return _result(f"Approval store error: {exc}", remove_buttons=False)

    try:
        if data.startswith(_cards.CB_APPROVE):
            return _approve_single(store, resolver, data[len(_cards.CB_APPROVE):], clicker)
        if data.startswith(_cards.CB_DENY):
            return _resolve_single(store, resolver, data[len(_cards.CB_DENY):], "deny")
        if data.startswith(_cards.CB_BUNDLE_APPROVE):
            return _approve_bundle(store, resolver, data[len(_cards.CB_BUNDLE_APPROVE):], clicker)
        if data.startswith(_cards.CB_BUNDLE_DENY):
            return _deny_bundle(store, resolver, data[len(_cards.CB_BUNDLE_DENY):])
        return None  # not one of ours
    finally:
        store.close()


def _approve_single(store, resolver, nonce, clicker):
    grant = store.get(nonce)
    if grant is None:
        return _result("This request no longer exists.")
    if grant.bundle_id is not None:
        return _result("This is a bundle step — approve the whole bundle card.",
                       remove_buttons=False)
    if not _resolve._authorised(grant.ctx, resolver):
        return _result("⛔ Not authorized: approve from the bound chat/user.",
                       remove_buttons=False)
    if grant.state != REQUESTED:
        return _result(f"Already {grant.state}.", edit=f"Request already {grant.state}.")
    ok = store.approve(nonce, approver=str(clicker.get("user_id", "")))
    if not ok:
        return _result("Could not approve (expired or already resolved).")
    who = clicker.get("user_name") or clicker.get("user_id") or "user"
    return _result("✅ Approved — it will run once.",
                   edit=f"✅ Approved by {who}: {grant.action_class}")


def _resolve_single(store, resolver, nonce, kind):
    grant = store.get(nonce)
    if grant is None:
        return _result("This request no longer exists.")
    if not _resolve._authorised(grant.ctx, resolver):
        return _result("⛔ Not authorized.", remove_buttons=False)
    ok = store.deny(nonce, reason="denied via card") if kind == "deny" else store.revoke(nonce)
    return _result("❌ Denied." if ok else "Already resolved.",
                   edit=f"❌ Denied: {grant.action_class}")


def _approve_bundle(store, resolver, bundle_id, clicker):
    steps = [g for g in store.list_pending() if g.bundle_id == bundle_id]
    if not steps:
        return _result("This bundle no longer exists or is already resolved.")
    if not _resolve._authorised(steps[0].ctx, resolver):
        return _result("⛔ Not authorized: approve from the bound chat/user.",
                       remove_buttons=False)
    ok = store.approve_bundle(bundle_id, approver=str(clicker.get("user_id", "")))
    who = clicker.get("user_name") or clicker.get("user_id") or "user"
    return _result(
        "✅ Bundle approved — steps run in order." if ok else "Could not approve bundle.",
        edit=f"✅ Bundle approved by {who} ({len(steps)} steps)" if ok else None,
    )


def _deny_bundle(store, resolver, bundle_id):
    steps = [g for g in store.list_pending() if g.bundle_id == bundle_id]
    if not steps:
        return _result("This bundle no longer exists or is already resolved.")
    if not _resolve._authorised(steps[0].ctx, resolver):
        return _result("⛔ Not authorized.", remove_buttons=False)
    n = store.revoke_bundle(bundle_id, reason="denied via card")
    return _result("❌ Bundle denied.", edit=f"❌ Bundle denied ({n} steps revoked)")
