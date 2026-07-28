"""Build interactive approval cards (no secrets) and deliver them to the
authoritative bound chat/thread via the generic gateway card bus.

Every card carries only redacted argv, the action class, immutable target ids,
and the nonce/bundle id inside button ``callback_data`` — never a secret value
and never a value the model must relay. Delivery goes through
``gateway.approval_cards.deliver_card``; a click returns through
``dispatch_gateway_action`` to :mod:`plugins.prod_approvals.callbacks`.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

# callback_data prefixes this plugin owns. Kept short: Telegram caps
# callback_data at 64 bytes and a nonce/bundle id is a 32-char uuid4 hex.
CB_APPROVE = "pa:approve:"
CB_DENY = "pa:deny:"
CB_BUNDLE_APPROVE = "pa:bundle:"
CB_BUNDLE_DENY = "pa:bdeny:"
CB_PREFIX = "pa:"


def _fmt_argv(redacted_argv: Sequence[str]) -> str:
    return " ".join(redacted_argv)


def _fmt_targets(targets: Sequence[Tuple[str, str]]) -> str:
    return ", ".join(f"{k}={v}" for k, v in targets) if targets else "—"


def build_single_card(
    ctx: Dict[str, str],
    *,
    nonce: str,
    action_class: str,
    redacted_argv: Sequence[str],
    targets: Sequence[Tuple[str, str]],
):
    """Return an ``ApprovalCard`` for a single pending production-write."""
    from gateway.approval_cards import ApprovalCard

    text = (
        "⚠️ Production write requires approval\n"
        f"class: {action_class}\n"
        f"cmd: {_fmt_argv(redacted_argv)}\n"
        f"target: {_fmt_targets(targets)}\n"
        f"task: {ctx.get('task_id') or '—'}  run: {ctx.get('run_id') or '—'}\n"
        "Approve to run it exactly once."
    )
    buttons: List[List[Tuple[str, str]]] = [[
        ("✅ Approve", f"{CB_APPROVE}{nonce}"),
        ("❌ Deny", f"{CB_DENY}{nonce}"),
    ]]
    return ApprovalCard(
        platform=str(ctx.get("platform", "")),
        chat_id=str(ctx.get("chat_id", "")),
        thread_id=str(ctx.get("thread_id", "")),
        text=text,
        buttons=buttons,
        key=nonce,
    )


def build_bundle_card(
    ctx: Dict[str, str],
    *,
    bundle_id: str,
    steps: Sequence[Dict[str, object]],
    kind: str = "forward",
):
    """Return an ``ApprovalCard`` for an ordered bounded bundle."""
    from gateway.approval_cards import ApprovalCard

    lines = [f"📦 Production {kind} bundle requires approval ({len(steps)} steps)"]
    for i, s in enumerate(steps):
        argv = _fmt_argv(s.get("redacted_argv", ()))  # type: ignore[arg-type]
        lines.append(f"  {i + 1}. [{s.get('action_class')}] {argv}")
    lines.append(f"task: {ctx.get('task_id') or '—'}  run: {ctx.get('run_id') or '—'}")
    lines.append("Approve to run all steps in order, each exactly once.")
    text = "\n".join(lines)
    buttons: List[List[Tuple[str, str]]] = [[
        ("✅ Approve bundle", f"{CB_BUNDLE_APPROVE}{bundle_id}"),
        ("❌ Deny", f"{CB_BUNDLE_DENY}{bundle_id}"),
    ]]
    return ApprovalCard(
        platform=str(ctx.get("platform", "")),
        chat_id=str(ctx.get("chat_id", "")),
        thread_id=str(ctx.get("thread_id", "")),
        text=text,
        buttons=buttons,
        key=bundle_id,
    )


def deliver(card) -> bool:
    """Deliver a card via the gateway card bus. False if no sender/failed."""
    if not card.platform or not card.chat_id:
        return False
    try:
        from gateway.approval_cards import deliver_card
        return bool(deliver_card(card))
    except Exception:  # pragma: no cover - defensive
        logger.debug("prod-approvals: card delivery raised", exc_info=True)
        return False
