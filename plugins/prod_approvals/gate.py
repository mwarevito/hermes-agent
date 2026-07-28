"""The tool-execution gate.

Registered as a ``tool_execution`` middleware. It wraps the real tool call and
either lets it run exactly once (an approved, unexpired, fingerprint- and
context-matched grant is consumed via CAS) or blocks it (returns a tool-result
string *without* calling ``next_call``), delivering a one-tap approval card to
the authoritative bound chat as it blocks.

Invariants:

1. **Fail closed on every error.** The middleware chain treats a *raised*
   exception before ``next_call`` as "proceed to the tool" (fail open). So the
   gate never lets an exception escape before it has decided: every internal
   error on a production-write path is caught and turned into a block. Only a
   genuine tool execution error (after the grant is consumed) propagates.

2. **Exactly once.** The approved→claimed CAS happens *before* ``next_call``.
   A crash mid-exec leaves the grant ``indeterminate``; a failure after claim
   marks it ``failed``; success must be *proven* or the outcome defaults to
   ``indeterminate``. None of these are ever auto-retried.

3. **Reads pass through.** A read-only production command (``railway status`` /
   ``railway variables`` listing) is never blocked and never consumes a grant,
   unless it is an explicit ordered step of an approved bundle (then it is
   consumed in order like any other step).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from plugins.prod_approvals import actions as _actions
from plugins.prod_approvals import cards as _cards
from plugins.prod_approvals import context as _context
from plugins.prod_approvals import fingerprint as _fp
from plugins.prod_approvals.store import (
    ApprovalStore,
    StoreUnavailable,
    INDETERMINATE,
)

# Tools that can carry a production-write. ``execute_code`` is included because
# it is an arbitrary-code smuggling vector.
_GATED_TOOLS = {"terminal", "execute_code"}


def _base_dir() -> Path:
    try:
        from hermes_constants import get_hermes_home
        return Path(get_hermes_home()) / "prod_approvals"
    except Exception:
        return Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))) / "prod_approvals"


def _block(reason: str, *, nonce: str = "", extra: Optional[Dict[str, Any]] = None) -> str:
    payload = {
        "ok": False,
        "blocked": True,
        "gate": "prod-approvals",
        "reason": reason,
    }
    if nonce:
        payload["approve_with"] = f"/approve-prod {nonce}"
        payload["nonce"] = nonce
    if extra:
        payload.update(extra)
    return json.dumps(payload)


def _open_store() -> ApprovalStore:
    key = _fp.load_or_create_key(_base_dir())
    store = ApprovalStore(_base_dir())
    # attach key for callers
    store._fp_key = key  # type: ignore[attr-defined]
    return store


def _classify_execute_code(code: str) -> Optional[str]:
    """execute_code cannot be structurally proven to a fixed argv, so any prod
    class token in the code is blocked outright."""
    if _actions.is_prod_write_class(code):
        return _block(
            "prod-write via execute_code is not allowed; use the native terminal "
            "path so the action can be structurally verified and approved",
            extra={"class": "execute_code-smuggling"},
        )
    return None


def _safe_finish(store: ApprovalStore, nonce: str, outcome: str) -> None:
    try:
        store.finish(nonce, outcome)
    except Exception:
        pass


def _safe_close(store: ApprovalStore) -> None:
    try:
        store.close()
    except Exception:
        pass


def evaluate(
    tool_name: str,
    args: Dict[str, Any],
    next_call: Callable[[Dict[str, Any]], Any],
    *,
    open_store: Callable[[], ApprovalStore] = _open_store,
    bind_context: Callable[..., Dict[str, str]] = _context.bind_context,
    deliver_card: Callable[[Any], bool] = _cards.deliver,
) -> Any:
    """Core gate logic, dependency-injected for tests."""
    if tool_name not in _GATED_TOOLS:
        return next_call(args)

    if tool_name == "execute_code":
        # Fail closed: any error while deciding whether this code smuggles a
        # production write blocks rather than proceeding (the chain fails open
        # on a raised exception, so we must catch here).
        try:
            code = args.get("code") or args.get("command") or ""
            blocked = _classify_execute_code(str(code))
        except Exception as exc:  # pragma: no cover - defensive
            return _block(
                f"execute_code classification error (fail closed): {exc}",
                extra={"class": "execute_code-error"},
            )
        if blocked is not None:
            return blocked
        return next_call(args)

    # terminal
    command = args.get("command")
    if not isinstance(command, str):
        return next_call(args)

    # Classification never raises for non-prod commands; be defensive anyway.
    try:
        classification = _actions.classify(command, cwd=str(args.get("cwd") or os.getcwd()))
    except Exception as exc:  # pragma: no cover - defensive
        try:
            is_prod = _actions.is_prod_write_class(command)
        except Exception:
            is_prod = True  # cannot prove it is safe → treat as prod → block
        if is_prod:
            return _block(f"classification error on prod-write command (fail closed): {exc}")
        return next_call(args)

    if classification is None:
        return next_call(args)  # not a production-write class

    if not getattr(classification, "is_action", False):
        # UnsafeCommand — a prod-write class we cannot structurally prove.
        return _block(
            f"unstructured or unsafe production write: {classification.reason}",
            extra={"detail": classification.detail, "class": classification.reason},
        )

    action = classification

    # From here every decision-path failure returns a block (fail closed).
    try:
        ctx = bind_context()
    except _context.ContextError as exc:
        return _block(f"context could not be bound authoritatively: {exc}")
    except Exception as exc:  # pragma: no cover - defensive
        return _block(f"context error: {exc}")

    try:
        store = open_store()
    except StoreUnavailable as exc:
        return _block(f"approval store unavailable: {exc}")
    except Exception as exc:
        return _block(f"approval store error: {exc}")

    # --- Decision phase: find or create a grant, claim exactly once. ---
    try:
        key = getattr(store, "_fp_key")
        grant_fp = _fp.grant_fingerprint(action, ctx, key)
        action_fp = _fp.action_fingerprint(action, key)
        redacted = _fp.redact_argv(action, key)

        # Prefer a standalone approved grant; then an approved bundle step.
        grant = store.find_consumable(grant_fp, ctx)
        if grant is None:
            grant = store.find_bundle_step(grant_fp, ctx)

        if grant is None:
            # No approval yet.
            if action.read_only:
                # Read-only production reads (status / variable listing) are
                # never blocked and never consume a grant. Only an *approved
                # bundle step* (handled above) gates a read; a standalone read
                # passes straight through.
                _safe_close(store)
                return next_call(args)

            pending = store.request(
                grant_fp=grant_fp,
                action_fp=action_fp,
                action_class=action.action_class,
                redacted_argv=redacted,
                targets=action.targets,
                ctx=ctx,
            )
            # Deliver a one-tap approval card to the bound chat. Delivery is
            # best-effort: if there is no gateway sender (CLI, tests) the block
            # still carries the nonce for the /approve-prod fallback.
            delivered = False
            try:
                card = _cards.build_single_card(
                    ctx,
                    nonce=pending.nonce,
                    action_class=action.action_class,
                    redacted_argv=redacted,
                    targets=action.targets,
                )
                delivered = bool(deliver_card(card))
            except Exception:  # pragma: no cover - delivery must never break the gate
                delivered = False
            _safe_close(store)
            return _block(
                "production write requires approval",
                nonce=pending.nonce,
                extra={
                    "class": action.action_class,
                    "argv": list(redacted),
                    "targets": [list(t) for t in action.targets],
                    "state": pending.state,
                    "card_delivered": delivered,
                },
            )

        # Consume exactly once (CAS). A lost race / revoke / expiry / unmet
        # dependency → block.
        if not store.claim(grant.nonce, grant.fence):
            _safe_close(store)
            return _block(
                "approval was already consumed, revoked, expired, or a "
                "dependency is unmet (fail closed)",
                extra={"class": action.action_class},
            )
    except StoreUnavailable as exc:
        _safe_close(store)
        return _block(f"approval store unavailable during resolve: {exc}")
    except Exception as exc:
        _safe_close(store)
        return _block(f"approval resolution error: {exc}")

    # --- Execution phase: we hold an exclusive claim. Execute exactly once. ---
    # NOTE: a genuine tool error must propagate unchanged (never converted to a
    # block), so this is deliberately outside the decision-phase try/except.
    consumed_nonce = grant.nonce
    try:
        result = next_call(args)
    except BaseException:
        # Unknown external outcome (crash / interrupt mid-exec). Never auto-retry.
        _safe_finish(store, consumed_nonce, INDETERMINATE)
        _safe_close(store)
        raise
    # Outcome defaults to indeterminate unless success is explicitly proven —
    # so a bundle dependency never advances on an unproven step.
    _safe_finish(store, consumed_nonce, _outcome_from_result(result))
    _safe_close(store)
    return result


def _outcome_from_result(result: Any) -> str:
    """Map a tool result to a terminal outcome, conservatively.

    Only an explicitly-proven success (``ok``/``success`` True, or a zero
    ``exit_code``/``returncode``) yields ``succeeded``. An explicit failure
    yields ``failed``. Anything unparseable or ambiguous yields
    ``indeterminate`` so dependent bundle steps never advance on an unproven
    outcome and nothing is silently treated as a success.
    """
    from plugins.prod_approvals.store import SUCCEEDED, FAILED
    text = result if isinstance(result, str) else json.dumps(result, default=str)
    try:
        data = json.loads(text)
    except (ValueError, TypeError):
        return INDETERMINATE
    if not isinstance(data, dict):
        return INDETERMINATE
    if data.get("ok") is False or data.get("success") is False or data.get("error"):
        return FAILED
    exit_code = data.get("exit_code", data.get("returncode"))
    if isinstance(exit_code, bool):  # guard: bool is an int subclass
        exit_code = None
    if isinstance(exit_code, int):
        return SUCCEEDED if exit_code == 0 else FAILED
    if data.get("ok") is True or data.get("success") is True:
        return SUCCEEDED
    return INDETERMINATE


def middleware(**kwargs: Any) -> Any:
    """The registered ``tool_execution`` middleware entrypoint."""
    tool_name = kwargs.get("tool_name") or ""
    args = kwargs.get("args") or {}
    next_call = kwargs["next_call"]
    return evaluate(tool_name, args, next_call)
