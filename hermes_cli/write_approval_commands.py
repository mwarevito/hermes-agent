#!/usr/bin/env python3
"""Shared handlers for the /memory and /skills write-approval subcommands.

Both the interactive CLI (``cli.py``) and the gateway (``gateway/run.py``) call
into this module so the pending-review UX (list / approve / reject / diff /
mode) lives in one place. Each caller owns only its surface concerns:
formatting the returned text and, for the gateway, persisting config + evicting
the cached agent on a mode change.

Every public handler returns a plain text string suitable for both a terminal
and a chat message. Skill diffs are intentionally NOT inlined here — the
``diff`` handler returns the full diff for the CLI pager, but on a messaging
platform the gateway truncates it and points the user at the dashboard / file.
"""

from __future__ import annotations

import json
from typing import List, Optional

from tools import write_approval as wa


def _fmt_state(subsystem: str) -> str:
    on = wa.write_approval_enabled(subsystem)
    return f"{subsystem}.write_approval = {'on' if on else 'off'}"


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

# Per-record diff excerpt budget for the pending list. A skill proposal is the
# only thing here big enough to need one: the summary line ("patch 'x' SKILL.md
# (+14/-4 lines)") names the file but not the change, and approving on a name
# alone is what left seven un-reviewed proposals sitting in profile kivi's
# queue from 16-28 June 2026. So the list carries the real unified diff against
# what is on disk — the same ``wa.skill_pending_diff`` the approve path replays
# and ``diff <id>`` prints — clipped to stay inside one chat bubble. The total
# budget is what stops a 7-record queue from becoming a 40 KB message; records
# past it keep their id and a pointer to ``diff <id>``.
_DIFF_LINES_PER_RECORD = 14
_DIFF_CHARS_PER_RECORD = 700
_DIFF_TOTAL_BUDGET = 2400


def _diff_excerpt(record) -> str:
    """Clipped unified diff for one staged skill write.

    Never returns "" on failure: a diff we could not compute is reported as
    such, because a silently missing diff reads exactly like "no change" and
    would make the owner approve blind.
    """
    try:
        diff = wa.skill_pending_diff(record) or ""
    except Exception as e:  # pragma: no cover - defensive
        return f"(diff unavailable: {e})"
    diff = diff.rstrip("\n")
    if not diff:
        return "(no textual change)"
    lines = diff.split("\n")
    clipped = lines[:_DIFF_LINES_PER_RECORD]
    text = "\n".join(clipped)
    if len(text) > _DIFF_CHARS_PER_RECORD:
        text = text[:_DIFF_CHARS_PER_RECORD]
        omitted = True
    else:
        omitted = len(lines) > len(clipped)
    if omitted:
        text += f"\n… (+{len(lines) - len(clipped)} more lines)"
    return text


def _fmt_pending_list(subsystem: str) -> str:
    records = wa.list_pending(subsystem)
    if not records:
        return f"No pending {subsystem} writes."
    lines = [f"Pending {subsystem} writes ({len(records)}):"]
    budget = _DIFF_TOTAL_BUDGET
    for r in records:
        origin = r.get("origin", "foreground")
        tag = " [auto]" if origin == "background_review" else ""
        rid = r["id"]
        lines.append(f"  {rid}{tag}  {r.get('summary', '')}")
        if subsystem == wa.SKILLS:
            if budget > 0:
                excerpt = _diff_excerpt(r)
                budget -= len(excerpt)
                for dl in excerpt.split("\n"):
                    lines.append(f"    {dl}")
            else:
                lines.append(f"    (diff omitted — /skills diff {rid})")
        # The command, spelled out with the id: the owner reviews from a phone
        # and must never have to compose one or remember the syntax.
        lines.append(
            f"    approve: /{subsystem} approve {rid}"
            f"   reject: /{subsystem} reject {rid}"
        )
    if subsystem == wa.SKILLS:
        lines.append("")
        lines.append("Full diff for one record: /skills diff <id>")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Subcommand dispatch
# ---------------------------------------------------------------------------

def handle_pending_subcommand(
    subsystem: str,
    args: List[str],
    *,
    memory_store=None,
    set_mode_fn=None,
    actor=None,
    actor_channel: str = "",
) -> Optional[str]:
    """Dispatch a /memory or /skills subcommand.

    Args:
        subsystem: ``memory`` or ``skills``.
        args: tokens after the slash command (e.g. ``["approve", "a1b2"]``).
        memory_store: live MemoryStore for applying approved memory writes
            (CLI passes ``self.agent._memory_store``; gateway applies against a
            freshly loaded store).
        set_mode_fn: optional callable ``(enabled: bool) -> None`` that
            persists the new write_approval boolean to config (gateway provides
            this; CLI uses its own ``save_config_value`` and passes a closure).
        actor: who is spending the decision — the platform user id from the
            gateway, the host user from the CLI. Recorded in the decision log
            (2026-08-12): ``kivi`` now has two approvers and ``workbot`` four,
            so "the owner did it" is no longer an answer. Callers that pass
            nothing are recorded as ``unknown`` rather than omitted.
        actor_channel: where that id lives (``telegram``, ``cli``, …), because
            a bare numeric id is ambiguous across platforms.

    Returns a text string to show the user. Returns None when the args are not
    a write-approval subcommand (caller falls through to its other handling,
    e.g. /skills search).
    """
    if not args:
        # Bare /memory or /skills with no sub → show pending + gate state.
        return f"{_fmt_state(subsystem)}\n\n" + _fmt_pending_list(subsystem)

    sub = args[0].lower()
    rest = args[1:]

    if sub == "pending":
        return _fmt_pending_list(subsystem)

    if sub in {"approve", "apply"}:
        return _approve(subsystem, rest, memory_store, actor, actor_channel)

    if sub in {"reject", "deny", "drop"}:
        return _reject(subsystem, rest, actor, actor_channel)

    if sub == "diff" and subsystem == wa.SKILLS:
        return _diff(rest)

    if sub in {"approval", "mode"}:  # 'mode' kept as a back-compat alias
        return _set_approval(subsystem, rest, set_mode_fn, actor, actor_channel)

    return None  # not ours — caller handles


def _resolve_one(subsystem: str, rest: List[str]):
    if not rest:
        return None, f"Usage: /{subsystem} approve|reject <id>  (or 'all')"
    return rest[0], None


def _unaudited_warning(problems: List[str]) -> str:
    """The line shown when the decision happened but was not recorded.

    Never silent: with several approvers per profile the decision log is the
    only thing that answers "who did this", so losing an entry is a real
    defect and has to be visible to the person who just acted.
    """
    return ("⚠ NOT recorded in the decision log (the change itself DID "
            "happen) — " + "; ".join(problems))


def _approve(subsystem: str, rest: List[str], memory_store,
             actor=None, actor_channel: str = "") -> str:
    target, err = _resolve_one(subsystem, rest)
    if err or target is None:
        return err or f"Usage: /{subsystem} approve <id>"

    records = wa.list_pending(subsystem)
    if not records:
        return f"No pending {subsystem} writes."

    if target.lower() == "all":
        targets = list(records)
    else:
        rec = wa.get_pending(subsystem, target)
        if not rec:
            return f"No pending {subsystem} write with id '{target}'."
        targets = [rec]

    applied, failed, unaudited = 0, [], []
    for rec in targets:
        ok, msg = _apply_one(subsystem, rec, memory_store)
        if ok:
            wa.discard_pending(subsystem, rec["id"])
            applied += 1
        else:
            failed.append(f"{rec['id']}: {msg}")
        # Recorded for the attempt, not just the success: a failed apply by a
        # newly added approver is exactly the thing worth being able to read
        # back later.
        audit_err = wa.record_decision(
            subsystem, rec["id"], "approve",
            actor=actor, actor_channel=actor_channel,
            summary=rec.get("summary", ""), origin=rec.get("origin", ""),
            applied=ok, error="" if ok else msg,
        )
        if audit_err:
            unaudited.append(f"{rec['id']}: {audit_err}")

    out = [f"Approved {applied} {subsystem} write(s)."]
    if failed:
        out.append("Failed:")
        out.extend(f"  {f}" for f in failed)
    if unaudited:
        out.append(_unaudited_warning(unaudited))
    return "\n".join(out)


def _apply_one(subsystem: str, rec, memory_store):
    payload = rec.get("payload", {})
    try:
        if subsystem == wa.MEMORY:
            if memory_store is None:
                return False, "memory store unavailable"
            from tools.memory_tool import apply_memory_pending
            result = apply_memory_pending(payload, memory_store)
            return bool(result.get("success")), result.get("error", "")
        else:
            from tools.skill_manager_tool import apply_skill_pending
            result = json.loads(apply_skill_pending(payload))
            return bool(result.get("success")), result.get("error", "")
    except Exception as e:
        return False, str(e)


def _reject(subsystem: str, rest: List[str],
            actor=None, actor_channel: str = "") -> str:
    target, err = _resolve_one(subsystem, rest)
    if err or target is None:
        return err or f"Usage: /{subsystem} reject <id>"

    def _audit(rec_id: str, summary: str, origin: str) -> str:
        return wa.record_decision(
            subsystem, rec_id, "reject",
            actor=actor, actor_channel=actor_channel,
            summary=summary, origin=origin, applied=False,
        )

    if target.lower() == "all":
        n, unaudited = 0, []
        for rec in wa.list_pending(subsystem):
            if wa.discard_pending(subsystem, rec["id"]):
                n += 1
                audit_err = _audit(rec["id"], rec.get("summary", ""),
                                   rec.get("origin", ""))
                if audit_err:
                    unaudited.append(f"{rec['id']}: {audit_err}")
        out = f"Rejected {n} pending {subsystem} write(s)."
        return out + ("\n" + _unaudited_warning(unaudited) if unaudited else "")
    # Read before discarding: the summary is gone once the file is unlinked,
    # and a decision log that only says "some id" is not a decision log.
    rec = wa.get_pending(subsystem, target)
    if wa.discard_pending(subsystem, target):
        audit_err = _audit(target, (rec or {}).get("summary", ""),
                           (rec or {}).get("origin", ""))
        out = f"Rejected pending {subsystem} write '{target}'."
        return out + ("\n" + _unaudited_warning([f"{target}: {audit_err}"])
                      if audit_err else "")
    return f"No pending {subsystem} write with id '{target}'."


def _diff(rest: List[str]) -> str:
    if not rest:
        return "Usage: /skills diff <id>"
    rec = wa.get_pending(wa.SKILLS, rest[0])
    if not rec:
        return f"No pending skill write with id '{rest[0]}'."
    diff = wa.skill_pending_diff(rec)
    header = f"# Pending skill write {rec['id']}: {rec.get('summary', '')}\n"
    return header + "\n" + diff


def _set_approval(subsystem: str, rest: List[str], set_mode_fn,
                  actor=None, actor_channel: str = "") -> str:
    """Turn the approval gate on/off for a subsystem.

    ``set_mode_fn`` (when provided) persists the new boolean to config.

    Audited alongside approve/reject (2026-08-12): turning the gate OFF is the
    single most consequential thing this surface can do — it stops staging
    future writes altogether — so with several approvers per profile it cannot
    be the one action nobody's name is attached to.
    """
    if not rest:
        return (f"{_fmt_state(subsystem)}\n"
                f"Set with: /{subsystem} approval <on|off>")
    arg = rest[0].strip().lower()
    truthy = {"on", "true", "yes", "1", "enable", "enabled"}
    falsey = {"off", "false", "no", "0", "disable", "disabled"}
    if arg in truthy:
        enabled = True
    elif arg in falsey:
        enabled = False
    else:
        return f"Invalid value '{arg}'. Use: on or off."
    if set_mode_fn is None:
        val = "true" if enabled else "false"
        return (f"To change the {subsystem} approval gate, run:\n"
                f"  hermes config set {subsystem}.write_approval {val}")
    try:
        set_mode_fn(enabled)
    except Exception as e:
        return f"Failed to set {subsystem}.write_approval: {e}"
    audit_err = wa.record_decision(
        subsystem, "", "approval_on" if enabled else "approval_off",
        actor=actor, actor_channel=actor_channel,
        summary=f"{subsystem}.write_approval -> {'on' if enabled else 'off'}",
    )
    out = f"{subsystem}.write_approval set to '{'on' if enabled else 'off'}'."
    if audit_err:
        out += "\n" + _unaudited_warning([f"gate: {audit_err}"])
    return out
