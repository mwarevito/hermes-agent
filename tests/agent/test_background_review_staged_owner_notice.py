"""A staged self-improvement proposal is an OWNER-only notice, and it must
carry the command that spends it.

Two defects this pins (2026-08-12):

1. The ⏸ line said "review with ``/skills pending`` in the CLI" and named no
   id, so the owner had to be at the host terminal AND remember the syntax.
   Measured consequence: seven proposals sat unreviewed in
   ``~/.hermes/profiles/kivi/pending/skills/`` from 16–28 June.
2. It rode ``agent.background_review_callback``, which the gateway points at
   the chat the turn happened in. Profile ``kivi`` (Gogi) talks to Llucky
   CLIENTS and has no ``display.memory_notifications`` key, so it takes the
   "on" default — a prospect would read the bot's internal curation queue.
"""

import json

import pytest


def _msgs(tool_result: dict, *, name="report-writer", action="patch"):
    """Minimal review-agent transcript: one skill_manage call + its result."""
    return [
        {
            "role": "assistant",
            "tool_calls": [{
                "id": "call_1",
                "function": {
                    "name": "skill_manage",
                    "arguments": json.dumps({"action": action, "name": name}),
                },
            }],
        },
        {
            "role": "tool",
            "tool_call_id": "call_1",
            "content": json.dumps(tool_result),
        },
    ]


_STAGED = {
    "success": True,
    "staged": True,
    "pending_id": "8f2bb0dc",
    "gist": "patch 'report-writer' SKILL.md (+1/-1 lines)",
    "message": "Staged for approval",
}


def test_staged_line_carries_the_approve_command():
    from agent.background_review import summarize_background_review_actions

    owner_only: list = []
    actions = summarize_background_review_actions(
        _msgs(_STAGED), [], owner_only_sink=owner_only,
    )
    assert owner_only, "staged proposal must produce an owner-only notice"
    line = owner_only[0]
    assert "/skills approve 8f2bb0dc" in line, line
    assert "/skills reject 8f2bb0dc" in line, line
    # Nothing about the proposal may ride the public rail.
    assert actions == [], actions


def test_staged_line_is_not_dropped_without_a_sink():
    """CLI callers pass no sink — the notice must degrade to public, not vanish."""
    from agent.background_review import summarize_background_review_actions

    actions = summarize_background_review_actions(_msgs(_STAGED), [])
    assert any("8f2bb0dc" in a for a in actions), actions


def test_non_staged_actions_stay_on_the_public_rail():
    from agent.background_review import summarize_background_review_actions

    owner_only: list = []
    actions = summarize_background_review_actions(
        _msgs({"success": True, "message": "Skill 'report-writer' updated."}),
        [], owner_only_sink=owner_only,
    )
    assert owner_only == [], owner_only
    assert any("updated" in a for a in actions), actions


# ---------------------------------------------------------------------------
# Delivery split
# ---------------------------------------------------------------------------

class _Agent:
    def __init__(self, owner_cb=None, public_cb=None):
        self.printed: list = []
        self.public: list = []
        self.owner: list = []
        self.background_review_callback = public_cb or (
            lambda m: self.public.append(m)
        )
        if owner_cb is not None:
            self.background_review_owner_callback = owner_cb

    def _safe_print(self, text):
        self.printed.append(text)


def test_owner_line_goes_only_to_the_owner_rail():
    from agent.background_review import deliver_review_summary

    a = _Agent()
    a.background_review_owner_callback = lambda m: a.owner.append(m)
    deliver_review_summary(a, ["Memory updated."], ["⏸ proposal /skills approve 8f2bb0dc"])
    assert any("8f2bb0dc" in m for m in a.owner), a.owner
    assert not any("8f2bb0dc" in m for m in a.public), a.public
    assert any("Memory updated." in m for m in a.public), a.public


def test_owner_line_is_suppressed_not_leaked_when_no_owner_rail(caplog):
    """kivi in a client chat: no owner rail → say nothing there, log loudly."""
    from agent.background_review import deliver_review_summary

    a = _Agent()
    with caplog.at_level("WARNING"):
        deliver_review_summary(a, [], ["⏸ proposal /skills approve 8f2bb0dc"])
    assert a.public == [], a.public
    assert any("8f2bb0dc" in r.getMessage() for r in caplog.records), caplog.text


def test_terminal_still_sees_everything():
    from agent.background_review import deliver_review_summary

    a = _Agent()
    a.background_review_owner_callback = lambda m: a.owner.append(m)
    deliver_review_summary(a, ["Memory updated."], ["⏸ proposal /skills approve 8f2bb0dc"])
    joined = "\n".join(a.printed)
    assert "Memory updated." in joined and "8f2bb0dc" in joined, joined
