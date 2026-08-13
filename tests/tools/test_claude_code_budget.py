"""Acceptance for the `budget-exhausted-is-not-an-error` local patch.

Canon: hermes-ops `infra/orchestration-deadlock-20260813.md` §2 — 13.08.2026
three terminal-lane cards were killed by `--max-budget-usd` and their work
discarded whole, because a cap surfaced as status `error` and an `error` is
(correctly) never resumed.

This file is the patch's acceptance INSIDE the pinned repo, which is the only
place `hermes-patch-verify` can run it. hermes-ops carries the richer suite
(`tests/test_claude_code_budget.py`, ~40 checks against a patched copy); what is
pinned here is the contract that must not silently disappear at the next repin:

  * a cap kill is its own outcome and IS resumed, bounded;
  * a genuine error is still NEVER resumed;
  * the cap claim is anchored to how claude actually says it, so a run that
    merely mentions the wording is not escalated;
  * a cap-killed chain reports the sum of the caps it burned, not the last one.
"""
import pytest

import tools.claude_code_core as core


# ---------------------------------------------------------------------------
# The outcome itself
# ---------------------------------------------------------------------------

def test_a_cap_kill_is_resumed():
    sid = "11111111-2222-3333-4444-555555555555"
    d = core.resume_decision(
        {"status": core.BUDGET_EXHAUSTED_STATUS, "claude_session_id": sid,
         "budget_resume_count": 0}, True)
    assert d["resume"] is True
    assert d["claude_session_id"] == sid
    assert d["budget_resume_count"] == 1


def test_a_real_error_is_still_never_resumed():
    sid = "11111111-2222-3333-4444-555555555555"
    d = core.resume_decision({"status": "error", "claude_session_id": sid}, True)
    assert d["resume"] is False


def test_budget_resumes_are_bounded():
    sid = "11111111-2222-3333-4444-555555555555"
    prev = {"status": core.BUDGET_EXHAUSTED_STATUS, "claude_session_id": sid,
            "budget_resume_count": core.DEFAULT_MAX_BUDGET_RESUMES}
    d = core.resume_decision(prev, True)
    assert d["resume"] is False
    assert "budget already raised" in d["reason"]


def test_a_cap_kill_without_a_workspace_is_not_resumed():
    sid = "11111111-2222-3333-4444-555555555555"
    d = core.resume_decision(
        {"status": core.BUDGET_EXHAUSTED_STATUS, "claude_session_id": sid}, False)
    assert d["resume"] is False


# ---------------------------------------------------------------------------
# Recognising the cap — anchored, not scanned
# ---------------------------------------------------------------------------

def test_the_live_wording_is_recognised():
    # Measured on the M1 2026-08-13: output.txt of t_fd8e442d was exactly this.
    assert core.budget_exhausted_in_text("Error: Exceeded USD budget (8)\n")


def test_a_run_that_only_mentions_the_wording_is_not_a_cap_kill():
    # A card about this very code fails for a real reason. Escalating it doubles
    # the cap up to three times and reports spend nobody spent.
    text = ('...\nтам строка "Exceeded USD budget" в маркерах\n'
            'Traceback (most recent call last):\nRuntimeError: boom\n')
    assert not core.budget_exhausted_in_text(text)


def test_the_marker_must_be_the_last_word_not_buried_in_the_tail():
    text = "Error: Exceeded USD budget (8)\n" + ("work continues\n" * 50)
    assert not core.budget_exhausted_in_text(text)


# ---------------------------------------------------------------------------
# Escalation arithmetic
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("prev,ceiling,expected", [
    (8, 50, 16.0),
    (32, 50, 50.0),
    (50, 50, None),
    (0, 50, None),
    (None, 50, None),
    ("nonsense", 50, None),
])
def test_next_budget_usd(prev, ceiling, expected):
    assert core.next_budget_usd(prev, ceiling) == expected


# ---------------------------------------------------------------------------
# Spend: an escalated chain costs the sum of its caps
# ---------------------------------------------------------------------------

def test_a_cap_killed_chain_reports_the_sum_of_its_caps(tmp_path):
    spend = core.read_spend(
        str(tmp_path), budget_usd=32.0, exhausted=True,
        budget_history=[{"budget_usd": 8.0, "outcome": "exhausted"},
                        {"budget_usd": 16.0, "outcome": "exhausted"},
                        {"budget_usd": 32.0, "outcome": "exhausted"}])
    assert spend["cost_usd"] == 56.0
    assert spend["cost_source"] == "budget-cap-floor"
    assert spend["cost_scope"] == "chain"
    assert spend["cost_breakdown_usd"] == [8.0, 16.0, 32.0]


def test_a_single_cap_kill_is_still_scoped_to_the_run(tmp_path):
    spend = core.read_spend(str(tmp_path), budget_usd=8.0, exhausted=True,
                            budget_history=[{"budget_usd": 8.0,
                                             "outcome": "exhausted"}])
    assert spend["cost_usd"] == 8.0
    assert spend["cost_scope"] == "run"


def test_spend_without_a_source_is_reported_as_unknown_not_zero(tmp_path):
    spend = core.read_spend(str(tmp_path), budget_usd=None, exhausted=False)
    assert spend["cost_usd"] is None
    assert spend["cost_source"] == "unknown"
