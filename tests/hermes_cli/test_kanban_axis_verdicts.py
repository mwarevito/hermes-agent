"""A card that declares acceptance criteria cannot be closed past them.

2026-08-08: a reviewer looped five times and returned "APPROVED 10/10" having
checked exactly one thing — contact provenance — while the card listed more. A
single global verdict has no axes, so it cannot be wrong per-axis; it can only
be wrong all at once, and nothing downstream can tell a thorough approval from a
lazy one.

The guard is OPT-IN by construction: a card that declares no criteria is
untouched. That half is pinned hardest, because a guard that blocked every
completion would be switched off within a day — and then the axes would be both
unenforced and unenforceable.
"""
import pytest

from hermes_cli import kanban_db as kb
from hermes_cli.kanban_db import _declared_criteria, _unaddressed_criteria


@pytest.fixture
def conn(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "k.db"))
    kb.init_db()
    c = kb.connect()
    yield c
    c.close()


BODY = "Сделать X.\n\n- [ ] контакты из источника\n- [ ] тексты по канону\n"


# --- the parser -------------------------------------------------------------

def test_checkboxes_are_the_criteria():
    assert _declared_criteria(BODY) == ["контакты из источника", "тексты по канону"]


def test_checked_boxes_count_too():
    assert _declared_criteria("- [x] уже сделано\n") == ["уже сделано"]


def test_prose_is_not_a_criterion():
    assert _declared_criteria("Просто описание задачи без списка") == []


def test_a_plain_bullet_is_not_a_criterion():
    """Only checkboxes declare criteria — ordinary bullets are just prose."""
    assert _declared_criteria("- сделать хорошо\n- не сделать плохо\n") == []


def test_no_body_declares_nothing():
    assert _declared_criteria(None) == []


# --- the rule ---------------------------------------------------------------

def test_a_card_without_criteria_is_never_blocked():
    """The opt-in half: today's boards are untouched."""
    assert _unaddressed_criteria("обычное описание", "готово") == []


def test_a_global_approval_addresses_nothing():
    """'APPROVED 10/10' is the exact shape that slipped through on 08.08."""
    assert _unaddressed_criteria(BODY, "APPROVED 10/10") == [
        "контакты из источника", "тексты по канону",
    ]


def test_a_verdict_per_criterion_passes():
    text = "контакты из источника — PASS\nтексты по канону — FAIL, канон не читался"
    assert _unaddressed_criteria(BODY, text) == []


def test_a_partial_verdict_names_what_is_missing():
    assert _unaddressed_criteria(BODY, "контакты из источника — PASS") == [
        "тексты по канону",
    ]


def test_quoting_the_criteria_without_any_outcome_is_not_a_verdict():
    """Restating the checklist is not the same as judging it."""
    text = "контакты из источника\nтексты по канону"
    assert len(_unaddressed_criteria(BODY, text)) == 2


def test_russian_outcome_tokens_count():
    text = "контакты из источника — ПРОШЛО\nтексты по канону — НЕ ПРОШЛО"
    assert _unaddressed_criteria(BODY, text) == []


def test_an_empty_completion_addresses_nothing():
    assert len(_unaddressed_criteria(BODY, None)) == 2


# --- the enforcement --------------------------------------------------------

def test_completing_past_the_criteria_is_refused(conn):
    tid = kb.create_task(conn, title="review me", body=BODY, assignee="w")
    with pytest.raises(ValueError) as exc:
        kb.complete_task(conn, tid, result="APPROVED 10/10")
    assert "тексты по канону" in str(exc.value)


def test_the_refusal_says_how_to_satisfy_it(conn):
    tid = kb.create_task(conn, title="review me", body=BODY, assignee="w")
    with pytest.raises(ValueError) as exc:
        kb.complete_task(conn, tid, result="APPROVED")
    assert "PASS/FAIL" in str(exc.value)


def test_a_per_axis_verdict_completes(conn):
    tid = kb.create_task(conn, title="review me", body=BODY, assignee="w")
    assert kb.complete_task(
        conn, tid,
        result="контакты из источника — PASS; тексты по канону — PASS",
    ) is True


def test_an_ordinary_card_still_completes_with_one_word(conn):
    """The opt-in half again, this time end to end."""
    tid = kb.create_task(conn, title="plain", body="просто сделать", assignee="w")
    assert kb.complete_task(conn, tid, result="готово") is True


def test_a_card_with_no_body_still_completes(conn):
    tid = kb.create_task(conn, title="no body", assignee="w")
    assert kb.complete_task(conn, tid, result="готово") is True


def test_the_summary_counts_as_the_verdict_too(conn):
    """Callers may pass structured handoff in summary rather than result."""
    tid = kb.create_task(conn, title="review me", body=BODY, assignee="w")
    assert kb.complete_task(
        conn, tid, result="см. summary",
        summary="контакты из источника — PASS; тексты по канону — PASS",
    ) is True
