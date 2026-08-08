"""An orchestrator that forgets the key must still not double-file a card.

2026-08-08: duplicate cards were created twice in one day. By 19:38 five llucky
and eleven hermes-infra cards sat in todo without a single start — among them
exact copies of cards that were already running. Deduplication by
``idempotency_key`` had existed the whole time; nothing ever passed a key to it.

So the key is derived when absent. The scope is the interesting part, and these
tests pin it: an auto key matches only another auto key, only an UNSTARTED and
still-open card, and only inside a window. Re-filing work that already ran, or
filing the same card tomorrow, is legitimate and must go through.
"""
import os

import pytest

from hermes_cli import kanban_db as kb


@pytest.fixture
def conn(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "k.db"))
    kb.init_db()
    c = kb.connect()
    yield c
    c.close()


def test_the_same_card_filed_twice_is_filed_once(conn):
    a = kb.create_task(conn, title="Harden Givi Supabase", body="do it", assignee="default")
    b = kb.create_task(conn, title="Harden Givi Supabase", body="do it", assignee="default")
    assert a == b


def test_a_different_title_is_a_different_card(conn):
    a = kb.create_task(conn, title="Card one", assignee="default")
    b = kb.create_task(conn, title="Card two", assignee="default")
    assert a != b


def test_a_different_body_is_a_different_card(conn):
    a = kb.create_task(conn, title="Same", body="first", assignee="default")
    b = kb.create_task(conn, title="Same", body="second", assignee="default")
    assert a != b


def test_a_different_assignee_is_a_different_card(conn):
    a = kb.create_task(conn, title="Same", assignee="default")
    b = kb.create_task(conn, title="Same", assignee="workbot")
    assert a != b


def test_an_explicit_key_still_wins(conn):
    a = kb.create_task(conn, title="X", assignee="default", idempotency_key="mine")
    b = kb.create_task(conn, title="totally different", assignee="w", idempotency_key="mine")
    assert a == b


def test_an_auto_key_never_collides_with_an_explicit_one(conn):
    """Explicit keys are the caller's namespace; auto keys must not invade it."""
    explicit = kb.create_task(conn, title="X", assignee="default", idempotency_key="X")
    auto = kb.create_task(conn, title="X", assignee="default")
    assert explicit != auto


def test_a_card_that_already_started_does_not_suppress_a_refile(conn):
    """Re-running the same work after it ran is legitimate."""
    first = kb.create_task(conn, title="Repeatable", assignee="default")
    conn.execute("UPDATE tasks SET status='running', started_at=1 WHERE id=?", (first,))
    conn.commit()
    second = kb.create_task(conn, title="Repeatable", assignee="default")
    assert second != first


def test_a_completed_card_does_not_suppress_a_refile(conn):
    first = kb.create_task(conn, title="Repeatable", assignee="default")
    conn.execute("UPDATE tasks SET status='done' WHERE id=?", (first,))
    conn.commit()
    assert kb.create_task(conn, title="Repeatable", assignee="default") != first


def test_a_blocked_card_does_not_suppress_a_refile(conn):
    """Blocked means a human is needed; filing a fresh attempt must be possible."""
    first = kb.create_task(conn, title="Repeatable", assignee="default")
    conn.execute("UPDATE tasks SET status='blocked' WHERE id=?", (first,))
    conn.commit()
    assert kb.create_task(conn, title="Repeatable", assignee="default") != first


def test_an_old_card_outside_the_window_does_not_suppress(conn, monkeypatch):
    first = kb.create_task(conn, title="Repeatable", assignee="default")
    conn.execute(
        "UPDATE tasks SET created_at = created_at - ? WHERE id=?",
        (kb.AUTO_IDEMPOTENCY_WINDOW + 60, first),
    )
    conn.commit()
    assert kb.create_task(conn, title="Repeatable", assignee="default") != first


def test_the_window_can_be_disabled(tmp_path, monkeypatch):
    """A deployment that wants the old behaviour must be able to have it."""
    monkeypatch.setenv("HERMES_KANBAN_AUTO_IDEMPOTENCY_WINDOW", "0")
    import importlib

    reloaded = importlib.reload(kb)
    try:
        monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "k2.db"))
        reloaded.init_db()
        c = reloaded.connect()
        try:
            a = reloaded.create_task(c, title="Same", assignee="default")
            b = reloaded.create_task(c, title="Same", assignee="default")
            assert a != b
        finally:
            c.close()
    finally:
        monkeypatch.delenv("HERMES_KANBAN_AUTO_IDEMPOTENCY_WINDOW", raising=False)
        importlib.reload(kb)
