"""Durable terminal-notification delivery for the kanban notifier.

Regression + behaviour matrix for the 2026-07-27 silent terminal-event loss.
Two root causes were confirmed and are covered here:

* RC1 — the tool auto-subscribe path stamped ``notifier_profile`` from
  ``os.environ["HERMES_PROFILE"]`` alone, which is unset for the default
  profile, persisting ``NULL`` ownership. Fixed by
  :func:`kanban_db.resolve_notifier_profile`, now shared by every creation
  path.
* RC2 — the subscription cursor advanced at *claim* time, before delivery
  was confirmed, with only an in-memory rewind on failure. A crash (or a
  send that returned without reaching the user) silently skipped the event.
  Fixed by the durable ``kanban_notify_deliveries`` lease/confirm ledger:
  the cursor only advances across a contiguous 'sent'/'dead' prefix.

The DB-level tests are the deterministic core (no event loop, no adapters);
the watcher-level tests exercise exact routing, dead-letter lifecycle, and
crash recovery through the real ``_kanban_notifier_watcher`` loop.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb
from unittest.mock import AsyncMock, MagicMock, patch


TERMINAL_KINDS = ("completed", "blocked", "gave_up", "crashed", "timed_out")


@pytest.fixture
def kanban_home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    # Ensure a clean profile-resolution environment: default profile, no
    # explicit HERMES_PROFILE (the exact condition that produced NULL owners).
    monkeypatch.delenv("HERMES_PROFILE", raising=False)
    monkeypatch.delenv("HERMES_PROFILE_NAME", raising=False)
    kb.init_db()
    return home


def _cursor(conn, tid):
    """Current subscription cursor for the single sub on ``tid``."""
    return int(kb.list_notify_subs(conn, tid)[0]["last_event_id"])


# Upstream v2026.8.3 snaps a NEW subscription's cursor to MAX(task_events.id)
# at creation (#29905: a cursor of 0 on an already-active task replayed every
# historical event on the next tick). ``create_task`` itself appends a
# ``created`` event, so a freshly-added sub starts at that id, NOT at 0. Every
# "cursor must not move" assertion below is therefore written relative to the
# cursor captured right after ``add_notify_sub`` — the invariant under test is
# "the cursor does not advance past an unconfirmed event", not "the cursor is 0".

def _sub_kwargs(tid, thread_id=""):
    return dict(
        task_id=tid, platform="telegram", chat_id="chat1", thread_id=thread_id,
    )


# ---------------------------------------------------------------------------
# RC1 — authoritative, never-NULL owner across every creation path
# ---------------------------------------------------------------------------

def test_resolve_notifier_profile_default_never_null(kanban_home):
    """Default profile (no HERMES_PROFILE env) resolves to 'default', not None."""
    assert kb.resolve_notifier_profile() == "default"


def test_resolve_notifier_profile_env_override(kanban_home, monkeypatch):
    monkeypatch.setenv("HERMES_PROFILE", "kivi")
    assert kb.resolve_notifier_profile() == "kivi"


def test_tool_autosubscribe_stamps_default_not_null(kanban_home, monkeypatch):
    """The tool auto-subscribe path must persist a concrete owner, not NULL.

    This is the exact RC1 regression: a default-profile gateway session
    creating a task via ``kanban_create`` previously wrote
    ``notifier_profile IS NULL``.
    """
    from tools import kanban_tools as kt

    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "telegram")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "405154434")
    monkeypatch.setenv("HERMES_SESSION_THREAD_ID", "698232")

    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="worker1")
        ok = kt._maybe_auto_subscribe(conn, tid)
        assert ok is True
        subs = kb.list_notify_subs(conn, tid)
    finally:
        conn.close()

    assert len(subs) == 1
    assert subs[0]["notifier_profile"] == "default"
    assert subs[0]["notifier_profile"] not in (None, "")
    # Exact routing metadata preserved.
    assert subs[0]["chat_id"] == "405154434"
    assert subs[0]["thread_id"] == "698232"


def test_cli_subscribe_stamps_default_not_null(kanban_home):
    """`add_notify_sub` with the shared resolver stamps a concrete owner."""
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="worker1")
        kb.add_notify_sub(
            conn, **_sub_kwargs(tid),
            notifier_profile=kb.resolve_notifier_profile(),
        )
        subs = kb.list_notify_subs(conn, tid)
    finally:
        conn.close()
    assert subs[0]["notifier_profile"] == "default"


# ---------------------------------------------------------------------------
# RC2 — durable lease/confirm; cursor never advances before confirmed send
# ---------------------------------------------------------------------------

def test_lease_does_not_advance_cursor(kanban_home):
    """Leasing an event must NOT move the subscription cursor (crash-before-send)."""
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="w")
        kb.add_notify_sub(conn, **_sub_kwargs(tid), notifier_profile="default")
        base_cursor = _cursor(conn, tid)
        kb._append_event(conn, tid, kind="gave_up")

        events = kb.lease_unseen_deliveries(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS, claimer="default",
        )
        assert [e.kind for e in events] == ["gave_up"]

        # Cursor is unmoved — nothing was confirmed.
        assert _cursor(conn, tid) == base_cursor

        # Simulate a crash (no confirm): a fresh lease re-delivers the event
        # rather than silently skipping it.
        events2 = kb.lease_unseen_deliveries(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS, claimer="default",
        )
        assert [e.kind for e in events2] == ["gave_up"], "event must survive a crash"
    finally:
        conn.close()


def test_confirm_advances_cursor_and_dedupes(kanban_home):
    """Crash-after-send-before-cursor-advance: confirm is durable; restart dedupes."""
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="w")
        kb.add_notify_sub(conn, **_sub_kwargs(tid), notifier_profile="default")
        base_cursor = _cursor(conn, tid)
        kb._append_event(conn, tid, kind="gave_up")

        events = kb.lease_unseen_deliveries(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS, claimer="default",
        )
        eid = events[0].id
        # Send "succeeds": confirm durably, but simulate a crash BEFORE the
        # cursor-advance call by not advancing yet.
        kb.confirm_notify_sent(conn, **_sub_kwargs(tid), event_id=eid)
        assert _cursor(conn, tid) == base_cursor, "confirm alone must not move cursor"

        # Restart: advance over the confirmed prefix — cursor catches up.
        new_cursor = kb.advance_notify_cursor_over_confirmed(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS,
        )
        assert new_cursor == eid
        subs = kb.list_notify_subs(conn, tid)
        assert int(subs[0]["last_event_id"]) == eid

        # A subsequent lease must NOT re-deliver the confirmed event.
        events2 = kb.lease_unseen_deliveries(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS, claimer="default",
        )
        assert events2 == [], "confirmed event must not re-send after restart"
    finally:
        conn.close()


def test_send_exception_never_marks_sent(kanban_home):
    """A recorded failure must never leave a 'sent' disposition (no false delivered)."""
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="w")
        kb.add_notify_sub(conn, **_sub_kwargs(tid), notifier_profile="default")
        base_cursor = _cursor(conn, tid)
        kb._append_event(conn, tid, kind="gave_up")
        events = kb.lease_unseen_deliveries(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS, claimer="default",
        )
        status = kb.record_notify_failure(
            conn, **_sub_kwargs(tid), event_id=events[0].id,
            error="403 bot blocked", retry_limit=5,
        )
        assert status == "failed"
        row = conn.execute(
            "SELECT status, attempts FROM kanban_notify_deliveries "
            "WHERE task_id=? AND event_id=?", (tid, events[0].id),
        ).fetchone()
        assert row["status"] == "failed"
        assert row["status"] != "sent"
        assert int(row["attempts"]) == 1
        # Cursor never advanced past an unsent event.
        assert kb.advance_notify_cursor_over_confirmed(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS,
        ) == base_cursor
    finally:
        conn.close()


def test_bounded_retry_then_dead_letter(kanban_home):
    """After retry_limit attempts an event is dead-lettered and the cursor advances."""
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="w")
        kb.add_notify_sub(conn, **_sub_kwargs(tid), notifier_profile="default")
        kb._append_event(conn, tid, kind="gave_up")
        events = kb.lease_unseen_deliveries(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS, claimer="default",
        )
        eid = events[0].id
        limit = 3
        statuses = []
        for _ in range(limit):
            # Re-lease then fail, mirroring one watcher tick each.
            kb.lease_unseen_deliveries(
                conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS, claimer="default",
                retry_limit=limit,
            )
            statuses.append(kb.record_notify_failure(
                conn, **_sub_kwargs(tid), event_id=eid, error="dead chat",
                retry_limit=limit,
            ))
        assert statuses == ["failed", "failed", "dead"]

        # Dead-lettered event is operator-visible...
        dl = kb.list_dead_letter_deliveries(conn, tid)
        assert len(dl) == 1 and dl[0]["event_id"] == eid

        # ...and the cursor steps past it so the sub is not wedged.
        assert kb.advance_notify_cursor_over_confirmed(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS,
        ) == eid

        # A dead event is not re-leased.
        assert kb.lease_unseen_deliveries(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS, claimer="default",
            retry_limit=limit,
        ) == []
    finally:
        conn.close()


def test_release_lease_does_not_burn_attempt(kanban_home):
    """Adapter-disconnect release returns the event to pending, attempts untouched."""
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="w")
        kb.add_notify_sub(conn, **_sub_kwargs(tid), notifier_profile="default")
        kb._append_event(conn, tid, kind="blocked")
        events = kb.lease_unseen_deliveries(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS, claimer="default",
        )
        eid = events[0].id
        kb.release_notify_leases(conn, **_sub_kwargs(tid), event_ids=[eid])
        row = conn.execute(
            "SELECT status, attempts, claimed_by FROM kanban_notify_deliveries "
            "WHERE task_id=? AND event_id=?", (tid, eid),
        ).fetchone()
        assert row["status"] == "pending"
        assert int(row["attempts"]) == 0
        assert row["claimed_by"] in (None, "")
        # Re-leasable by anyone; cursor never moved.
        again = kb.lease_unseen_deliveries(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS, claimer="default",
        )
        assert [e.id for e in again] == [eid]
    finally:
        conn.close()


def test_cursor_stops_at_first_unconfirmed_event(kanban_home):
    """Ordered dedupe: cursor advances over sent prefix, stops at a pending gap."""
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="w")
        kb.add_notify_sub(conn, **_sub_kwargs(tid), notifier_profile="default")
        base_cursor = _cursor(conn, tid)
        kb._append_event(conn, tid, kind="blocked")     # e1
        kb._append_event(conn, tid, kind="gave_up")      # e2
        events = kb.lease_unseen_deliveries(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS, claimer="default",
        )
        e1, e2 = events[0].id, events[1].id
        # Confirm only the SECOND event (out-of-order confirmation).
        kb.confirm_notify_sent(conn, **_sub_kwargs(tid), event_id=e2)
        cursor = kb.advance_notify_cursor_over_confirmed(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS,
        )
        # e1 is still pending, so the cursor must NOT jump to e2.
        assert cursor == base_cursor, "cursor must not skip an unconfirmed earlier event"
        # Now confirm e1: the whole prefix advances to e2.
        kb.confirm_notify_sent(conn, **_sub_kwargs(tid), event_id=e1)
        cursor = kb.advance_notify_cursor_over_confirmed(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS,
        )
        assert cursor == e2
    finally:
        conn.close()


def test_multi_gateway_lease_is_single_owner(kanban_home):
    """Two gateways: a live lease held by one blocks the other from re-claiming."""
    conn_a = kb.connect()
    conn_b = kb.connect()
    try:
        tid = kb.create_task(conn_a, title="t", assignee="w")
        # Legacy NULL-owned subscription: BOTH gateways consider it deliverable.
        kb.add_notify_sub(conn_a, **_sub_kwargs(tid), notifier_profile=None)
        kb._append_event(conn_a, tid, kind="gave_up")

        # Gateway A leases first (long lease so it stays live).
        a_events = kb.lease_unseen_deliveries(
            conn_a, **_sub_kwargs(tid), kinds=TERMINAL_KINDS, claimer="gw-a",
            lease_seconds=3600,
        )
        assert len(a_events) == 1

        # Gateway B (different claimer) must NOT get the event while A holds it.
        b_events = kb.lease_unseen_deliveries(
            conn_b, **_sub_kwargs(tid), kinds=TERMINAL_KINDS, claimer="gw-b",
            lease_seconds=3600,
        )
        assert b_events == [], "a live lease must be single-owner across gateways"

        # A confirms delivery — B still sees nothing (dedupe by disposition).
        kb.confirm_notify_sent(conn_a, **_sub_kwargs(tid), event_id=a_events[0].id)
        assert kb.lease_unseen_deliveries(
            conn_b, **_sub_kwargs(tid), kinds=TERMINAL_KINDS, claimer="gw-b",
        ) == []
    finally:
        conn_a.close()
        conn_b.close()


def test_failed_lease_is_reclaimable_by_other_gateway(kanban_home):
    """If the leasing gateway fails delivery, another may retry the event."""
    conn_a = kb.connect()
    conn_b = kb.connect()
    try:
        tid = kb.create_task(conn_a, title="t", assignee="w")
        kb.add_notify_sub(conn_a, **_sub_kwargs(tid), notifier_profile=None)
        kb._append_event(conn_a, tid, kind="gave_up")

        a_events = kb.lease_unseen_deliveries(
            conn_a, **_sub_kwargs(tid), kinds=TERMINAL_KINDS, claimer="gw-a",
        )
        # A can't reach the chat (wrong token) — records a failure, clears lease.
        kb.record_notify_failure(
            conn_a, **_sub_kwargs(tid), event_id=a_events[0].id,
            error="403", retry_limit=5,
        )
        # B (the token that CAN reach it) re-leases and delivers.
        b_events = kb.lease_unseen_deliveries(
            conn_b, **_sub_kwargs(tid), kinds=TERMINAL_KINDS, claimer="gw-b",
        )
        assert [e.id for e in b_events] == [a_events[0].id]
    finally:
        conn_a.close()
        conn_b.close()


def test_legacy_null_ownership_cas_no_steal(kanban_home):
    """CAS ownership: the first claimant wins; a second cannot steal it."""
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="w")
        kb.add_notify_sub(conn, **_sub_kwargs(tid), notifier_profile=None)
        assert kb.claim_notify_ownership(conn, **_sub_kwargs(tid), profile="gw-a") is True
        assert kb.claim_notify_ownership(conn, **_sub_kwargs(tid), profile="gw-b") is False
        subs = kb.list_notify_subs(conn, tid)
        assert subs[0]["notifier_profile"] == "gw-a"
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Watcher-level integration (exact routing, dead-letter, crash recovery)
# ---------------------------------------------------------------------------

def _make_runner(adapters):
    from gateway.run import GatewayRunner
    runner = object.__new__(GatewayRunner)
    runner._running = True
    runner._kanban_sub_fail_counts = {}
    runner._kanban_notifier_profile = "default"
    runner.adapters = adapters
    return runner


async def _run_watcher(runner, *, stop_after_ticks=None, timeout=10.0):
    _orig_sleep = asyncio.sleep
    tick_count = 0

    async def _fast_sleep(_):
        nonlocal tick_count
        await _orig_sleep(0)
        tick_count += 1
        if stop_after_ticks is not None and tick_count >= stop_after_ticks:
            runner._running = False

    with patch("gateway.run.asyncio.sleep", side_effect=_fast_sleep):
        await asyncio.wait_for(
            runner._kanban_notifier_watcher(interval=1), timeout=timeout,
        )


@pytest.mark.asyncio
async def test_watcher_delivers_with_exact_thread_metadata(kanban_home):
    """The subscribed thread_id must ride the send metadata unchanged."""
    from gateway.config import Platform

    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="worker1")
        kb.add_notify_sub(
            conn, task_id=tid, platform="telegram", chat_id="chat1",
            thread_id="698232", notifier_profile="default",
        )
        kb._append_event(conn, tid, kind="gave_up")
    finally:
        conn.close()

    captured = {}

    async def _send(chat_id, msg, metadata=None):
        captured["chat_id"] = chat_id
        captured["metadata"] = metadata
        runner._running = False

    adapter = MagicMock()
    adapter.send = AsyncMock(side_effect=_send)
    runner = _make_runner({Platform.TELEGRAM: adapter})

    await _run_watcher(runner)

    assert captured["chat_id"] == "chat1"
    assert captured["metadata"] == {"thread_id": "698232"}
    # Delivery is durably recorded and the cursor advanced.
    conn = kb.connect()
    try:
        row = conn.execute(
            "SELECT status FROM kanban_notify_deliveries WHERE task_id=?", (tid,),
        ).fetchone()
        subs = kb.list_notify_subs(conn, tid)
    finally:
        conn.close()
    assert row["status"] == "sent"
    assert int(subs[0]["last_event_id"]) >= 1


@pytest.mark.asyncio
async def test_watcher_send_exception_no_false_delivered_keeps_sub(kanban_home):
    """A raising send must not advance the cursor, mark 'sent', or drop the sub."""
    from gateway.config import Platform

    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="worker1")
        kb.add_notify_sub(
            conn, task_id=tid, platform="telegram", chat_id="chat1",
            notifier_profile="default",
        )
        base_cursor = _cursor(conn, tid)
        kb._append_event(conn, tid, kind="gave_up")
    finally:
        conn.close()

    adapter = MagicMock()
    adapter.send = AsyncMock(side_effect=RuntimeError("403 blocked"))
    runner = _make_runner({Platform.TELEGRAM: adapter})

    await _run_watcher(runner, stop_after_ticks=3)

    adapter.send.assert_called()  # it tried
    conn = kb.connect()
    try:
        row = conn.execute(
            "SELECT status, attempts FROM kanban_notify_deliveries WHERE task_id=?",
            (tid,),
        ).fetchone()
        subs = kb.list_notify_subs(conn, tid)
    finally:
        conn.close()
    assert row["status"] in ("failed", "dead")
    assert row["status"] != "sent", "send exception must never look delivered"
    # Cursor held while the (retryable) event is unconfirmed.
    if row["status"] == "failed":
        assert int(subs[0]["last_event_id"]) == base_cursor
    # The subscription row is preserved — never silently deleted.
    assert len(subs) == 1


@pytest.mark.asyncio
async def test_watcher_crash_recovery_redelivers_once(kanban_home):
    """First tick's send crashes; a later tick delivers exactly once."""
    from gateway.config import Platform

    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="worker1")
        kb.add_notify_sub(
            conn, task_id=tid, platform="telegram", chat_id="chat1",
            notifier_profile="default",
        )
        kb._append_event(conn, tid, kind="gave_up")
    finally:
        conn.close()

    calls = {"n": 0}

    async def _send(chat_id, msg, metadata=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient network drop")
        runner._running = False  # deliver on the 2nd attempt

    adapter = MagicMock()
    adapter.send = AsyncMock(side_effect=_send)
    runner = _make_runner({Platform.TELEGRAM: adapter})

    await _run_watcher(runner, stop_after_ticks=8)

    # Delivered after the transient failure, and durably 'sent'.
    conn = kb.connect()
    try:
        row = conn.execute(
            "SELECT status, attempts FROM kanban_notify_deliveries WHERE task_id=?",
            (tid,),
        ).fetchone()
        subs = kb.list_notify_subs(conn, tid)
    finally:
        conn.close()
    assert row["status"] == "sent"
    assert int(subs[0]["last_event_id"]) >= 1


@pytest.mark.asyncio
async def test_watcher_adapter_disconnect_releases_lease(kanban_home):
    """Adapter present at collect but gone at delivery → lease released, no advance."""
    from gateway.config import Platform

    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="worker1")
        kb.add_notify_sub(
            conn, task_id=tid, platform="telegram", chat_id="chat1",
            notifier_profile="default",
        )
        base_cursor = _cursor(conn, tid)
        kb._append_event(conn, tid, kind="gave_up")
    finally:
        conn.close()

    # A mapping whose keys() advertise TELEGRAM (so _collect leases) but whose
    # .get() returns None (adapter disconnected between collect and delivery).
    class _DisconnectedAdapters(dict):
        def get(self, _key, _default=None):
            return None

    adapters = _DisconnectedAdapters({Platform.TELEGRAM: object()})
    runner = _make_runner(adapters)

    await _run_watcher(runner, stop_after_ticks=3)

    conn = kb.connect()
    try:
        row = conn.execute(
            "SELECT status, attempts FROM kanban_notify_deliveries WHERE task_id=?",
            (tid,),
        ).fetchone()
        subs = kb.list_notify_subs(conn, tid)
    finally:
        conn.close()
    # Lease was released (pending, no attempt burned), cursor untouched.
    assert row["status"] == "pending"
    assert int(row["attempts"]) == 0
    assert int(subs[0]["last_event_id"]) == base_cursor


@pytest.mark.asyncio
async def test_watcher_dead_letter_keeps_subscription_and_advances(kanban_home, monkeypatch):
    """A permanently dead chat dead-letters the event but keeps the sub alive."""
    from gateway.config import Platform

    # Tight retry bound so the test converges in a few ticks. The watcher reads
    # the bound from the merged config (kanban.notify_retry_limit), NOT the
    # module default — patching kb.DEFAULT_NOTIFY_RETRY_LIMIT alone is inert
    # because DEFAULT_CONFIG already supplies notify_retry_limit=5. Write the
    # real config the watcher loads.
    (kanban_home / "config.yaml").write_text(
        "kanban:\n  notify_retry_limit: 2\n", encoding="utf-8",
    )

    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="worker1")
        kb.add_notify_sub(
            conn, task_id=tid, platform="telegram", chat_id="chat1",
            notifier_profile="default",
        )
        kb._append_event(conn, tid, kind="gave_up")
    finally:
        conn.close()

    adapter = MagicMock()
    adapter.send = AsyncMock(side_effect=RuntimeError("chat deleted"))
    runner = _make_runner({Platform.TELEGRAM: adapter})

    await _run_watcher(runner, stop_after_ticks=6)

    conn = kb.connect()
    try:
        dl = kb.list_dead_letter_deliveries(conn, tid)
        subs = kb.list_notify_subs(conn, tid)
    finally:
        conn.close()
    assert len(dl) == 1, "event should be dead-lettered after the retry bound"
    assert len(subs) == 1, "subscription must survive dead-letter (never dropped)"
    # Cursor advanced past the dead event so the sub is not wedged.
    assert int(subs[0]["last_event_id"]) >= 1


@pytest.mark.asyncio
async def test_watcher_terminal_lifecycle_gave_up_keeps_sub_completed_unsubs(kanban_home):
    """gave_up keeps the sub (durably sent); a later completed unsubscribes."""
    from gateway.config import Platform

    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="worker1")
        kb.add_notify_sub(
            conn, task_id=tid, platform="telegram", chat_id="chat1",
            notifier_profile="default",
        )
        kb._append_event(conn, tid, kind="gave_up")
    finally:
        conn.close()

    delivered = []

    async def _send(chat_id, msg, metadata=None):
        delivered.append(msg)
        runner._running = False

    adapter = MagicMock()
    adapter.send = AsyncMock(side_effect=_send)
    runner = _make_runner({Platform.TELEGRAM: adapter})
    await _run_watcher(runner)

    conn = kb.connect()
    try:
        subs = kb.list_notify_subs(conn, tid)
        assert len(subs) == 1, "gave_up must not unsubscribe"
        # Now the task actually completes.
        kb.complete_task(conn, tid, result="done at last")
    finally:
        conn.close()

    runner._running = True
    await _run_watcher(runner)

    conn = kb.connect()
    try:
        subs = kb.list_notify_subs(conn, tid)
    finally:
        conn.close()
    assert subs == [], "completed (task done) must unsubscribe"
    assert any("gave up" in m for m in delivered)
    assert any("done" in m for m in delivered)


# ---------------------------------------------------------------------------
# Fix: process-unique lease owner, fencing, ownership-gated dead-letter
# (B1 + concurrent same-profile blocker)
# ---------------------------------------------------------------------------

def test_same_profile_two_process_lease_exclusion(kanban_home):
    """Two processes of the SAME profile must not both hold a live lease.

    Regression for: the watcher used ``notifier_profile`` as the lease claimer,
    so two concurrent same-profile gateways shared a claimer and each treated
    the other's live pending lease as "already mine" → double delivery. The
    claimer is now process-unique (profile:pid:nonce).
    """
    conn_a = kb.connect()
    conn_b = kb.connect()
    try:
        tid = kb.create_task(conn_a, title="t", assignee="w")
        kb.add_notify_sub(conn_a, **_sub_kwargs(tid), notifier_profile="default")
        kb._append_event(conn_a, tid, kind="gave_up")
        owner_a = "default:1001:aaaa"   # same profile, distinct process tokens
        owner_b = "default:1002:bbbb"
        a = kb.lease_unseen_deliveries(
            conn_a, **_sub_kwargs(tid), kinds=TERMINAL_KINDS,
            claimer=owner_a, lease_seconds=3600,
        )
        assert len(a) == 1
        b = kb.lease_unseen_deliveries(
            conn_b, **_sub_kwargs(tid), kinds=TERMINAL_KINDS,
            claimer=owner_b, lease_seconds=3600,
        )
        assert b == [], "a live lease must exclude a second same-profile process"
    finally:
        conn_a.close()
        conn_b.close()


def test_stale_lease_owner_cannot_confirm_fail_release(kanban_home):
    """After a re-lease, the stale owner's confirm/fail/release are all no-ops."""
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="w")
        kb.add_notify_sub(conn, **_sub_kwargs(tid), notifier_profile="default")
        kb._append_event(conn, tid, kind="gave_up")
        owner_a = "default:1:aaaa"
        owner_b = "default:2:bbbb"
        # A leases with an already-expiring window (lease_seconds=0).
        ev = kb.lease_unseen_deliveries(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS,
            claimer=owner_a, lease_seconds=0,
        )
        eid = ev[0].id
        # B re-leases the expired row.
        ev_b = kb.lease_unseen_deliveries(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS,
            claimer=owner_b, lease_seconds=3600,
        )
        assert [e.id for e in ev_b] == [eid]
        # A is stale: every mutation is fenced out.
        assert kb.confirm_notify_sent(
            conn, **_sub_kwargs(tid), event_id=eid, lease_owner=owner_a,
        ) is False
        assert kb.record_notify_failure(
            conn, **_sub_kwargs(tid), event_id=eid, error="x", lease_owner=owner_a,
        ) == "stale"
        kb.release_notify_leases(
            conn, **_sub_kwargs(tid), event_ids=[eid], lease_owner=owner_a,
        )
        row = conn.execute(
            "SELECT status, claimed_by FROM kanban_notify_deliveries "
            "WHERE task_id=? AND event_id=?", (tid, eid),
        ).fetchone()
        assert row["status"] == "pending"
        assert row["claimed_by"] == owner_b, "stale owner must not mutate B's row"
        # B (the live owner) can still confirm.
        assert kb.confirm_notify_sent(
            conn, **_sub_kwargs(tid), event_id=eid, lease_owner=owner_b,
        ) is True
    finally:
        conn.close()


def test_ownerless_failure_never_dead_never_advances_then_adopts(kanban_home):
    """B1: a wrong-token gateway on an ownerless sub can never dead-letter or
    advance the cursor; a later good gateway delivers, advances, and adopts."""
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="w")
        kb.add_notify_sub(conn, **_sub_kwargs(tid), notifier_profile=None)  # legacy NULL
        base_cursor = _cursor(conn, tid)
        kb._append_event(conn, tid, kind="gave_up")
        owner_bad = "gw-bad:1:aaaa"
        ev = kb.lease_unseen_deliveries(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS, claimer=owner_bad,
        )
        eid = ev[0].id
        # Fail far past the retry bound; allow_dead=False (ownerless).
        for _ in range(10):
            kb.lease_unseen_deliveries(
                conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS,
                claimer=owner_bad, retry_limit=2,
            )
            st = kb.record_notify_failure(
                conn, **_sub_kwargs(tid), event_id=eid, error="403 wrong token",
                retry_limit=2, allow_dead=False, lease_owner=owner_bad,
            )
            assert st == "failed", "ownerless failure must never become 'dead'"
        assert kb.list_dead_letter_deliveries(conn, tid) == []
        assert kb.advance_notify_cursor_over_confirmed(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS,
        ) == base_cursor, "cursor must not advance past an undelivered ownerless event"

        # A gateway that CAN reach the chat leases, delivers, advances, adopts.
        owner_good = "gw-good:2:bbbb"
        gev = kb.lease_unseen_deliveries(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS, claimer=owner_good,
        )
        assert [e.id for e in gev] == [eid], "still deliverable by the right gateway"
        kb.confirm_notify_sent(
            conn, **_sub_kwargs(tid), event_id=eid, lease_owner=owner_good,
        )
        assert kb.advance_notify_cursor_over_confirmed(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS,
        ) == eid
        assert kb.claim_notify_ownership(
            conn, **_sub_kwargs(tid), profile="gw-good",
        ) is True
        assert kb.list_notify_subs(conn, tid)[0]["notifier_profile"] == "gw-good"
    finally:
        conn.close()


def test_owned_failure_reaches_dead_at_bound(kanban_home):
    """An OWNED sub still dead-letters at the configured bound (allow_dead=True)."""
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="w")
        kb.add_notify_sub(conn, **_sub_kwargs(tid), notifier_profile="default")
        kb._append_event(conn, tid, kind="blocked")
        owner = "default:1:aaaa"
        ev = kb.lease_unseen_deliveries(
            conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS, claimer=owner,
        )
        eid = ev[0].id
        statuses = []
        for _ in range(2):
            kb.lease_unseen_deliveries(
                conn, **_sub_kwargs(tid), kinds=TERMINAL_KINDS,
                claimer=owner, retry_limit=2,
            )
            statuses.append(kb.record_notify_failure(
                conn, **_sub_kwargs(tid), event_id=eid, error="chat deleted",
                retry_limit=2, allow_dead=True, lease_owner=owner,
            ))
        assert statuses == ["failed", "dead"]
        dl = kb.list_dead_letter_deliveries(conn, tid)
        assert len(dl) == 1 and dl[0]["event_id"] == eid
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_watcher_accepted_send_then_confirm_failure_resends(kanban_home):
    """Honest at-least-once: Telegram accepts the send but the local confirm
    write fails (crash-after-remote-accept). The event is re-sent — a duplicate
    — rather than lost. We do NOT claim strict exactly-once (no remote
    idempotency key exists for Telegram)."""
    from gateway.config import Platform

    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="worker1")
        kb.add_notify_sub(
            conn, task_id=tid, platform="telegram", chat_id="chat1",
            notifier_profile="default",
        )
        kb._append_event(conn, tid, kind="gave_up")
    finally:
        conn.close()

    sends = {"n": 0}

    async def _send(chat_id, msg, metadata=None):
        sends["n"] += 1

    adapter = MagicMock()
    adapter.send = AsyncMock(side_effect=_send)
    runner = _make_runner({Platform.TELEGRAM: adapter})

    # Break only the FIRST confirm — the remote already accepted the message,
    # then the durable write fails. The real confirm runs on the retry.
    real_confirm = runner._kanban_confirm_sent
    confirm_calls = {"n": 0}

    def _confirm(*a, **k):
        confirm_calls["n"] += 1
        if confirm_calls["n"] == 1:
            raise RuntimeError("db write failed after remote accept")
        r = real_confirm(*a, **k)
        runner._running = False  # stop once the second (durable) confirm lands
        return r

    runner._kanban_confirm_sent = _confirm

    await _run_watcher(runner, stop_after_ticks=8)

    assert sends["n"] >= 2, "accepted-send + confirm-failure must re-send (at-least-once)"
    conn = kb.connect()
    try:
        row = conn.execute(
            "SELECT status FROM kanban_notify_deliveries WHERE task_id=?", (tid,),
        ).fetchone()
    finally:
        conn.close()
    assert row["status"] == "sent", "the re-send is eventually confirmed durably"


@pytest.mark.asyncio
async def test_watcher_unknown_terminal_kind_does_not_wedge(kanban_home, monkeypatch):
    """A future terminal kind with no message template must not wedge the cursor.

    It is leased (in TERMINAL_KINDS) but unrenderable by this build; the watcher
    confirms it (advancing the cursor) and logs, instead of retrying forever.
    """
    from gateway.config import Platform
    import gateway.kanban_watchers as kw

    monkeypatch.setattr(kw, "TERMINAL_KINDS", kw.TERMINAL_KINDS + ("future_kind",))

    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="t", assignee="worker1")
        kb.add_notify_sub(
            conn, task_id=tid, platform="telegram", chat_id="chat1",
            notifier_profile="default",
        )
        kb._append_event(conn, tid, kind="future_kind")
    finally:
        conn.close()

    adapter = MagicMock()
    adapter.send = AsyncMock()
    runner = _make_runner({Platform.TELEGRAM: adapter})

    await _run_watcher(runner, stop_after_ticks=3)

    adapter.send.assert_not_called()  # no template → nothing is sent
    conn = kb.connect()
    try:
        row = conn.execute(
            "SELECT status FROM kanban_notify_deliveries WHERE task_id=?", (tid,),
        ).fetchone()
        subs = kb.list_notify_subs(conn, tid)
    finally:
        conn.close()
    assert row["status"] == "sent", "unrenderable kind is confirmed, not left pending"
    assert int(subs[0]["last_event_id"]) >= 1, "cursor advanced past the unrenderable event"
    assert len(subs) == 1, "subscription preserved"
