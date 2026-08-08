"""A background-completion notice reaches a busy session — in the tail, not the head.

2026-08-03: background-process completion notifications are forged in-process
with ``internal=True`` and often carry no user identity, so the #17775 sender
gate on the busy path dropped them silently — and permanently, because the
process watcher makes exactly ONE injection attempt. That is "the job finished
and nobody ever heard".

Two properties, and the second is the subtle one. The event must go to the
OVERFLOW TAIL, never the head slot: the head is a merge target, so a
notification parked there absorbs the user's next message — text glued onto a
process dump, or a photo grafted on with the dump as its caption — collapsing two
turns into one and losing the user's identity and reply anchor.

No test named this patch until now, so the version scanner could not see it and
an upgrade would have dropped it silently.
"""
import asyncio

import pytest

from gateway.config import Platform
from gateway.run import GatewayRunner


class _Source:
    def __init__(self, platform=Platform.TELEGRAM, user_id=None):
        self.platform = platform
        self.user_id = user_id
        self.user_name = user_id or "unknown"
        self.chat_id = "c1"
        self.chat_type = "dm"
        self.thread_id = None


class _Event:
    def __init__(self, internal=False, user_id=None, text="done"):
        self.internal = internal
        self.source = _Source(user_id=user_id)
        self.text = text


def _runner(*, draining=False, depth=0, authorized=False):
    r = GatewayRunner.__new__(GatewayRunner)
    r.adapters = {Platform.TELEGRAM: object()}
    r._draining = draining
    r._queued_events = {}
    r._queue_depth = lambda key, adapter=None: depth
    r._is_user_authorized = lambda source: authorized
    r._status_action_gerund = lambda: "shutting down"
    return r


def _handle(runner, event, key="s1"):
    return asyncio.run(runner._handle_active_session_busy_message(event, key))


def test_an_internal_event_is_queued_even_without_a_user_identity():
    """The incident shape: forged completion notice, user_id=None, session busy."""
    runner = _runner(authorized=False)
    assert _handle(runner, _Event(internal=True)) is True
    assert len(runner._queued_events["s1"]) == 1


def test_the_notice_lands_in_the_overflow_tail_not_the_head_slot():
    """The head slot is a merge target — a notice parked there eats the next message."""
    runner = _runner(authorized=False)
    _handle(runner, _Event(internal=True, text="job A done"))
    _handle(runner, _Event(internal=True, text="job B done"))
    texts = [e.text for e in runner._queued_events["s1"]]
    assert texts == ["job A done", "job B done"], "order lost — events were merged, not appended"


def test_a_normal_unauthorized_event_is_still_refused():
    """The #17775 gate must keep holding for real senders."""
    runner = _runner(authorized=False)
    _handle(runner, _Event(internal=False, user_id="stranger"))
    assert runner._queued_events == {}


def test_a_notice_is_dropped_loudly_while_the_gateway_is_draining(caplog):
    """The in-memory queue dies with the process — say so, do not pretend."""
    runner = _runner(draining=True)
    with caplog.at_level("WARNING"):
        assert _handle(runner, _Event(internal=True)) is True
    assert runner._queued_events == {}
    assert any("Dropping internal follow-up" in r.getMessage() for r in caplog.records)


def test_a_full_queue_refuses_instead_of_growing_without_bound(caplog):
    runner = _runner(depth=GatewayRunner._BUSY_QUEUE_MAX_PENDING)
    with caplog.at_level("WARNING"):
        assert _handle(runner, _Event(internal=True)) is True
    assert runner._queued_events == {}
    assert any("pending queue at cap" in r.getMessage() for r in caplog.records)


def test_an_unknown_platform_is_handled_without_raising():
    runner = _runner()
    runner.adapters = {}
    assert _handle(runner, _Event(internal=True)) is True
