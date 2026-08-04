"""Internal gateway-forged events must be QUEUED, not dropped, on a busy session.

Regression suite for the 2026-08-03 incident: a background-process completion
notification (forged with internal=True, user_id=None) arrived while Vito's turn
was running and was silently dropped by the #17775 sender gate — permanently,
because the process watcher makes exactly one injection attempt.

Structure mirrors tests/gateway/test_busy_session_auth_bypass.py (the #17775
suite): build a bare GatewayRunner via object.__new__ and drive
_handle_active_session_busy_message directly.
"""

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from gateway.platforms.base import MessageEvent, MessageType, Platform  # noqa: E402
from gateway.run import GatewayRunner  # noqa: E402


class _FakeAdapter:
    def __init__(self):
        self._pending_messages = {}
        self.sent = []

    async def _send_with_retry(self, **kwargs):
        self.sent.append(kwargs)
        return SimpleNamespace(message_id=1)


def _make_event(*, internal, user_id, text="notify", message_type=MessageType.TEXT):
    source = SimpleNamespace(
        platform=Platform.TELEGRAM,
        chat_id="405154434",
        chat_type="dm",
        user_id=user_id,
        user_name=None,
        thread_id=None,
        message_id=None,
    )
    event = MessageEvent(
        text=text,
        message_type=message_type,
        source=source,
        internal=internal,
    )
    return event


def _make_runner(adapter, *, authorized=False):
    runner = object.__new__(GatewayRunner)
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._draining = False
    runner._queued_events = {}
    runner._is_user_authorized = lambda source: authorized
    return runner


SESSION_KEY = "agent:main:telegram:dm:405154434:700619"


def test_internal_event_is_queued_not_dropped():
    adapter = _FakeAdapter()
    runner = _make_runner(adapter)
    event = _make_event(internal=True, user_id=None)

    handled = asyncio.run(
        runner._handle_active_session_busy_message(event, SESSION_KEY)
    )

    assert handled is True
    assert adapter._pending_messages == {}, (
        "the head slot is a merge target — internal events must never sit there"
    )
    assert runner._queued_events.get(SESSION_KEY) == [event], (
        "internal notification waits in the overflow tail"
    )
    assert adapter.sent == [], "no busy-ack for internal events"


def test_user_text_after_a_queued_notification_stays_its_own_turn():
    """The 2026-08-04 review blocker: a notification parked in the head slot
    absorbed the user's next message (texts glued together, one turn lost).

    Drives the merge site directly (_queue_or_replace_pending_event is what the
    busy path calls for an authorized user) rather than the full inbound path,
    which needs the whole gateway scaffolding."""
    adapter = _FakeAdapter()
    runner = _make_runner(adapter)
    notification = _make_event(
        internal=True, user_id=None, text="[Background process finished]"
    )
    asyncio.run(
        runner._handle_active_session_busy_message(notification, SESSION_KEY)
    )

    user_event = _make_event(
        internal=False, user_id="405154434", text="ну что там по деплою?"
    )
    runner._queue_or_replace_pending_event(SESSION_KEY, user_event)

    assert notification.text == "[Background process finished]", (
        "the notification must not absorb the user's text"
    )
    assert adapter._pending_messages.get(SESSION_KEY) is user_event, (
        "the user's message owns the head slot, not the notification"
    )
    assert notification in runner._queued_events.get(SESSION_KEY, [])


def test_user_photo_after_a_queued_notification_is_not_grafted_onto_it():
    adapter = _FakeAdapter()
    runner = _make_runner(adapter, authorized=True)
    notification = _make_event(internal=True, user_id=None, text="[proc done]")
    asyncio.run(
        runner._handle_active_session_busy_message(notification, SESSION_KEY)
    )

    assert notification.message_type == MessageType.TEXT
    assert not getattr(notification, "media_urls", None), (
        "no media may be grafted onto the notification"
    )
    assert runner._queued_events.get(SESSION_KEY) == [notification]


def test_internal_event_during_drain_is_dropped_with_a_warning():
    """In-memory queues die with the process — do not pretend delivery."""
    adapter = _FakeAdapter()
    runner = _make_runner(adapter)
    runner._draining = True
    runner._status_action_gerund = lambda: "restarting"

    event = _make_event(internal=True, user_id=None)
    handled = asyncio.run(
        runner._handle_active_session_busy_message(event, SESSION_KEY)
    )

    assert handled is True
    assert adapter._pending_messages == {}
    assert runner._queued_events.get(SESSION_KEY, []) == []
    assert adapter.sent == [], "no ack for internal events"


def test_stop_clears_queued_internal_events():
    """/stop must not leave a notification that fires after the NEXT message."""
    adapter = _FakeAdapter()
    runner = _make_runner(adapter)
    runner._running_agents = {}
    runner._pending_messages = {}
    runner._invalidate_session_run_generation = lambda *a, **k: None
    runner._release_running_agent_state = lambda *a, **k: None
    runner.adapters = {Platform.TELEGRAM: adapter}

    event = _make_event(internal=True, user_id=None)
    asyncio.run(runner._handle_active_session_busy_message(event, SESSION_KEY))
    assert runner._queued_events.get(SESSION_KEY)

    asyncio.run(
        runner._interrupt_and_clear_session(
            SESSION_KEY,
            event.source,
            interrupt_reason="stop",
            invalidation_reason="stop",
            release_running_state=False,
        )
    )

    assert not runner._queued_events.get(SESSION_KEY), "overflow cleared by /stop"


def test_internal_event_appends_behind_a_pending_user_message():
    adapter = _FakeAdapter()
    runner = _make_runner(adapter)
    user_event = _make_event(internal=False, user_id="405154434", text="user first")
    adapter._pending_messages[SESSION_KEY] = user_event

    internal_event = _make_event(internal=True, user_id=None)
    handled = asyncio.run(
        runner._handle_active_session_busy_message(internal_event, SESSION_KEY)
    )

    assert handled is True
    assert adapter._pending_messages[SESSION_KEY] is user_event, (
        "the user's message keeps the head slot"
    )
    assert internal_event in runner._queued_events.get(SESSION_KEY, []), (
        "internal notification goes to the FIFO overflow"
    )


def test_internal_event_does_not_merge_into_a_pending_photo():
    """A pending PHOTO must never absorb the notification text (that would lose
    the internal flag and turn the process dump into a photo caption)."""
    adapter = _FakeAdapter()
    runner = _make_runner(adapter)
    photo_event = _make_event(
        internal=False, user_id="405154434", text="caption",
        message_type=MessageType.PHOTO,
    )
    adapter._pending_messages[SESSION_KEY] = photo_event

    internal_event = _make_event(internal=True, user_id=None, text="proc done")
    asyncio.run(
        runner._handle_active_session_busy_message(internal_event, SESSION_KEY)
    )

    assert adapter._pending_messages[SESSION_KEY] is photo_event
    assert photo_event.text == "caption", "photo caption must not be rewritten"
    assert internal_event in runner._queued_events.get(SESSION_KEY, [])


def test_internal_event_dropped_only_at_queue_cap():
    adapter = _FakeAdapter()
    runner = _make_runner(adapter)
    adapter._pending_messages[SESSION_KEY] = _make_event(
        internal=False, user_id="405154434", text="head"
    )
    runner._queued_events[SESSION_KEY] = [
        _make_event(internal=False, user_id="405154434", text="q%d" % i)
        for i in range(GatewayRunner._BUSY_QUEUE_MAX_PENDING)
    ]
    before = len(runner._queued_events[SESSION_KEY])

    internal_event = _make_event(internal=True, user_id=None)
    handled = asyncio.run(
        runner._handle_active_session_busy_message(internal_event, SESSION_KEY)
    )

    assert handled is True
    assert len(runner._queued_events[SESSION_KEY]) == before, (
        "queue stays bounded at the cap"
    )


def test_unauthorized_non_internal_user_is_still_dropped():
    """Regression guard for #17775 — the real security case must not weaken."""
    adapter = _FakeAdapter()
    runner = _make_runner(adapter, authorized=False)
    event = _make_event(internal=False, user_id="453088539", text="intruder")

    handled = asyncio.run(
        runner._handle_active_session_busy_message(event, SESSION_KEY)
    )

    assert handled is True
    assert adapter._pending_messages == {}, "unauthorized sender is not queued"
    assert runner._queued_events.get(SESSION_KEY, []) == []
    assert adapter.sent == []


def test_non_internal_user_id_none_is_still_dropped():
    adapter = _FakeAdapter()
    runner = _make_runner(adapter, authorized=False)
    event = _make_event(internal=False, user_id=None)

    handled = asyncio.run(
        runner._handle_active_session_busy_message(event, SESSION_KEY)
    )

    assert handled is True
    assert adapter._pending_messages == {}
    assert runner._queued_events.get(SESSION_KEY, []) == []


def test_internal_event_without_adapter_is_handled_quietly():
    runner = object.__new__(GatewayRunner)
    runner.adapters = {}
    runner._draining = False
    runner._queued_events = {}
    runner._is_user_authorized = lambda source: False

    event = _make_event(internal=True, user_id=None)
    handled = asyncio.run(
        runner._handle_active_session_busy_message(event, SESSION_KEY)
    )
    assert handled is True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
