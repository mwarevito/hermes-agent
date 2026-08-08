"""An unlisted chat must be discoverable, not merely silent.

2026-08-08: chat -1004386716667 ("Llucky / Ops") was absent from workbot's
``group_allowed_chats`` and had been since at least 25.07, so every forward Vito
made into it was dropped by the whitelist. The database holds ZERO observed
messages from that chat for all time, and nothing was ever logged — from the
outside the bot simply "did not react", and the hole stayed invisible for two
weeks.

One DEBUG line per chat per hour. The rate limit is the load-bearing half: a
guard that narrates every message in a busy unlisted group gets muted, which
recreates the same blindness by another route.
"""
import logging

import pytest

from plugins.platforms.telegram.adapter import (
    _UNLISTED_CHAT_LOG_INTERVAL,
    TelegramAdapter,
)


class _Adapter(TelegramAdapter):
    """``name`` is a read-only property on the real adapter — override it."""

    name = "telegram"

    def __init__(self):  # no real construction: only the logger helper is used
        pass


def _adapter():
    return _Adapter()


def _drop(adapter, chat_id="-1004386716667", guest=False):
    TelegramAdapter._log_unlisted_chat_drop(adapter, chat_id, guest)


def test_the_first_dropped_message_names_the_chat(caplog):
    adapter = _adapter()
    with caplog.at_level(logging.DEBUG):
        _drop(adapter)
    msgs = [r.getMessage() for r in caplog.records]
    assert any("-1004386716667" in m for m in msgs)


def test_the_line_says_how_to_admit_the_chat(caplog):
    """A log line that only says 'dropped' costs a search; this one ends it."""
    adapter = _adapter()
    with caplog.at_level(logging.DEBUG):
        _drop(adapter)
    text = " ".join(r.getMessage() for r in caplog.records)
    assert "group_allowed_chats" in text
    assert "allow this group" in text


def test_it_is_debug_not_info(caplog):
    """INFO on a chatty unlisted group is how a guard earns a mute."""
    adapter = _adapter()
    with caplog.at_level(logging.DEBUG):
        _drop(adapter)
    assert [r.levelno for r in caplog.records] == [logging.DEBUG]


def test_a_second_message_from_the_same_chat_stays_quiet(caplog):
    adapter = _adapter()
    with caplog.at_level(logging.DEBUG):
        _drop(adapter)
        _drop(adapter)
        _drop(adapter)
    assert len(caplog.records) == 1


def test_a_different_chat_gets_its_own_line(caplog):
    """Rate limiting is per chat — one noisy group must not hide another."""
    adapter = _adapter()
    with caplog.at_level(logging.DEBUG):
        _drop(adapter, chat_id="-100111")
        _drop(adapter, chat_id="-100222")
    assert len(caplog.records) == 2


def test_it_speaks_again_after_the_interval(caplog, monkeypatch):
    """A long-running gateway must not hide the hole forever."""
    import plugins.platforms.telegram.adapter as mod

    adapter = _adapter()
    clock = {"t": 1000.0}
    monkeypatch.setattr(mod.time, "time", lambda: clock["t"])
    with caplog.at_level(logging.DEBUG):
        _drop(adapter)
        clock["t"] += _UNLISTED_CHAT_LOG_INTERVAL + 1
        _drop(adapter)
    assert len(caplog.records) == 2


def test_a_guest_mention_is_not_reported_as_a_drop(caplog):
    """Guest mode let it through — reporting a drop would be a lie."""
    adapter = _adapter()
    with caplog.at_level(logging.DEBUG):
        _drop(adapter, guest=True)
    assert caplog.records == []
