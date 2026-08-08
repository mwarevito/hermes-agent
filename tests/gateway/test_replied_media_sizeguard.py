"""A replied-to screenshot must not be dropped for having no declared size.

Telegram frequently omits ``file_size`` on nested ``reply_to_message`` media —
notably on the LARGEST PhotoSize, which is exactly the one worth fetching. The
old ``0 < size`` guard read "unknown" as "reject" and silently dropped the
screenshots people reply with, which is the single most common way an image
reaches the bot.

The patch only rejects a size that is KNOWN to exceed the cap, and re-checks the
real byte length after download so an absent declaration cannot smuggle a huge
file through. Both halves are pinned here; until now no test named this patch, so
the version scanner could not see it and an upgrade would have dropped it.
"""
import pytest

from plugins.platforms.telegram.adapter import TelegramAdapter


class _Event:
    def __init__(self):
        self.media_urls = []
        self.media_types = []
        self.text = ""
        self.message_type = None


class _Source:
    def __init__(self, file_size, payload=b"x" * 10):
        self.file_size = file_size
        self._payload = payload

    async def get_file(self):
        payload = self._payload

        class _F:
            file_path = "photo.jpg"

            async def download_as_bytearray(self):
                return bytearray(payload)

        return _F()


class _Msg:
    def __init__(self, reply):
        self.reply_to_message = reply


def _adapter(source, max_bytes=100):
    a = TelegramAdapter.__new__(TelegramAdapter)
    a._max_doc_bytes = max_bytes
    a._observed_media_source = lambda m: (source, "photo.jpg", "image/jpeg", "image")
    return a


def _run(adapter, msg, event, monkeypatch, cached_path="/tmp/cached.jpg"):
    import asyncio

    import gateway.platforms.base as base

    class _Cached:
        path = cached_path
        media_type = "image/jpeg"
        kind = "image"
        display_name = "photo.jpg"

    monkeypatch.setattr(base, "cache_media_bytes", lambda *a, **k: _Cached(), raising=False)
    adapter._append_observed_note = lambda text, note: (text or "") + note
    asyncio.run(TelegramAdapter._cache_replied_media(adapter, msg, event))
    return event


def test_media_with_no_declared_size_is_still_fetched(monkeypatch):
    """The incident shape: file_size omitted on the largest PhotoSize."""
    event = _Event()
    adapter = _adapter(_Source(file_size=None, payload=b"x" * 10))
    _run(adapter, _Msg(object()), event, monkeypatch)
    assert event.media_urls == ["/tmp/cached.jpg"]


def test_media_declared_under_the_cap_is_fetched(monkeypatch):
    event = _Event()
    adapter = _adapter(_Source(file_size=10, payload=b"x" * 10))
    _run(adapter, _Msg(object()), event, monkeypatch)
    assert event.media_urls == ["/tmp/cached.jpg"]


def test_media_declared_over_the_cap_is_refused_without_downloading(monkeypatch):
    event = _Event()
    source = _Source(file_size=10_000)

    async def _boom():
        raise AssertionError("must not download a file declared over the cap")

    source.get_file = _boom
    _run(_adapter(source), _Msg(object()), event, monkeypatch)
    assert event.media_urls == []


def test_an_undeclared_file_that_turns_out_huge_is_still_refused(monkeypatch):
    """The second half: absence of a declaration must not smuggle it through."""
    event = _Event()
    adapter = _adapter(_Source(file_size=None, payload=b"x" * 5_000))
    _run(adapter, _Msg(object()), event, monkeypatch)
    assert event.media_urls == []


def test_a_garbage_declared_size_is_treated_as_unknown(monkeypatch):
    event = _Event()
    adapter = _adapter(_Source(file_size="не число", payload=b"x" * 10))
    _run(adapter, _Msg(object()), event, monkeypatch)
    assert event.media_urls == ["/tmp/cached.jpg"]


def test_a_message_that_is_not_a_reply_does_nothing(monkeypatch):
    event = _Event()
    adapter = _adapter(_Source(file_size=None))
    _run(adapter, _Msg(None), event, monkeypatch)
    assert event.media_urls == []


def test_a_reply_without_media_does_nothing(monkeypatch):
    event = _Event()
    adapter = TelegramAdapter.__new__(TelegramAdapter)
    adapter._max_doc_bytes = 100
    adapter._observed_media_source = lambda m: (None, None, None, None)
    _run(adapter, _Msg(object()), event, monkeypatch)
    assert event.media_urls == []


def test_a_download_failure_is_swallowed_not_raised(monkeypatch):
    """A broken fetch must not take down the whole message-handling path."""
    event = _Event()
    source = _Source(file_size=None)

    async def _boom():
        raise RuntimeError("telegram said no")

    source.get_file = _boom
    _run(_adapter(source), _Msg(object()), event, monkeypatch)
    assert event.media_urls == []
