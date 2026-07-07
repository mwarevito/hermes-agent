"""
Tests for Telegram Business / Secretary Mode ingestion in
plugins/platforms/telegram/adapter.py.

Secretary Mode lets a Telegram Business account connect the bot so it can see
the owner's managed DMs. The adapter treats these strictly as *observed*
context: business updates are stored in session history but never dispatch the
agent and never auto-reply. Authorization is keyed on the Business connection
*owner* (the account that linked the bot), not on the other DM participant.

These tests build Telegram objects with SimpleNamespace rather than MagicMock
on purpose: a bare MagicMock auto-vivifies every attribute to a truthy child
mock, which both hides bugs and (historically) tripped the business-message
guard. SimpleNamespace models real attribute presence/absence.
"""

import os
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.telegram.adapter import TelegramAdapter


OWNER_ID = "405154434"
OTHER_ID = "158338933"  # a managed-DM counterparty, never the authorizer


# ---------------------------------------------------------------------------
# Fakes / builders
# ---------------------------------------------------------------------------

class _FakeSession:
    def __init__(self, session_id="sess-1"):
        self.session_id = session_id


class _FakeStore:
    """Minimal session store capturing transcript appends."""

    def __init__(self):
        self.appends = []  # list of (session_id, entry)
        self.sessions_for = []  # sources passed to get_or_create_session

    def get_or_create_session(self, source):
        self.sessions_for.append(source)
        return _FakeSession()

    def append_to_transcript(self, session_id, entry):
        self.appends.append((session_id, entry))


def _make_connection(conn_id="bc-1", owner_id=OWNER_ID, *, is_enabled=True, can_reply=False):
    return SimpleNamespace(
        id=conn_id,
        user=SimpleNamespace(id=owner_id, full_name="Vito Owner", first_name="Vito"),
        user_chat_id=owner_id,
        is_enabled=is_enabled,
        rights=SimpleNamespace(can_reply=can_reply),
    )


def _make_business_message(
    conn_id="bc-1",
    chat_id="900900",
    text="hello from a managed DM",
    from_id=OTHER_ID,
    message_id=77,
    caption=None,
):
    return SimpleNamespace(
        business_connection_id=conn_id,
        chat=SimpleNamespace(id=chat_id, full_name="Customer Jane", title=None),
        from_user=SimpleNamespace(id=from_id, full_name="Customer Jane", first_name="Jane"),
        text=text,
        caption=caption,
        message_id=message_id,
        # media flags absent -> all None
        photo=None,
        video=None,
        audio=None,
        voice=None,
        document=None,
        sticker=None,
    )


def _update_business(message, *, edited=False, update_id=5):
    return SimpleNamespace(
        update_id=update_id,
        business_message=None if edited else message,
        edited_business_message=message if edited else None,
    )


def _update_normal_text(text="regular dm"):
    """A normal (non-business) update, as PTB would deliver it."""
    return SimpleNamespace(
        update_id=1,
        business_message=None,
        edited_business_message=None,
        message=SimpleNamespace(text=text),
    )


def _update_connection(connection):
    return SimpleNamespace(business_connection=connection)


def _update_deleted(conn_id="bc-1", chat_id="900900", message_ids=(1, 2, 3)):
    return SimpleNamespace(
        deleted_business_messages=SimpleNamespace(
            business_connection_id=conn_id,
            chat=SimpleNamespace(id=chat_id, full_name="Customer Jane", title=None),
            message_ids=list(message_ids),
        )
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def store():
    return _FakeStore()


def _make_adapter(store, *, extra=None, authorized_owner=OWNER_ID):
    config = PlatformConfig(enabled=True, token="fake-token", extra=extra or {})
    a = TelegramAdapter(config)
    a.handle_message = AsyncMock()
    a._session_store = store
    # Authorize only the given owner id (None -> authorize nobody).
    a._is_callback_user_authorized = lambda user_id, **_kw: (
        authorized_owner is not None and str(user_id) == str(authorized_owner)
    )
    return a


@pytest.fixture()
def adapter(store):
    return _make_adapter(store, extra={"business_secretary_enabled": True})


@pytest.fixture(autouse=True)
def _clear_business_env(monkeypatch):
    for var in (
        "TELEGRAM_BUSINESS_SECRETARY_ENABLED",
        "TELEGRAM_BUSINESS_OBSERVE_MESSAGES",
        "TELEGRAM_BUSINESS_AUTO_REPLY",
        "TELEGRAM_BUSINESS_ALLOWED_CHATS",
        "TELEGRAM_BUSINESS_BLOCKED_CHATS",
    ):
        monkeypatch.delenv(var, raising=False)


# ---------------------------------------------------------------------------
# Connection lifecycle + owner authorization
# ---------------------------------------------------------------------------

class TestBusinessConnectionAuth:
    @pytest.mark.asyncio
    async def test_authorized_owner_connection_is_stored(self, adapter):
        await adapter._handle_business_connection(_update_connection(_make_connection()), None)
        assert "bc-1" in adapter._business_connections
        assert adapter._business_connections["bc-1"]["user_id"] == OWNER_ID

    @pytest.mark.asyncio
    async def test_unauthorized_owner_connection_is_rejected(self, store):
        adapter = _make_adapter(store, extra={"business_secretary_enabled": True}, authorized_owner=None)
        await adapter._handle_business_connection(_update_connection(_make_connection()), None)
        assert "bc-1" not in adapter._business_connections

    @pytest.mark.asyncio
    async def test_disabled_connection_is_removed(self, adapter):
        await adapter._handle_business_connection(_update_connection(_make_connection()), None)
        assert "bc-1" in adapter._business_connections
        await adapter._handle_business_connection(
            _update_connection(_make_connection(is_enabled=False)), None
        )
        assert "bc-1" not in adapter._business_connections

    @pytest.mark.asyncio
    async def test_connection_ignored_when_secretary_disabled(self, store):
        adapter = _make_adapter(store, extra={"business_secretary_enabled": False})
        await adapter._handle_business_connection(_update_connection(_make_connection()), None)
        assert adapter._business_connections == {}


# ---------------------------------------------------------------------------
# Message ingestion = observe only, never dispatch
# ---------------------------------------------------------------------------

class TestBusinessMessageIngestion:
    @pytest.mark.asyncio
    async def test_business_message_observed_not_dispatched(self, adapter, store):
        await adapter._handle_business_connection(_update_connection(_make_connection()), None)
        consumed = await adapter._consume_business_message_update(
            _update_business(_make_business_message())
        )
        assert consumed is True
        adapter.handle_message.assert_not_called()
        assert len(store.appends) == 1
        _sid, entry = store.appends[0]
        assert entry["observed"] is True
        assert entry["telegram_business"] is True
        assert entry["business_connection_id"] == "bc-1"
        assert "hello from a managed DM" in entry["content"]

    @pytest.mark.asyncio
    async def test_synthetic_chat_id_namespaced_by_connection(self, adapter, store):
        await adapter._handle_business_connection(_update_connection(_make_connection()), None)
        await adapter._consume_business_message_update(_update_business(_make_business_message()))
        source = store.sessions_for[0]
        assert source.chat_id == "business:bc-1:900900"
        assert source.chat_type == "dm"
        assert source.user_id is None

    @pytest.mark.asyncio
    async def test_edited_business_message_observed(self, adapter, store):
        await adapter._handle_business_connection(_update_connection(_make_connection()), None)
        await adapter._consume_business_message_update(
            _update_business(_make_business_message(text="edited text"), edited=True)
        )
        assert len(store.appends) == 1
        _sid, entry = store.appends[0]
        assert "edited Telegram Business message" in entry["content"]

    @pytest.mark.asyncio
    async def test_ignored_when_secretary_disabled(self, store):
        adapter = _make_adapter(store, extra={"business_secretary_enabled": False})
        consumed = await adapter._consume_business_message_update(
            _update_business(_make_business_message())
        )
        # Still consumed so it can't leak into the normal DM auth/dispatch path,
        # but nothing is observed and the agent is not invoked.
        assert consumed is True
        adapter.handle_message.assert_not_called()
        assert store.appends == []

    @pytest.mark.asyncio
    async def test_not_observed_when_observe_disabled(self, store):
        adapter = _make_adapter(
            store,
            extra={"business_secretary_enabled": True, "business_observe_messages": False},
        )
        await adapter._handle_business_connection(_update_connection(_make_connection()), None)
        consumed = await adapter._consume_business_message_update(
            _update_business(_make_business_message())
        )
        assert consumed is True
        assert store.appends == []

    @pytest.mark.asyncio
    async def test_message_from_unknown_unfetchable_connection_dropped(self, adapter, store):
        # No connection pre-registered and no bot to fetch it -> not observed.
        await adapter._consume_business_message_update(_update_business(_make_business_message()))
        assert store.appends == []

    @pytest.mark.asyncio
    async def test_auto_reply_flag_still_only_observes(self, store):
        adapter = _make_adapter(
            store,
            extra={"business_secretary_enabled": True, "business_auto_reply": True},
        )
        await adapter._handle_business_connection(_update_connection(_make_connection()), None)
        await adapter._consume_business_message_update(_update_business(_make_business_message()))
        adapter.handle_message.assert_not_called()
        assert len(store.appends) == 1


# ---------------------------------------------------------------------------
# Media ingestion: Business-DM photos/voice are downloaded, cached, analyzable
# ---------------------------------------------------------------------------

def _fake_file(file_path, payload=b"BYTES"):
    return SimpleNamespace(
        file_path=file_path,
        download_as_bytearray=AsyncMock(return_value=bytearray(payload)),
    )


def _patch_media_cache(monkeypatch, *, path, media_type, kind):
    cached = SimpleNamespace(
        path=path,
        media_type=media_type,
        kind=kind,
        context_note=lambda: f"[cached {kind} at {path}]",
    )
    import gateway.platforms.base as base_mod
    monkeypatch.setattr(base_mod, "cache_media_bytes", lambda *a, **k: cached)
    return cached


class TestBusinessMediaIngestion:
    """Business-DM photos/voice must be downloaded + cached so they can be
    analyzed later (vision/STT), not stored as a dead text placeholder."""

    @pytest.mark.asyncio
    async def test_business_voice_is_cached_and_referenced(self, adapter, store, monkeypatch):
        voice = SimpleNamespace(file_size=2048, get_file=AsyncMock(return_value=_fake_file("v/note.ogg")))
        msg = _make_business_message(text=None, caption=None)
        msg.voice = voice
        _patch_media_cache(monkeypatch, path="/cache/note.ogg", media_type="audio/ogg", kind="audio")

        await adapter._handle_business_connection(_update_connection(_make_connection()), None)
        await adapter._consume_business_message_update(_update_business(msg))

        assert len(store.appends) == 1
        _sid, entry = store.appends[0]
        assert "[Telegram Business voice message]" not in entry["content"]
        assert "/cache/note.ogg" in entry["content"]
        assert entry.get("media_urls") == ["/cache/note.ogg"]
        assert entry.get("media_types") == ["audio/ogg"]
        voice.get_file.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_business_photo_is_cached_and_referenced(self, adapter, store, monkeypatch):
        # PTB delivers photos as a list of PhotoSize; the largest is taken.
        photo_size = SimpleNamespace(file_size=4096, get_file=AsyncMock(return_value=_fake_file("p/img.jpg")))
        msg = _make_business_message(text=None, caption=None)
        msg.photo = [photo_size]
        _patch_media_cache(monkeypatch, path="/cache/img.jpg", media_type="image/jpeg", kind="image")

        await adapter._handle_business_connection(_update_connection(_make_connection()), None)
        await adapter._consume_business_message_update(_update_business(msg))

        _sid, entry = store.appends[0]
        assert "/cache/img.jpg" in entry["content"]
        assert entry.get("media_urls") == ["/cache/img.jpg"]
        assert entry.get("media_types") == ["image/jpeg"]

    @pytest.mark.asyncio
    async def test_business_caption_preserved_with_media(self, adapter, store, monkeypatch):
        photo_size = SimpleNamespace(file_size=4096, get_file=AsyncMock(return_value=_fake_file("p/img.jpg")))
        msg = _make_business_message(text=None, caption="look at this")
        msg.photo = [photo_size]
        _patch_media_cache(monkeypatch, path="/cache/img.jpg", media_type="image/jpeg", kind="image")

        await adapter._handle_business_connection(_update_connection(_make_connection()), None)
        await adapter._consume_business_message_update(_update_business(msg))

        _sid, entry = store.appends[0]
        assert "look at this" in entry["content"]
        assert "/cache/img.jpg" in entry["content"]

    @pytest.mark.asyncio
    async def test_oversized_business_media_noted_not_downloaded(self, adapter, store, monkeypatch):
        big = SimpleNamespace(file_size=999_000_000, get_file=AsyncMock())
        msg = _make_business_message(text=None, caption=None)
        msg.voice = big

        await adapter._handle_business_connection(_update_connection(_make_connection()), None)
        await adapter._consume_business_message_update(_update_business(msg))

        _sid, entry = store.appends[0]
        big.get_file.assert_not_awaited()
        assert entry.get("media_urls") in (None, [])
        assert "too large" in entry["content"].lower()


# ---------------------------------------------------------------------------
# Local chat allow/block narrowing
# ---------------------------------------------------------------------------

class TestBusinessChatFilters:
    @pytest.mark.asyncio
    async def test_blocked_chat_is_skipped(self, store):
        adapter = _make_adapter(
            store,
            extra={"business_secretary_enabled": True, "business_blocked_chats": "900900"},
        )
        await adapter._handle_business_connection(_update_connection(_make_connection()), None)
        await adapter._consume_business_message_update(_update_business(_make_business_message()))
        assert store.appends == []

    @pytest.mark.asyncio
    async def test_allowed_chats_narrows_to_listed_only(self, store):
        adapter = _make_adapter(
            store,
            extra={"business_secretary_enabled": True, "business_allowed_chats": "111,222"},
        )
        await adapter._handle_business_connection(_update_connection(_make_connection()), None)
        # chat 900900 not in the allowlist -> skipped
        await adapter._consume_business_message_update(
            _update_business(_make_business_message(chat_id="900900"))
        )
        assert store.appends == []
        # a listed chat is observed
        await adapter._consume_business_message_update(
            _update_business(_make_business_message(chat_id="222"))
        )
        assert len(store.appends) == 1


# ---------------------------------------------------------------------------
# Deletions
# ---------------------------------------------------------------------------

class TestBusinessDeletions:
    @pytest.mark.asyncio
    async def test_deleted_messages_recorded_as_observed_note(self, adapter, store):
        await adapter._handle_business_connection(_update_connection(_make_connection()), None)
        await adapter._handle_business_messages_deleted(_update_deleted(message_ids=(10, 11)), None)
        assert len(store.appends) == 1
        _sid, entry = store.appends[0]
        assert entry["observed"] is True
        assert entry["telegram_business"] is True
        assert "deleted" in entry["content"]
        assert "10, 11" in entry["content"]

    @pytest.mark.asyncio
    async def test_deletions_ignored_when_secretary_disabled(self, store):
        adapter = _make_adapter(store, extra={"business_secretary_enabled": False})
        await adapter._handle_business_messages_deleted(_update_deleted(), None)
        assert store.appends == []


# ---------------------------------------------------------------------------
# Guard contract: normal updates must pass through untouched
# ---------------------------------------------------------------------------

class TestGuardContract:
    @pytest.mark.asyncio
    async def test_normal_update_not_consumed(self, adapter):
        # The regression that broke 21 document tests: a normal update must
        # return False so it continues to the real dispatch path.
        consumed = await adapter._consume_business_message_update(_update_normal_text())
        assert consumed is False

    @pytest.mark.asyncio
    async def test_business_update_consumed(self, adapter):
        consumed = await adapter._consume_business_message_update(
            _update_business(_make_business_message())
        )
        assert consumed is True


# ---------------------------------------------------------------------------
# Config readers
# ---------------------------------------------------------------------------

class TestBusinessConfigReaders:
    def test_secretary_enabled_defaults_true(self, store):
        adapter = _make_adapter(store, extra={})
        assert adapter._telegram_business_secretary_enabled() is True

    def test_secretary_enabled_accepts_bool_and_string(self, store):
        assert _make_adapter(store, extra={"business_secretary_enabled": True})._telegram_business_secretary_enabled() is True
        assert _make_adapter(store, extra={"business_secretary_enabled": "yes"})._telegram_business_secretary_enabled() is True
        assert _make_adapter(store, extra={"business_secretary_enabled": "off"})._telegram_business_secretary_enabled() is False

    def test_secretary_enabled_env_fallback(self, store, monkeypatch):
        monkeypatch.setenv("TELEGRAM_BUSINESS_SECRETARY_ENABLED", "1")
        assert _make_adapter(store, extra={})._telegram_business_secretary_enabled() is True

    def test_observe_messages_defaults_true(self, store):
        assert _make_adapter(store, extra={})._telegram_business_observe_messages() is True

    def test_auto_reply_defaults_false(self, store):
        assert _make_adapter(store, extra={})._telegram_business_auto_reply() is False

    def test_allowed_and_blocked_chats_parse_csv_and_list(self, store):
        a = _make_adapter(store, extra={"business_allowed_chats": "1, 2 ,3", "business_blocked_chats": ["9", "8"]})
        assert a._telegram_business_allowed_chats() == {"1", "2", "3"}
        assert a._telegram_business_blocked_chats() == {"9", "8"}

    def test_chat_allowed_logic(self, store):
        a = _make_adapter(store, extra={"business_allowed_chats": "111", "business_blocked_chats": "222"})
        assert a._telegram_business_chat_allowed("111") is True
        assert a._telegram_business_chat_allowed("222") is False  # blocked
        assert a._telegram_business_chat_allowed("333") is False  # not in non-empty allowlist
        # empty allowlist => accept anything not blocked
        b = _make_adapter(store, extra={"business_blocked_chats": "222"})
        assert b._telegram_business_chat_allowed("333") is True
        assert b._telegram_business_chat_allowed("222") is False
        assert b._telegram_business_chat_allowed("") is False
