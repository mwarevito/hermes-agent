"""Tests for Bot API 10.1 Rich Messages (sendRichMessage) on Telegram.

Final / new-message replies opportunistically use ``sendRichMessage`` with the
RAW agent markdown so tables, task lists, etc. render natively. The legacy
MarkdownV2 ``send_message`` path stays as the fallback for unsupported /
oversized content and for transports that lack the endpoint.

The ``telegram`` package is mocked by ``tests/gateway/conftest.py``
(:func:`_ensure_telegram_mock`), so these tests construct a real
``TelegramAdapter`` and wire a mock bot.
"""

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.base import SendResult
from plugins.platforms.telegram.adapter import TelegramAdapter
from telegram.error import BadRequest, NetworkError, TimedOut


# Content exercising rich-only constructs: a heading, a real Markdown table,
# and a task list. Pipes / brackets must survive untouched into the payload.
RICH_CONTENT = "## Results\n\n| Case | Status |\n|---|---|\n| rich | ✅ |\n\n- [x] table renders"
CJK_RICH_CONTENT = "## 持仓\n\n| 项目 | 状态 |\n|---|---|\n| 早盘 | 正常 |"
ASTRAL_CJK_RICH_CONTENT = "## Rare Han\n\n| glyph | status |\n|---|---|\n| \U00030000 | ok |"
TABLE_ONLY_CONTENT = (
    "| Team | W | L | GB |\n"
    "|---|---|---|---|\n"
    "| Red Sox | 36 | 34 | 6.0 |\n"
    "| Dodgers | 40 | 30 | 2.0 |"
)
DANGEROUS_DETAILS_MATH = (
    "<details><summary>Complex proof</summary>\n\n"
    "$$\\sum_{i=1}^{n} i = \\frac{n(n+1)}{2}$$\n\n"
    "And inline \\(\\alpha + \\beta\\)\n"
    "</details>"
)

# PTB 22.6's real unknown-endpoint errors: do_api_request can raise
# EndPointNotFound for Bot API 404s, and the request layer can wrap that same
# missing endpoint as InvalidToken. Use class names here so the tests don't
# depend on optional PTB internals.
EndPointNotFound = type("EndPointNotFound", (Exception,), {})
InvalidToken = type("InvalidToken", (Exception,), {})
PTB_ENDPOINT_NOT_FOUND = EndPointNotFound(
    "Endpoint 'sendRichMessage' not found in Bot API"
)
PTB_INVALID_TOKEN_404 = InvalidToken(
    "Either the bot token was rejected by Telegram or the endpoint "
    "'sendRichMessage' does not exist."
)


def _make_adapter(extra=None):
    """Build a TelegramAdapter with a mock bot wired for the rich path."""
    config = PlatformConfig(
        enabled=True,
        token="fake-token",
        extra={"rich_messages": True, **(extra or {})},
    )
    adapter = TelegramAdapter(config)
    bot = MagicMock()
    # do_api_request as an AsyncMock makes inspect.iscoroutinefunction(...) True,
    # so _bot_supports_rich() is satisfied (real Bot.do_api_request is async too).
    bot.do_api_request = AsyncMock(return_value=SimpleNamespace(message_id=123))
    bot.send_message = AsyncMock(return_value=MagicMock(message_id=1))
    bot.send_chat_action = AsyncMock()  # keeps the post-send typing re-trigger quiet
    bot.send_message_draft = AsyncMock(return_value=True)  # legacy draft fallback
    bot.edit_message_text = AsyncMock(return_value=MagicMock(message_id=1))  # legacy edit path
    bot.delete_message = AsyncMock(return_value=True)
    adapter._bot = bot
    return adapter


def _rich_api_kwargs(adapter):
    """Return the api_kwargs dict from the single sendRichMessage call."""
    call = adapter._bot.do_api_request.call_args
    assert call.args[0] == "sendRichMessage"
    return call.kwargs["api_kwargs"]


@pytest.mark.asyncio
async def test_details_without_math_still_uses_rich_send():
    adapter = _make_adapter()

    result = await adapter.send(
        "12345",
        "<details><summary>Notes</summary>\nNo equations here.\n</details>",
    )

    assert result.success is True
    bot = adapter._bot
    assert bot is not None
    bot.do_api_request.assert_awaited_once()
    bot.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_math_outside_details_still_uses_rich_send():
    adapter = _make_adapter()

    result = await adapter.send("12345", "Outside details: $$x^2 + y^2$$")

    assert result.success is True
    bot = adapter._bot
    assert bot is not None
    bot.do_api_request.assert_awaited_once()
    bot.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_astral_cjk_rich_content_skips_rich_send_to_avoid_tdesktop_garble():
    adapter = _make_adapter()

    result = await adapter.send("12345", ASTRAL_CJK_RICH_CONTENT)

    assert result.success is True
    adapter._bot.do_api_request.assert_not_called()
    adapter._bot.send_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_plain_markdown_stays_on_legacy_path():
    """Ordinary replies (no table/task-list/details/math) stay on the legacy
    MarkdownV2 path for consistent client rendering, even with rich enabled."""
    adapter = _make_adapter()

    result = await adapter.send("12345", "Hello **there**\n\nA normal reply.")

    assert result.success is True
    bot = adapter._bot
    assert bot is not None
    bot.do_api_request.assert_not_called()
    bot.send_message.assert_awaited()


@pytest.mark.asyncio
async def test_expect_edits_metadata_keeps_preview_on_legacy_path():
    adapter = _make_adapter()

    result = await adapter.send(
        "12345",
        RICH_CONTENT,
        metadata={"expect_edits": True},
    )

    assert result.success is True
    # Streaming preview sends will be edited later, so they must not be born as
    # rich messages until Hermes wires rich_message edits directly.
    bot = adapter._bot
    assert bot is not None
    bot.do_api_request.assert_not_called()
    bot.send_message.assert_awaited()


@pytest.mark.asyncio
async def test_oversized_content_skips_rich_and_chunks():
    adapter = _make_adapter()
    # > 32,768 characters -> rich pre-check fails, legacy chunking takes over.
    oversized = "a" * 40000
    assert len(oversized) > TelegramAdapter.RICH_MESSAGE_MAX_CHARS

    result = await adapter.send("12345", oversized)

    assert result.success is True
    adapter._bot.do_api_request.assert_not_called()
    # Oversized content is split into multiple legacy chunks.
    assert adapter._bot.send_message.await_count > 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exc",
    [
        BadRequest("can't parse rich message"),
        BadRequest("Method not found"),
    ],
)
async def test_permanent_rich_error_falls_back_to_legacy(exc):
    adapter = _make_adapter()
    adapter._bot.do_api_request = AsyncMock(side_effect=exc)

    result = await adapter.send("12345", RICH_CONTENT)

    assert result.success is True
    adapter._bot.do_api_request.assert_awaited_once()
    adapter._bot.send_message.assert_awaited()  # legacy fallback ran


@pytest.mark.asyncio
async def test_unknown_endpoint_error_falls_back_to_legacy():
    """A non-BadRequest 'Method not found' (old PTB/endpoint) degrades gracefully."""
    adapter = _make_adapter()
    adapter._bot.do_api_request = AsyncMock(side_effect=RuntimeError("Method not found"))

    result = await adapter.send("12345", RICH_CONTENT)

    assert result.success is True
    adapter._bot.send_message.assert_awaited()


@pytest.mark.asyncio
async def test_capability_error_latches_rich_send_off():
    """Endpoint-missing errors latch rich off so later sends skip the
    doomed extra roundtrip entirely."""
    adapter = _make_adapter()
    adapter._bot.do_api_request = AsyncMock(side_effect=RuntimeError("Method not found"))

    result = await adapter.send("12345", RICH_CONTENT)
    assert result.success is True
    assert adapter._rich_send_disabled is True

    # Second send skips rich entirely (no second do_api_request call).
    adapter._bot.do_api_request.reset_mock()
    adapter._bot.send_message.reset_mock()
    result2 = await adapter.send("12345", RICH_CONTENT)
    assert result2.success is True
    adapter._bot.do_api_request.assert_not_called()
    adapter._bot.send_message.assert_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("exc", [PTB_ENDPOINT_NOT_FOUND, PTB_INVALID_TOKEN_404])
async def test_real_ptb_endpoint_missing_falls_back_and_latches_off(exc):
    adapter = _make_adapter()
    adapter._bot.do_api_request = AsyncMock(side_effect=exc)

    result = await adapter.send("12345", RICH_CONTENT)

    assert result.success is True
    bot = adapter._bot
    assert bot is not None
    bot.do_api_request.assert_awaited_once()
    bot.send_message.assert_awaited()
    assert adapter._rich_send_disabled is True


@pytest.mark.asyncio
async def test_per_message_bad_request_does_not_latch_off():
    """A parser/limit BadRequest is per-message — rich must stay enabled
    for subsequent messages."""
    adapter = _make_adapter()
    adapter._bot.do_api_request = AsyncMock(side_effect=BadRequest("can't parse rich message"))

    result = await adapter.send("12345", RICH_CONTENT)
    assert result.success is True
    assert adapter._rich_send_disabled is False

    # Next message re-attempts rich.
    adapter._bot.do_api_request = AsyncMock(return_value=SimpleNamespace(message_id=124))
    result2 = await adapter.send("12345", RICH_CONTENT)
    assert result2.success is True
    adapter._bot.do_api_request.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("exc", [TimedOut("timed out"), NetworkError("connection reset")])
async def test_transient_rich_error_does_not_legacy_resend(exc):
    """Transient transport errors must NOT trigger a legacy resend (duplicate risk)."""
    adapter = _make_adapter()
    adapter._bot.do_api_request = AsyncMock(side_effect=exc)

    result = await adapter.send("12345", RICH_CONTENT)

    assert result.success is False
    adapter._bot.do_api_request.assert_awaited_once()
    adapter._bot.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_rich_transport_error_redacts_bot_token_even_when_redaction_disabled(monkeypatch):
    import agent.redact as redact

    monkeypatch.setattr(redact, "_REDACT_ENABLED", False)
    token = "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"
    adapter = _make_adapter()
    adapter._bot.do_api_request = AsyncMock(
        side_effect=NetworkError(
            f"Timed out requesting https://api.telegram.org/bot{token}/sendRichMessage"
        )
    )

    result = await adapter.send("12345", RICH_CONTENT)

    assert result.success is False
    assert result.error is not None
    assert token not in result.error
    assert "bot123456789:***/sendRichMessage" in result.error
    adapter._bot.send_message.assert_not_called()


@pytest.mark.asyncio
async def test_legacy_send_error_redacts_bot_token_without_traceback(monkeypatch, caplog):
    import agent.redact as redact

    monkeypatch.setattr(redact, "_REDACT_ENABLED", False)
    token = "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"
    adapter = _make_adapter({"rich_messages": False})
    adapter._bot.send_message = AsyncMock(
        side_effect=BadRequest(
            f"Bad Request: https://api.telegram.org/bot{token}/sendMessage"
        )
    )

    with caplog.at_level(logging.ERROR):
        result = await adapter.send("12345", "Plain legacy content.")

    assert result.success is False
    assert result.error is not None
    assert token not in result.error
    assert "bot123456789:***/sendMessage" in result.error
    assert token not in caplog.text
    assert "bot123456789:***/sendMessage" in caplog.text
    adapter._bot.do_api_request.assert_not_called()


@pytest.mark.asyncio
async def test_routing_direct_messages_topic_id_drops_message_thread_id():
    adapter = _make_adapter()

    await adapter.send("-100123", RICH_CONTENT, metadata={"direct_messages_topic_id": "20189"})

    api_kwargs = _rich_api_kwargs(adapter)
    assert api_kwargs["direct_messages_topic_id"] == 20189
    # _thread_kwargs_for_send pairs the topic id with message_thread_id=None;
    # the rich payload must drop the None key, not send a stray field.
    assert "message_thread_id" not in api_kwargs


@pytest.mark.asyncio
async def test_notification_silent_by_default():
    adapter = _make_adapter()

    await adapter.send("-100123", RICH_CONTENT)

    api_kwargs = _rich_api_kwargs(adapter)
    assert api_kwargs["disable_notification"] is True


@pytest.mark.asyncio
async def test_table_only_uses_legacy_with_default_config():
    """Default config (rich_messages unset → False) keeps tables on legacy path."""
    config = PlatformConfig(enabled=True, token="fake-token")
    adapter = TelegramAdapter(config)
    bot = MagicMock()
    bot.do_api_request = AsyncMock(return_value=SimpleNamespace(message_id=123))
    bot.send_message = AsyncMock(return_value=MagicMock(message_id=1))
    bot.send_chat_action = AsyncMock()
    adapter._bot = bot

    result = await adapter.send("12345", TABLE_ONLY_CONTENT)

    assert result.success is True
    bot.do_api_request.assert_not_called()
    bot.send_message.assert_awaited()


# ── Streaming drafts: sendRichMessageDraft ─────────────────────────────


@pytest.mark.asyncio
async def test_cjk_rich_content_skips_rich_draft_to_avoid_tdesktop_garble():
    adapter = _make_adapter(extra={"rich_drafts": True})
    adapter._bot.do_api_request = AsyncMock(return_value=True)

    result = await adapter.send_draft("12345", draft_id=7, content=CJK_RICH_CONTENT)

    assert result.success is True
    adapter._bot.do_api_request.assert_not_called()
    adapter._bot.send_message_draft.assert_awaited_once()


# ----------------------------------------------------------------------
# prefers_fresh_final_streaming: Telegram keeps streamed finals on the edit
# path, even when rich messages are enabled, so users do not briefly see two
# copies of the answer while the preview cleanup delete races the fresh send.
# ----------------------------------------------------------------------
def test_prefers_fresh_final_streaming_stays_disabled_when_rich_enabled():
    adapter = _make_adapter()
    assert adapter.prefers_fresh_final_streaming(RICH_CONTENT) is False


# ----------------------------------------------------------------------
# streaming_overflow_limit: with rich on, the stream consumer may accumulate up
# to the 32,768-char rich cap before splitting, so a reply that fits one
# sendRichMessage / sendRichMessageDraft isn't fragmented at the 4,096 limit.
# ----------------------------------------------------------------------


def test_streaming_overflow_limit_none_when_rich_latched_off():
    adapter = _make_adapter()
    adapter._rich_send_disabled = True
    assert adapter.streaming_overflow_limit() is None


# ----------------------------------------------------------------------------
# Rich finalize via editMessageText (Bot API 10.1 rich_message edit param).
# Streamed previews finalize by editing the existing message IN PLACE as rich,
# so tables/task lists survive without a fresh send + delete (no duplicate).
# ----------------------------------------------------------------------------


def _rich_edit_kwargs(adapter):
    """Return the api_kwargs dict from the single editMessageText rich call."""
    call = adapter._bot.do_api_request.call_args
    assert call.args[0] == "editMessageText"
    return call.kwargs["api_kwargs"]


@pytest.mark.asyncio
async def test_finalize_edit_uses_rich_for_table_content():
    """Finalizing a streamed preview whose content is a table edits the
    existing message IN PLACE via editMessageText's rich_message param —
    no fresh send, no delete, no duplicate."""
    adapter = _make_adapter()

    result = await adapter.edit_message(
        "12345", "555", RICH_CONTENT, finalize=True,
    )

    assert result.success is True
    assert result.message_id == "555"  # same message, edited in place
    api_kwargs = _rich_edit_kwargs(adapter)
    assert api_kwargs["message_id"] == 555
    # RAW markdown is passed through so table pipes survive.
    assert api_kwargs["rich_message"]["markdown"] == RICH_CONTENT
    # No fresh send / delete — the whole point of the in-place rich edit.
    adapter._bot.edit_message_text.assert_not_called()
    adapter._bot.delete_message.assert_not_called()


@pytest.mark.asyncio
async def test_legacy_edit_error_logs_redacted_bot_token_without_traceback(monkeypatch, caplog):
    import agent.redact as redact

    monkeypatch.setattr(redact, "_REDACT_ENABLED", False)
    token = "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"
    adapter = _make_adapter()
    adapter._bot.edit_message_text = AsyncMock(
        side_effect=BadRequest(
            f"Bad Request: https://api.telegram.org/bot{token}/editMessageText"
        )
    )

    with caplog.at_level(logging.WARNING):
        result = await adapter.edit_message(
            "12345", "555", "Just a normal answer.", finalize=True,
        )

    assert result.success is False
    assert result.error is not None
    assert token not in result.error
    assert "bot123456789:***/editMessageText" in result.error
    assert token not in caplog.text
    assert "bot123456789:***/editMessageText" in caplog.text


# --------------------------------------------------------------------------
# Rich-reply recovery (#47375): Telegram does not echo a sendRichMessage's
# content in reply_to_message (.text/.caption empty, .api_kwargs None), so we
# record message_id -> text at send time and recover it on inbound reply.
# --------------------------------------------------------------------------


def _reply_message(reply_to_id, *, reply_text=None, reply_caption=None, quote_text=None):
    """Build a mock inbound reply Message for _build_message_event."""
    replied = SimpleNamespace(
        message_id=int(reply_to_id),
        text=reply_text,
        caption=reply_caption,
    )
    quote = SimpleNamespace(text=quote_text) if quote_text is not None else None
    return SimpleNamespace(
        message_id=999,
        chat=SimpleNamespace(id=12345, type="private", title=None, full_name="U"),
        from_user=SimpleNamespace(
            id=42, username="u", first_name="U", last_name=None,
            full_name="U", is_bot=False,
        ),
        text="what did this mean?",
        caption=None,
        reply_to_message=replied,
        quote=quote,
        message_thread_id=None,
        is_topic_message=False,
        entities=[],
        date=None,
    )


def _reply_message_with_rich_blocks(
    reply_to_id,
    *,
    blocks,
    quote_text=None,
    api_kwargs_factory=dict,
):
    """Build a reply whose echoed content lives only in api_kwargs.rich_message."""
    replied = SimpleNamespace(
        message_id=int(reply_to_id),
        text=None,
        caption=None,
        api_kwargs=api_kwargs_factory({"rich_message": {"blocks": blocks}}),
    )
    quote = SimpleNamespace(text=quote_text) if quote_text is not None else None
    return SimpleNamespace(
        message_id=999,
        chat=SimpleNamespace(id=12345, type="private", title=None, full_name="U"),
        from_user=SimpleNamespace(
            id=42, username="u", first_name="U", last_name=None,
            full_name="U", is_bot=False,
        ),
        text="what did this mean?",
        caption=None,
        reply_to_message=replied,
        quote=quote,
        message_thread_id=None,
        is_topic_message=False,
        entities=[],
        date=None,
    )


@pytest.mark.asyncio
async def test_rich_reply_records_and_recovers_text(monkeypatch, tmp_path):
    """A reply to a rich-sent message resolves the original text via the index."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    from gateway.platforms.base import MessageType
    from gateway import rich_sent_store

    adapter = _make_adapter()

    # _try_send_rich records (chat_id, message_id) -> content on a successful
    # rich send. Drive that path directly so the test doesn't depend on send()
    # gating heuristics (length, content shape) choosing the rich path.
    adapter._bot.do_api_request = AsyncMock(
        return_value=SimpleNamespace(message_id=678)
    )
    send_result = await adapter._try_send_rich(
        "12345", "Your morning briefing: CI is green.", None, None,
    )
    assert send_result is not None and send_result.success is True
    assert send_result.message_id == "678"
    assert rich_sent_store.lookup("12345", "678") == "Your morning briefing: CI is green."

    # Inbound reply carries NO text/caption (the rich-message blind spot).
    event = adapter._build_message_event(
        _reply_message("678"), MessageType.TEXT,
    )
    assert event.reply_to_message_id == "678"
    assert event.reply_to_text == "Your morning briefing: CI is green."


@pytest.mark.asyncio
async def test_rich_reply_lookup_miss_leaves_text_none(monkeypatch, tmp_path):
    """No recorded entry -> reply_to_text stays None, no crash."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    from gateway.platforms.base import MessageType

    adapter = _make_adapter()
    event = adapter._build_message_event(
        _reply_message("404"), MessageType.TEXT,
    )
    assert event.reply_to_message_id == "404"
    assert event.reply_to_text is None


@pytest.mark.asyncio
async def test_rich_reply_native_quote_wins_over_lookup(monkeypatch, tmp_path):
    """A native partial quote takes precedence over the send-time index."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    from gateway.platforms.base import MessageType
    from gateway import rich_sent_store

    rich_sent_store.record("12345", "678", "full recorded body")
    adapter = _make_adapter()
    event = adapter._build_message_event(
        _reply_message("678", quote_text="just this part"), MessageType.TEXT,
    )
    assert event.reply_to_text == "just this part"


@pytest.mark.asyncio
async def test_rich_reply_caption_wins_over_lookup(monkeypatch, tmp_path):
    """When Telegram DOES echo a caption, it wins over the index fallback."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    from gateway.platforms.base import MessageType
    from gateway import rich_sent_store

    rich_sent_store.record("12345", "678", "recorded body")
    adapter = _make_adapter()
    event = adapter._build_message_event(
        _reply_message("678", reply_caption="echoed caption"), MessageType.TEXT,
    )
    assert event.reply_to_text == "echoed caption"


# --------------------------------------------------------------------------
# Channel-origin rich-reply recovery: a rich message posted to a channel and
# forwarded into another chat is recorded under the ORIGIN's
# (chat_id, message_id). A reply to the forwarded copy has a different local
# message id in a different chat, so recovery must resolve via the
# authoritative channel forward origin. Only MessageOriginChannel is
# authoritative — user/hidden/chat/malformed origins never resolve cross-chat.
# --------------------------------------------------------------------------


def _forwarded_reply_message(
    *,
    current_chat_id,
    local_reply_id,
    forward_origin=None,
    to_dict=None,
    api_kwargs=None,
    forward_from_chat=None,
    forward_from_message_id=None,
    reply_text=None,
    reply_caption=None,
    quote_text=None,
):
    """Inbound reply, in ``current_chat_id``, to a forwarded message."""
    replied = SimpleNamespace(
        message_id=int(local_reply_id),
        text=reply_text,
        caption=reply_caption,
        forward_origin=forward_origin,
        forward_from_chat=forward_from_chat,
        forward_from_message_id=forward_from_message_id,
        api_kwargs=api_kwargs,
    )
    if to_dict is not None:
        replied.to_dict = lambda: to_dict
    quote = SimpleNamespace(text=quote_text) if quote_text is not None else None
    return SimpleNamespace(
        message_id=999,
        chat=SimpleNamespace(
            id=current_chat_id, type="supergroup", title="Feed group", full_name=None
        ),
        from_user=SimpleNamespace(
            id=42, username="u", first_name="U", last_name=None,
            full_name="U", is_bot=False,
        ),
        text="what did this mean?",
        caption=None,
        reply_to_message=replied,
        quote=quote,
        message_thread_id=None,
        is_topic_message=False,
        entities=[],
        date=None,
    )


def _channel_origin(chat_id, message_id):
    return SimpleNamespace(
        type="channel",
        chat=SimpleNamespace(id=chat_id),
        message_id=message_id,
    )


@pytest.mark.asyncio
async def test_forwarded_channel_rich_reply_recovers_from_origin(monkeypatch, tmp_path):
    """Reply to a forwarded channel post recovers via (origin chat, origin id)."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    from gateway.platforms.base import MessageType
    from gateway import rich_sent_store

    # Rich message was posted to channel -100123 as message 678.
    rich_sent_store.record("-100123", "678", "channel briefing body")
    adapter = _make_adapter()

    # The forwarded copy lives in the discussion group -100999 as a DIFFERENT
    # local message id (555); same-chat lookup on (-100999, 555) misses.
    event = adapter._build_message_event(
        _forwarded_reply_message(
            current_chat_id=-100999,
            local_reply_id=555,
            forward_origin=_channel_origin(-100123, 678),
        ),
        MessageType.TEXT,
    )
    assert event.reply_to_message_id == "555"
    assert event.reply_to_text == "channel briefing body"


@pytest.mark.asyncio
async def test_forwarded_channel_rich_reply_recovers_from_raw_shape(monkeypatch, tmp_path):
    """Recovery also works from the raw serialized forward_origin dict."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    from gateway.platforms.base import MessageType
    from gateway import rich_sent_store

    rich_sent_store.record("-100123", "678", "channel briefing body")
    adapter = _make_adapter()

    # No forward_origin object; only the raw to_dict() serialized shape.
    event = adapter._build_message_event(
        _forwarded_reply_message(
            current_chat_id=-100999,
            local_reply_id=555,
            forward_origin=None,
            to_dict={
                "message_id": 555,
                "forward_origin": {
                    "type": "channel",
                    "chat": {"id": -100123},
                    "message_id": 678,
                },
            },
        ),
        MessageType.TEXT,
    )
    assert event.reply_to_text == "channel briefing body"


@pytest.mark.asyncio
async def test_forwarded_channel_rich_reply_recovers_from_legacy_fields(monkeypatch, tmp_path):
    """Legacy forward_from_chat + forward_from_message_id also recover."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    from gateway.platforms.base import MessageType
    from gateway import rich_sent_store

    rich_sent_store.record("-100123", "678", "channel briefing body")
    adapter = _make_adapter()

    event = adapter._build_message_event(
        _forwarded_reply_message(
            current_chat_id=-100999,
            local_reply_id=555,
            forward_origin=None,
            forward_from_chat=SimpleNamespace(id=-100123, type="channel"),
            forward_from_message_id=678,
        ),
        MessageType.TEXT,
    )
    assert event.reply_to_text == "channel briefing body"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "origin",
    [
        # MessageOriginUser — no message id, not authoritative.
        SimpleNamespace(type="user", sender_user=SimpleNamespace(id=-100123)),
        # MessageOriginHiddenUser — sender name only.
        SimpleNamespace(type="hidden_user", sender_user_name="X"),
        # MessageOriginChat — sender_chat but NO message id.
        SimpleNamespace(type="chat", sender_chat=SimpleNamespace(id=-100123)),
        # Malformed channel origin — missing message id.
        SimpleNamespace(type="channel", chat=SimpleNamespace(id=-100123), message_id=None),
        # Malformed channel origin — missing chat.
        SimpleNamespace(type="channel", chat=None, message_id=678),
    ],
)
async def test_non_channel_or_malformed_origin_never_recovers(monkeypatch, tmp_path, origin):
    """User/hidden/chat/malformed origins never resolve text cross-chat."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    from gateway.platforms.base import MessageType
    from gateway import rich_sent_store

    # Text recorded under -100123:678 must NOT leak into the -100999 group via
    # a non-authoritative origin (privacy / message-id collision boundary).
    rich_sent_store.record("-100123", "678", "private channel body")
    adapter = _make_adapter()

    event = adapter._build_message_event(
        _forwarded_reply_message(
            current_chat_id=-100999,
            local_reply_id=678,  # collision: same numeric id, different chat
            forward_origin=origin,
        ),
        MessageType.TEXT,
    )
    assert event.reply_to_text is None


@pytest.mark.asyncio
async def test_forwarded_channel_native_quote_wins_over_origin(monkeypatch, tmp_path):
    """A native partial quote still beats channel-origin recovery."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    from gateway.platforms.base import MessageType
    from gateway import rich_sent_store

    rich_sent_store.record("-100123", "678", "full channel body")
    adapter = _make_adapter()

    event = adapter._build_message_event(
        _forwarded_reply_message(
            current_chat_id=-100999,
            local_reply_id=555,
            forward_origin=_channel_origin(-100123, 678),
            quote_text="just this slice",
        ),
        MessageType.TEXT,
    )
    assert event.reply_to_text == "just this slice"


# --------------------------------------------------------------------------
# Cron-feed send bypass: scheduled cron deliveries carry cron_delivery=True so
# the adapter uses legacy sendMessage (real text survives forwarding/reply)
# rather than a rich message. Ordinary sends still go rich.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cron_delivery_metadata_bypasses_rich_send():
    adapter = _make_adapter()

    result = await adapter.send(
        "12345", RICH_CONTENT, metadata={"cron_delivery": True},
    )

    assert result.success is True
    # Legacy path only — rich endpoint untouched.
    adapter._bot.do_api_request.assert_not_called()
    adapter._bot.send_message.assert_awaited()


@pytest.mark.asyncio
async def test_ordinary_send_still_uses_rich():
    adapter = _make_adapter()

    result = await adapter.send("12345", RICH_CONTENT, metadata={})

    assert result.success is True
    adapter._bot.do_api_request.assert_awaited_once()
    adapter._bot.send_message.assert_not_called()


# --------------------------------------------------------------------------
# Explicit privacy-boundary proofs for channel-origin recovery. The positive
# tests above prove the path fires when it should; these prove it does NOT
# leak across the store's trust boundaries: per-account (HERMES_HOME) store
# isolation, cache miss / eviction / restart, and malformed / non-authoritative
# RAW (api_kwargs / to_dict) origins.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_channel_origin_across_separate_hermes_home_stores_cannot_cross(
    monkeypatch, tmp_path
):
    """A (chat_id, message_id) recorded under one HERMES_HOME never resolves
    under a different HERMES_HOME (separate account/profile store)."""
    from gateway.platforms.base import MessageType
    from gateway import rich_sent_store

    home_a = tmp_path / "account_a"
    home_b = tmp_path / "account_b"

    # Account A recorded the rich channel post.
    monkeypatch.setenv("HERMES_HOME", str(home_a))
    rich_sent_store.record("-100123", "678", "account A private body")

    # Account B (different store) must not see it — same channel+message tuple.
    monkeypatch.setenv("HERMES_HOME", str(home_b))
    adapter = _make_adapter()
    event = adapter._build_message_event(
        _forwarded_reply_message(
            current_chat_id=-100999,
            local_reply_id=555,
            forward_origin=_channel_origin(-100123, 678),
        ),
        MessageType.TEXT,
    )
    assert event.reply_to_text is None

    # Sanity: under account A's store the SAME reply DOES resolve — proves the
    # None above is store isolation, not a broken origin/helper (non-vacuous).
    monkeypatch.setenv("HERMES_HOME", str(home_a))
    event_a = adapter._build_message_event(
        _forwarded_reply_message(
            current_chat_id=-100999,
            local_reply_id=555,
            forward_origin=_channel_origin(-100123, 678),
        ),
        MessageType.TEXT,
    )
    assert event_a.reply_to_text == "account A private body"


@pytest.mark.asyncio
async def test_channel_origin_cache_miss_returns_none(monkeypatch, tmp_path):
    """A well-formed channel origin whose tuple is absent from the store
    (eviction / restart / never-recorded) resolves to None, not a leak."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    from gateway.platforms.base import MessageType
    from gateway import rich_sent_store

    # A DIFFERENT channel message is in the store; the replied-to tuple is not.
    rich_sent_store.record("-100123", "999", "some other channel body")
    adapter = _make_adapter()

    event = adapter._build_message_event(
        _forwarded_reply_message(
            current_chat_id=-100999,
            local_reply_id=555,
            forward_origin=_channel_origin(-100123, 678),  # not recorded
        ),
        MessageType.TEXT,
    )
    assert event.reply_to_text is None


@pytest.mark.asyncio
@pytest.mark.parametrize("carrier", ["api_kwargs", "to_dict"])
@pytest.mark.parametrize(
    "raw_origin",
    [
        # Malformed channel — missing message_id.
        {"type": "channel", "chat": {"id": -100123}},
        # Malformed channel — missing chat.
        {"type": "channel", "message_id": 678},
        # Malformed channel — chat present but no id.
        {"type": "channel", "chat": {}, "message_id": 678},
        # Non-authoritative raw user origin.
        {"type": "user", "sender_user": {"id": -100123}},
        # Non-authoritative raw hidden_user origin.
        {"type": "hidden_user", "sender_user_name": "X"},
        # Non-authoritative raw chat origin (no message id).
        {"type": "chat", "sender_chat": {"id": -100123}},
    ],
)
async def test_raw_malformed_or_nonauthoritative_origin_never_recovers(
    monkeypatch, tmp_path, carrier, raw_origin
):
    """Raw serialized (api_kwargs / to_dict) origins that are malformed channel
    or non-channel never trigger a cross-chat lookup, even on an id collision."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    from gateway.platforms.base import MessageType
    from gateway import rich_sent_store

    # Text under -100123:678 must not leak into -100999 via a raw origin.
    rich_sent_store.record("-100123", "678", "private channel body")
    adapter = _make_adapter()

    kwargs = dict(
        current_chat_id=-100999,
        local_reply_id=678,  # collision: same numeric id, different chat
        forward_origin=None,
    )
    if carrier == "api_kwargs":
        kwargs["api_kwargs"] = {"forward_origin": raw_origin}
    else:
        kwargs["to_dict"] = {"message_id": 678, "forward_origin": raw_origin}

    event = adapter._build_message_event(
        _forwarded_reply_message(**kwargs), MessageType.TEXT,
    )
    assert event.reply_to_text is None
