"""Reply context is recovered from raw Bot API fields — and not invented.

Two things are pinned here, and they pull against each other:

1. The recovery block itself (``_telegram_api_kwargs_reply_context`` /
   ``_telegram_observed_reply_text``). Without it a group message like
   ``@giviagentbot check pls`` sent as a reply reaches the agent as bare
   ``check pls``. It is a LOCAL patch that no test named until now, so the
   version scanner could not see it and an upgrade would have dropped it
   silently — the "Jul 1" class.

2. The phantom quote (2026-08-08). Telegram threads a forum topic by making
   every message a reply to the topic-creation message, so the recovery block
   announced ``[Reply #122 — content unavailable]`` for messages nobody replied
   to. Told there IS context it cannot see, the agent fills the hole with a
   guess. The topic root now yields no quote at all.
"""
import pytest

from plugins.platforms.telegram.adapter import TelegramAdapter


class _Adapter:
    """Bare instance: only the pure helpers under test are exercised."""

    name = "telegram"
    _session_store = None

    _telegram_reply_is_topic_root = staticmethod(
        TelegramAdapter._telegram_reply_is_topic_root
    )
    _telegram_api_kwargs_reply_context = (
        TelegramAdapter._telegram_api_kwargs_reply_context
    )
    _telegram_observed_reply_text = TelegramAdapter._telegram_observed_reply_text

    def __init__(self, raw, quote=None):
        self._raw = raw
        self._quote = quote

    def _telegram_raw_payload(self, message):
        return self._raw

    def _telegram_api_kwargs_quote_text(self, message):
        return self._quote

    @staticmethod
    def _telegram_payload_text(payload):
        if isinstance(payload, dict):
            text = payload.get("text")
            return text if isinstance(text, str) and text.strip() else None
        return None

    def _telegram_group_observe_shared_source(self, source):
        return source


def _ctx(raw, quote=None):
    a = _Adapter(raw, quote)
    return _Adapter._telegram_api_kwargs_reply_context(a, object(), object())


# --- 1. the recovery block still recovers ----------------------------------

def test_quote_text_wins_when_present():
    raw = {"reply_to_message": {"message_id": 500, "text": "original"}}
    assert _ctx(raw, quote="the quoted bit") == ("500", "the quoted bit")


def test_reply_payload_text_is_used_when_there_is_no_quote():
    raw = {"reply_to_message": {"message_id": 500, "text": "what they said"}}
    assert _ctx(raw) == ("500", "what they said")


def test_external_reply_supplies_the_id_and_text():
    raw = {"external_reply": {"message_id": 77, "text": "from another chat"}}
    assert _ctx(raw) == ("77", "from another chat")


def test_no_reply_at_all_yields_nothing():
    assert _ctx({"text": "plain message"}) == (None, None)


def test_no_raw_payload_yields_nothing():
    a = _Adapter(None)
    assert _Adapter._telegram_api_kwargs_reply_context(a, object(), object()) == (None, None)


def test_unrecoverable_reply_still_announces_itself():
    """Privacy-hidden content: the agent must know a reply exists."""
    raw = {"reply_to_message": {"message_id": 900}}
    rid, text = _ctx(raw)
    assert rid == "900"
    assert "900" in text and "unavailable" in text


# --- 2. but the topic root is not a quote ----------------------------------

def test_topic_root_produces_no_phantom_quote():
    """The 2026-08-08 shape: every message in a topic 'replies' to its root."""
    raw = {"message_thread_id": 122, "reply_to_message": {"message_id": 122}}
    rid, text = _ctx(raw)
    assert rid == "122", "the id is still needed for routing"
    assert text is None, "a topic root must not be announced as a lost quote"


def test_forum_topic_created_service_message_is_not_a_quote():
    raw = {"reply_to_message": {"message_id": 122, "forum_topic_created": {"name": "Ops"}}}
    assert _ctx(raw)[1] is None


def test_forum_topic_edited_service_message_is_not_a_quote():
    raw = {"reply_to_message": {"message_id": 122, "forum_topic_edited": {"name": "Ops2"}}}
    assert _ctx(raw)[1] is None


def test_a_real_reply_inside_a_topic_is_still_a_quote():
    """The half that must not break: replying to a PERSON inside a topic."""
    raw = {
        "message_thread_id": 122,
        "reply_to_message": {"message_id": 987, "text": "Vito: посмотри вот это"},
    }
    assert _ctx(raw) == ("987", "Vito: посмотри вот это")


def test_a_real_but_unrecoverable_reply_inside_a_topic_still_announces_itself():
    """Only the ROOT is silenced — a genuine hidden reply keeps its placeholder."""
    raw = {"message_thread_id": 122, "reply_to_message": {"message_id": 987}}
    rid, text = _ctx(raw)
    assert rid == "987"
    assert text is not None and "987" in text


@pytest.mark.parametrize("thread_id,reply_id,expected", [
    (122, 122, True),
    ("122", 122, True),
    (122, "122", True),
    (122, 987, False),
    (None, 122, False),
])
def test_topic_root_detection_compares_ids_as_strings(thread_id, reply_id, expected):
    raw = {"message_thread_id": thread_id}
    assert TelegramAdapter._telegram_reply_is_topic_root(raw, None, reply_id) is expected


def test_topic_root_detection_survives_a_missing_payload():
    assert TelegramAdapter._telegram_reply_is_topic_root(None, None, "122") is False
    assert TelegramAdapter._telegram_reply_is_topic_root({}, None, None) is False
