"""A subscription's own reply anchor must count as an anchor.

2026-08-08. Vito's background results stopped arriving entirely: 16 subscriptions,
zero deliveries. The subscription rows carried a valid
``telegram_reply_to_message_id``, but the resolver asked
``_thread_metadata_for_target`` — which sees only the (chat, thread) target and
not the subscription — whether an anchor existed, was told "no", and downgraded
the send to the root DM.

For a user who reaches the bot only through a DM topic, the root DM is not a
weaker lane; it is a closed one ("Forbidden: bot can't initiate conversation with
a user"). So the downgrade, whose whole purpose was to guarantee visibility,
guaranteed the opposite.

The 2026-08-05 downgrade itself is still correct when there is genuinely no
anchor anywhere — the second test pins that half so this fix cannot quietly undo
it.
"""

from gateway.config import Platform
from gateway.run import GatewayRunner


def _runner(target_meta):
    """A runner whose target-level resolver returns ``target_meta``."""
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._thread_metadata_for_target = lambda *a, **kw: dict(target_meta)
    return runner


def _sub(anchor=None, thread_id="702915", task_id="t_test"):
    sub = {
        "task_id": task_id,
        "chat_id": "405154434",
        "thread_id": thread_id,
        "platform": "telegram",
    }
    if anchor is not None:
        sub["delivery_metadata"] = {
            "chat_type": "dm",
            "direct_messages_topic_id": thread_id,
            "telegram_dm_topic_reply_fallback": True,
            "telegram_reply_to_message_id": anchor,
            "thread_id": thread_id,
        }
    return sub


TARGET_META = {
    "thread_id": "702915",
    "direct_messages_topic_id": "702915",
    "telegram_dm_topic_reply_fallback": True,
}


def test_stored_anchor_keeps_thread_routing(caplog):
    runner = _runner(TARGET_META)
    with caplog.at_level("WARNING"):
        meta = runner._kanban_visible_thread_metadata(
            Platform.TELEGRAM, _sub(anchor="9178"), adapter=object()
        )

    assert meta.get("telegram_reply_to_message_id") == "9178"
    assert meta.get("thread_id") == "702915", "the topic must survive — it is the only open lane"
    assert not any("root DM" in r.getMessage() for r in caplog.records)


def test_anchorless_sub_still_downgrades_to_root_dm(caplog):
    """The 2026-08-05 t_0fc6b0dd guard, unchanged: no anchor anywhere -> strip."""
    runner = _runner(TARGET_META)
    with caplog.at_level("WARNING"):
        meta = runner._kanban_visible_thread_metadata(
            Platform.TELEGRAM, _sub(anchor=None), adapter=object()
        )

    assert meta == {}
    assert any("root DM" in r.getMessage() for r in caplog.records)


def test_empty_anchor_string_is_not_an_anchor():
    """An empty/None anchor must not defeat the downgrade by looking truthy."""
    runner = _runner(TARGET_META)
    sub = _sub(anchor="9178")
    sub["delivery_metadata"]["telegram_reply_to_message_id"] = ""
    assert runner._kanban_visible_thread_metadata(
        Platform.TELEGRAM, sub, adapter=object()
    ) == {}


def test_cache_does_not_leak_a_decision_across_subs_on_one_topic():
    """Two subscriptions, same topic, different anchors — different answers.

    The decision cache is keyed per target; the anchor is per subscription. If
    the anchor were left out of the key, whichever sub ran first would decide for
    the other — either resurrecting the black hole or bypassing the downgrade.
    """
    runner = _runner(TARGET_META)

    with_anchor = runner._kanban_visible_thread_metadata(
        Platform.TELEGRAM, _sub(anchor="9178", task_id="t_a"), adapter=object()
    )
    without = runner._kanban_visible_thread_metadata(
        Platform.TELEGRAM, _sub(anchor=None, task_id="t_b"), adapter=object()
    )

    assert with_anchor.get("telegram_reply_to_message_id") == "9178"
    assert without == {}

    # And again in the opposite order, now served from cache.
    again_with = runner._kanban_visible_thread_metadata(
        Platform.TELEGRAM, _sub(anchor="9178", task_id="t_c"), adapter=object()
    )
    assert again_with.get("telegram_reply_to_message_id") == "9178"


def test_target_meta_that_already_has_an_anchor_is_untouched():
    """When the target resolver knows an anchor, the sub must not override it."""
    runner = _runner(dict(TARGET_META, telegram_reply_to_message_id="555"))
    meta = runner._kanban_visible_thread_metadata(
        Platform.TELEGRAM, _sub(anchor="9178"), adapter=object()
    )
    assert meta.get("telegram_reply_to_message_id") == "555"


def test_no_target_metadata_at_all_stays_empty():
    """An empty target meta means "no thread routing to speak of" — unchanged."""
    runner = _runner({})
    assert runner._kanban_visible_thread_metadata(
        Platform.TELEGRAM, _sub(anchor="9178"), adapter=object()
    ) == {}
