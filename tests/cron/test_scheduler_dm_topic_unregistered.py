"""Regression: cron delivery to a Telegram private DM topic the adapter has
no registry entry for (live incident 2026-08-05, topic 701226).

The scheduler classifies ``telegram:<positive chat_id>:<thread_id>`` as a
DM-topic target itself, but used to call the canonical metadata resolver
without ``chat_type="dm"``. The resolver then depended on the adapter's
``_get_dm_topic_info`` registry; for an unregistered (legacy/auto-created)
topic it produced bare ``thread_id`` metadata and the Telegram adapter
refused the send with "requires a reply anchor". The scheduler must assert
its own classification by passing ``chat_type="dm"``.
"""

from concurrent.futures import Future
from unittest.mock import AsyncMock, MagicMock, patch

from cron.scheduler import _deliver_result


class _UnregisteredDmTopicAdapter:
    """Class-level ``_get_dm_topic_info`` knows nothing about the requested
    topic (returns None) — the live shape for topics created outside the
    adapter's DM-topic registry."""

    def __init__(self, send_result):
        self.send = AsyncMock(return_value=send_result)

    def _get_dm_topic_info(self, chat_id, thread_id):
        return None


def test_unregistered_dm_topic_still_gets_direct_topic_id_metadata():
    from gateway.config import Platform

    send_result = MagicMock(success=True, raw_response=None)
    adapter = _UnregisteredDmTopicAdapter(send_result)

    pconfig = MagicMock()
    pconfig.enabled = True
    mock_cfg = MagicMock()
    mock_cfg.platforms = {Platform.TELEGRAM: pconfig}

    loop = MagicMock()
    loop.is_running.return_value = True

    def fake_run_coro(coro, _loop):
        # Cron delivery routes through DeliveryRouter._deliver_to_platform;
        # execute the coroutine for real so the metadata asserted below is what
        # the ADAPTER received, not merely what the scheduler intended.
        import asyncio as _asyncio
        future = Future()
        try:
            future.set_result(_asyncio.run(coro))
        except BaseException as exc:  # noqa: BLE001
            future.set_exception(exc)
        return future

    job = {
        "id": "dm-topic-unregistered-job",
        "deliver": "telegram:405154434:701226",
    }

    with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
         patch("cron.scheduler.load_config", return_value={"cron": {"wrap_response": False}}), \
         patch("asyncio.run_coroutine_threadsafe", side_effect=fake_run_coro), \
         patch("tools.send_message_tool._send_to_platform", new=AsyncMock()) as standalone_send:
        result = _deliver_result(
            job,
            "Hello from cron",
            adapters={Platform.TELEGRAM: adapter},
            loop=loop,
        )

    assert result is None
    adapter.send.assert_called_once()
    sent_metadata = adapter.send.call_args.kwargs.get("metadata") or adapter.send.call_args[0][-1]
    assert sent_metadata["telegram_dm_topic_reply_fallback"] is True
    assert sent_metadata["direct_messages_topic_id"] == "701226"
    standalone_send.assert_not_called()
