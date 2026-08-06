"""Regression: cron delivery to a Telegram private DM topic with no usable
reply anchor (live incidents 2026-08-05, topics 701226 and 701649).

The scheduler classifies ``telegram:<positive chat_id>:<thread_id>`` as a
DM-topic target itself. Cron sends never carry a reply anchor, and an
anchor-less DM-topic send routed via ``direct_messages_topic_id`` is
accepted by the Bot API but can land in a lane the user's client never
renders (the 2026-08-05 evening one-shot loss). The scheduler must fall
back to a ROOT-DM send with no thread routing — visible-but-unthreaded
beats invisible — and log a warning naming the job.
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


def test_anchorless_dm_topic_falls_back_to_root_dm(caplog):
    from gateway.config import Platform

    send_result = MagicMock(success=True, message_id="4242", raw_response=None)
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
         patch("cron.scheduler._record_delivery_ack") as record_ack, \
         patch("asyncio.run_coroutine_threadsafe", side_effect=fake_run_coro), \
         patch("tools.send_message_tool._send_to_platform", new=AsyncMock()) as standalone_send:
        with caplog.at_level("WARNING", logger="cron.scheduler"):
            result = _deliver_result(
                job,
                "Hello from cron",
                adapters={Platform.TELEGRAM: adapter},
                loop=loop,
            )

    assert result is None
    adapter.send.assert_called_once()
    sent_metadata = adapter.send.call_args.kwargs.get("metadata") or adapter.send.call_args[0][-1]
    # Root-DM fallback: NO thread routing of any kind may reach the adapter —
    # direct_messages_topic_id without an anchor is the invisible lane.
    assert "direct_messages_topic_id" not in sent_metadata
    assert "thread_id" not in sent_metadata
    assert "telegram_dm_topic_reply_fallback" not in sent_metadata
    # The degradation is warned about, naming the job.
    assert any(
        "no usable reply anchor" in rec.getMessage() and "dm-topic-unregistered-job" in rec.getMessage()
        for rec in caplog.records
    )
    # Confirmed send (success + message id) still records the ack trail.
    record_ack.assert_called_once_with(job, "4242")
    standalone_send.assert_not_called()
