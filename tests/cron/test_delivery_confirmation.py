"""Verified-delivery tests for cron output (design v2, 2026-08-06).

A send only counts as delivered when the platform confirmed it: ``success is
True``. Falsy/None results are failures that arm the EXISTING
``pending_delivery`` output-only retry (live incident 2026-08-05 evening: a
falsy send result was counted as success and a one-shot's output was silently
lost). A confirmed send WITHOUT a message id (the SignalAdapter shape:
success=True/message_id=None on every real send) is CONFIRMED-BUT-UNACKED —
delivered once, no retry, only the ack bookkeeping is skipped. Confirmed sends
leave a durable ack trail — the platform message id lands in
``messages.platform_message_id`` on the cron session's final non-empty
assistant row and in the ``cron/completed.json`` archive that now replaces
the erase-on-retire of one-shot jobs.
"""

import json
import logging
from concurrent.futures import Future
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import cron.scheduler as s
from cron.jobs import (
    create_job,
    load_jobs,
    mark_job_run,
    record_delivered_message_id,
)


@pytest.fixture()
def tmp_cron_dir(tmp_path, monkeypatch):
    """Redirect cron storage to a temp directory."""
    monkeypatch.setattr("cron.jobs.CRON_DIR", tmp_path / "cron")
    monkeypatch.setattr("cron.jobs.JOBS_FILE", tmp_path / "cron" / "jobs.json")
    monkeypatch.setattr("cron.jobs.OUTPUT_DIR", tmp_path / "cron" / "output")
    return tmp_path


def _completed_file(tmp_cron_dir):
    return tmp_cron_dir / "cron" / "completed.json"


def _mock_gateway_env(send_result):
    """Build the (config, adapter, loop, future) mocks for a live-adapter send."""
    from gateway.config import Platform

    adapter = MagicMock()
    adapter.send = AsyncMock(return_value=send_result)

    pconfig = MagicMock()
    pconfig.enabled = True
    mock_cfg = MagicMock()
    mock_cfg.platforms = {Platform.TELEGRAM: pconfig}

    loop = MagicMock()
    loop.is_running.return_value = True

    def fake_run_coro(coro, _loop):
        # Execute the scheduled coroutine (DeliveryRouter._deliver_to_platform
        # for text, adapter.send_* for media) instead of returning a canned
        # future: only then do the adapter mocks above actually decide the
        # outcome under test.
        import asyncio as _asyncio
        completed = Future()
        try:
            completed.set_result(_asyncio.run(coro))
        except BaseException as exc:  # noqa: BLE001
            completed.set_exception(exc)
        return completed

    return mock_cfg, adapter, loop, fake_run_coro


# =========================================================================
# B: falsy/None send result = failure → pending_delivery armed
# =========================================================================

class TestFalsySendResultIsFailure:
    def test_none_send_result_arms_pending_delivery(self, tmp_cron_dir, monkeypatch):
        """A None send result on BOTH the live-adapter and standalone paths
        must arm the output-only pending_delivery retry instead of retiring
        the one-shot as delivered."""
        from gateway.config import Platform

        job = create_job(prompt="one-shot", schedule="30m", deliver="telegram:405154434")
        monkeypatch.setattr(s, "run_job", lambda j, **_kw: (True, "doc", "final response", None))
        monkeypatch.setattr(s, "save_job_output", lambda jid, out: "/tmp/out.md")

        mock_cfg, adapter, loop, fake_run_coro = _mock_gateway_env(None)

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("cron.scheduler.load_config", return_value={"cron": {"wrap_response": False}}), \
             patch("asyncio.run_coroutine_threadsafe", side_effect=fake_run_coro), \
             patch("tools.send_message_tool._send_to_platform", new=AsyncMock(return_value=None)):
            ok = s.run_one_job(job, adapters={Platform.TELEGRAM: adapter}, loop=loop)

        assert ok is True
        jobs = load_jobs()
        assert len(jobs) == 1, "one-shot must NOT be retired on unconfirmed delivery"
        updated = jobs[0]
        assert updated["pending_delivery"]
        assert updated["pending_delivery"]["payload"] == "final response"
        assert updated["last_delivery_error"]

    def test_explicit_failure_result_is_failure(self, tmp_cron_dir):
        """success is not True (explicit failure) is NOT a confirmed delivery."""
        job = create_job(prompt="one-shot", schedule="30m", deliver="telegram:405154434")
        send_result = MagicMock(success=False, message_id=None, error="boom", raw_response=None)
        mock_cfg, adapter, loop, fake_run_coro = _mock_gateway_env(send_result)

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("cron.scheduler.load_config", return_value={"cron": {"wrap_response": False}}), \
             patch("asyncio.run_coroutine_threadsafe", side_effect=fake_run_coro), \
             patch("tools.send_message_tool._send_to_platform", new=AsyncMock(return_value=None)):
            result = s._deliver_result(
                job, "content",
                adapters={list(mock_cfg.platforms)[0]: adapter}, loop=loop,
            )

        assert result is not None


# =========================================================================
# Signal shape: success=True, message_id=None → CONFIRMED-BUT-UNACKED
# =========================================================================

class TestSignalShapeConfirmedWithoutId:
    def test_success_without_message_id_is_confirmed_no_standalone(self, tmp_cron_dir, caplog):
        """SignalAdapter returns success=True/message_id=None on every real
        send. That is a CONFIRMED delivery: delivered once, NO standalone
        fallback (the old rule caused guaranteed double delivery), ack
        skipped with an INFO line."""
        job = create_job(prompt="one-shot", schedule="30m", deliver="telegram:405154434")
        send_result = MagicMock(success=True, message_id=None, raw_response=None)
        mock_cfg, adapter, loop, fake_run_coro = _mock_gateway_env(send_result)

        standalone = AsyncMock(return_value={"message_id": "999"})
        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("cron.scheduler.load_config", return_value={"cron": {"wrap_response": False}}), \
             patch("asyncio.run_coroutine_threadsafe", side_effect=fake_run_coro), \
             patch("tools.send_message_tool._send_to_platform", new=standalone), \
             caplog.at_level(logging.INFO, logger="cron.scheduler"):
            result = s._deliver_result(
                job, "content",
                adapters={list(mock_cfg.platforms)[0]: adapter}, loop=loop,
            )

        assert result is None, "success without message id must count as delivered"
        adapter.send.assert_called_once()
        standalone.assert_not_called()
        assert any(
            "without platform message id" in r.getMessage() and r.levelno == logging.INFO
            for r in caplog.records
        )
        # No ack was recorded (nothing to record).
        assert load_jobs()[0].get("delivered_message_id") is None

    def test_success_without_id_retires_oneshot_without_pending(self, tmp_cron_dir, monkeypatch):
        """End-to-end via run_one_job: the one-shot retires cleanly — no
        pending_delivery, no retry, archived without a message id."""
        from gateway.config import Platform

        job = create_job(prompt="one-shot", schedule="30m", deliver="telegram:405154434")
        monkeypatch.setattr(s, "run_job", lambda j, **_kw: (True, "doc", "final response", None))
        monkeypatch.setattr(s, "save_job_output", lambda jid, out: "/tmp/out.md")

        send_result = MagicMock(success=True, message_id=None, raw_response=None)
        mock_cfg, adapter, loop, fake_run_coro = _mock_gateway_env(send_result)

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("cron.scheduler.load_config", return_value={"cron": {"wrap_response": False}}), \
             patch("asyncio.run_coroutine_threadsafe", side_effect=fake_run_coro):
            ok = s.run_one_job(job, adapters={Platform.TELEGRAM: adapter}, loop=loop)

        assert ok is True
        retired = load_jobs()
        assert len(retired) == 1 and retired[0]["state"] == "completed", (
            "one-shot must retire — delivery was confirmed (the record is "
            "RETAINED for inspection until the retention sweep prunes it)"
        )
        assert retired[0]["enabled"] is False
        assert "pending_delivery" not in retired[0], "confirmed delivery must not arm a retry"
        adapter.send.assert_called_once()
        entries = json.loads(_completed_file(tmp_cron_dir).read_text(encoding="utf-8"))
        assert entries[-1]["id"] == job["id"]
        assert entries[-1]["last_delivery_error"] is None
        assert entries[-1]["delivered_message_id"] is None


# =========================================================================
# B: confirmed send → platform_message_id ack + job completes
# =========================================================================

class TestConfirmedSendAck:
    def test_confirmed_send_writes_platform_message_id_and_completes(self, tmp_cron_dir):
        """success is True + message_id → delivery succeeds, the telegram
        message id is written to the cron session's final assistant row, and
        the retired one-shot is archived with the delivered_message_id."""
        from hermes_state import SessionDB

        job = create_job(prompt="one-shot", schedule="30m", deliver="telegram:405154434")

        # Seed the cron session the way run_job creates it.
        sid = f"cron_{job['id']}_20260806_120000"
        db = SessionDB()
        db.create_session(sid, source="cron")
        db.append_message(sid, "user", "run the thing")
        db.append_message(sid, "assistant", "the result")
        db.close()

        send_result = MagicMock(success=True, message_id=98765, raw_response=None)
        mock_cfg, adapter, loop, fake_run_coro = _mock_gateway_env(send_result)

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("cron.scheduler.load_config", return_value={"cron": {"wrap_response": False}}), \
             patch("asyncio.run_coroutine_threadsafe", side_effect=fake_run_coro):
            result = s._deliver_result(
                job, "content",
                adapters={list(mock_cfg.platforms)[0]: adapter}, loop=loop,
            )

        assert result is None

        # Ack written to the final assistant row of the cron session.
        db = SessionDB()
        row = db._conn.execute(
            "SELECT platform_message_id FROM messages "
            "WHERE session_id = ? AND role = 'assistant' ORDER BY id DESC LIMIT 1",
            (sid,),
        ).fetchone()
        db.close()
        assert row["platform_message_id"] == "98765"

        # Ack stamped on the job record for the archive.
        assert load_jobs()[0]["delivered_message_id"] == "98765"

        # Clean delivery → the one-shot completes (retires) and is archived.
        mark_job_run(job["id"], True, None, delivery_error=None)
        retired = load_jobs()
        assert len(retired) == 1 and retired[0]["state"] == "completed"
        assert retired[0]["enabled"] is False
        assert "pending_delivery" not in retired[0]
        entries = json.loads(_completed_file(tmp_cron_dir).read_text(encoding="utf-8"))
        assert entries[-1]["id"] == job["id"]
        assert entries[-1]["delivered_message_id"] == "98765"


# =========================================================================
# Media confirmation: media sends are collected, not fire-and-forget
# =========================================================================

def _safe_media_file(tmp_path, monkeypatch, name, data=b"media"):
    """Create a media file under a patched safe root so filter_media_delivery_paths accepts it."""
    root = tmp_path / "media-cache"
    media_file = root / name
    media_file.parent.mkdir(parents=True, exist_ok=True)
    media_file.write_bytes(data)
    monkeypatch.setattr("gateway.platforms.base.MEDIA_DELIVERY_SAFE_ROOTS", (root,))
    return media_file.resolve()


class TestMediaDeliveryConfirmation:
    def test_media_only_all_failed_arms_pending_delivery(self, tmp_cron_dir, tmp_path, monkeypatch):
        """A MEDIA-only payload whose every media send failed is a delivery
        failure: pending_delivery arms exactly like a failed text send."""
        from gateway.config import Platform

        photo = _safe_media_file(tmp_path, monkeypatch, "photo.jpg")
        payload = f"MEDIA:{photo}"

        job = create_job(prompt="one-shot", schedule="30m", deliver="telegram:405154434")
        monkeypatch.setattr(s, "run_job", lambda j, **_kw: (True, "doc", payload, None))
        monkeypatch.setattr(s, "save_job_output", lambda jid, out: "/tmp/out.md")

        fail_result = MagicMock(success=False, message_id=None, error="boom", raw_response=None)
        mock_cfg, adapter, loop, fake_run_coro = _mock_gateway_env(fail_result)
        adapter.send_image_file = AsyncMock(return_value=fail_result)

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("cron.scheduler.load_config", return_value={"cron": {"wrap_response": False}}), \
             patch("asyncio.run_coroutine_threadsafe", side_effect=fake_run_coro), \
             patch("tools.send_message_tool._send_to_platform", new=AsyncMock(return_value=None)):
            ok = s.run_one_job(job, adapters={Platform.TELEGRAM: adapter}, loop=loop)

        assert ok is True
        adapter.send.assert_not_called()  # media-only: no text send
        jobs = load_jobs()
        assert len(jobs) == 1, "one-shot must NOT be retired when nothing reached the user"
        assert jobs[0]["pending_delivery"]
        assert jobs[0]["pending_delivery"]["payload"] == payload
        assert jobs[0]["last_delivery_error"]

    def test_text_confirmed_media_failed_no_retry_warns(self, tmp_cron_dir, tmp_path, monkeypatch, caplog):
        """Text already CONFIRMED delivered + media failed → NO pending_delivery
        (a retry would duplicate the text); a WARNING names the job id and the
        failed paths."""
        from gateway.config import Platform

        photo = _safe_media_file(tmp_path, monkeypatch, "photo.jpg")
        payload = f"the text\nMEDIA:{photo}"

        job = create_job(prompt="one-shot", schedule="30m", deliver="telegram:405154434")
        monkeypatch.setattr(s, "run_job", lambda j, **_kw: (True, "doc", payload, None))
        monkeypatch.setattr(s, "save_job_output", lambda jid, out: "/tmp/out.md")

        text_ok = MagicMock(success=True, message_id=111, raw_response=None)
        media_fail = MagicMock(success=False, message_id=None, error="boom", raw_response=None)
        mock_cfg, adapter, loop, fake_run_coro = _mock_gateway_env(text_ok)
        adapter.send_image_file = AsyncMock(return_value=media_fail)

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("cron.scheduler.load_config", return_value={"cron": {"wrap_response": False}}), \
             patch("asyncio.run_coroutine_threadsafe", side_effect=fake_run_coro), \
             caplog.at_level(logging.WARNING, logger="cron.scheduler"):
            ok = s.run_one_job(job, adapters={Platform.TELEGRAM: adapter}, loop=loop)

        assert ok is True
        retired = load_jobs()
        assert len(retired) == 1 and retired[0]["state"] == "completed", (
            "job must retire — the text already reached the user"
        )
        assert "pending_delivery" not in retired[0], (
            "a retry would duplicate the text that was already delivered"
        )
        warning = [
            r for r in caplog.records
            if "NOT scheduling a delivery retry" in r.getMessage()
        ]
        assert warning, "partial media failure must be surfaced as a warning"
        assert job["id"] in warning[0].getMessage()
        assert photo.name in warning[0].getMessage()
        # Text ack still recorded in the archive.
        entries = json.loads(_completed_file(tmp_cron_dir).read_text(encoding="utf-8"))
        assert entries[-1]["delivered_message_id"] == "111"

    def test_media_message_id_used_as_ack_when_text_has_none(self, tmp_cron_dir, tmp_path, monkeypatch):
        """Text confirmed WITHOUT a message id + media confirmed WITH one →
        the media message id becomes the delivery ack."""
        photo = _safe_media_file(tmp_path, monkeypatch, "photo.jpg")
        payload = f"the text\nMEDIA:{photo}"

        job = create_job(prompt="one-shot", schedule="30m", deliver="telegram:405154434")

        text_ok_no_id = MagicMock(success=True, message_id=None, raw_response=None)
        media_ok = MagicMock(success=True, message_id=222, raw_response=None)
        mock_cfg, adapter, loop, fake_run_coro = _mock_gateway_env(text_ok_no_id)
        adapter.send_image_file = AsyncMock(return_value=media_ok)

        with patch("gateway.config.load_gateway_config", return_value=mock_cfg), \
             patch("cron.scheduler.load_config", return_value={"cron": {"wrap_response": False}}), \
             patch("asyncio.run_coroutine_threadsafe", side_effect=fake_run_coro):
            result = s._deliver_result(
                job, payload,
                adapters={list(mock_cfg.platforms)[0]: adapter}, loop=loop,
            )

        assert result is None
        assert load_jobs()[0]["delivered_message_id"] == "222"


# =========================================================================
# Ack row selection: same predicate the watchdog reads
# =========================================================================

class TestAckRowSelection:
    def test_ack_targets_last_nonempty_assistant_row(self, tmp_cron_dir):
        """The ack must land on the last assistant row with non-empty content —
        NOT a later empty/whitespace row (tool-call artifacts), matching the
        row the delivery watchdog reads."""
        from hermes_state import SessionDB

        job = create_job(prompt="one-shot", schedule="30m", deliver="telegram:405154434")
        sid = f"cron_{job['id']}_20260806_120000"
        db = SessionDB()
        db.create_session(sid, source="cron")
        db.append_message(sid, "user", "run the thing", timestamp=1000.0)
        db.append_message(sid, "assistant", "the real result", timestamp=1001.0)
        # Later assistant rows with empty/whitespace content must be skipped
        # even though they win on timestamp/id ordering.
        db.append_message(sid, "assistant", "", timestamp=1002.0)
        db.append_message(sid, "assistant", "   ", timestamp=1003.0)
        db.close()

        s._record_delivery_ack(job, 777)

        db = SessionDB()
        rows = db._conn.execute(
            "SELECT content, platform_message_id FROM messages "
            "WHERE session_id = ? AND role = 'assistant' ORDER BY id",
            (sid,),
        ).fetchall()
        db.close()
        assert rows[0]["content"] == "the real result"
        assert rows[0]["platform_message_id"] == "777"
        assert rows[1]["platform_message_id"] is None
        assert rows[2]["platform_message_id"] is None


# =========================================================================
# C: retired one-shots are archived, not erased
# =========================================================================

class TestCompletedJobArchive:
    def test_retired_oneshot_archived_with_audit_fields(self, tmp_cron_dir):
        job = create_job(
            prompt="one-shot", schedule="30m", deliver="origin",
            origin={"platform": "telegram", "chat_id": "405154434", "thread_id": "701649"},
        )
        record_delivered_message_id(job["id"], "555")

        mark_job_run(job["id"], True, None, delivery_error=None)

        retired = load_jobs()
        assert len(retired) == 1 and retired[0]["state"] == "completed"
        entries = json.loads(_completed_file(tmp_cron_dir).read_text(encoding="utf-8"))
        assert len(entries) == 1
        record = entries[0]
        assert record["id"] == job["id"]
        assert record["name"] == job["name"]
        assert record["deliver"] == "origin"
        assert record["origin"]["chat_id"] == "405154434"
        assert record["last_delivery_error"] is None
        assert record["delivered_message_id"] == "555"
        assert record["completed_at"]

    def test_archive_capped_at_200_entries(self, tmp_cron_dir):
        completed_file = _completed_file(tmp_cron_dir)
        completed_file.parent.mkdir(parents=True, exist_ok=True)
        completed_file.write_text(
            json.dumps([{"id": f"old{i}"} for i in range(200)]), encoding="utf-8"
        )

        job = create_job(prompt="one-shot", schedule="30m")
        mark_job_run(job["id"], True, None, delivery_error=None)

        entries = json.loads(completed_file.read_text(encoding="utf-8"))
        assert len(entries) == 200
        assert entries[-1]["id"] == job["id"]
        assert entries[0]["id"] == "old1"  # oldest entry dropped

    def test_record_delivered_message_id_missing_job_is_noop(self, tmp_cron_dir):
        record_delivered_message_id("nonexistent", "1")  # must not raise
        assert load_jobs() == []

    def test_corrupt_completed_json_quarantined_and_archive_continues(self, tmp_cron_dir, caplog):
        """A corrupt completed.json is renamed to completed.json.corrupt.<ts>
        and the new record still lands in a fresh archive — never silently
        dropped forever."""
        completed_file = _completed_file(tmp_cron_dir)
        completed_file.parent.mkdir(parents=True, exist_ok=True)
        completed_file.write_text("{not valid json", encoding="utf-8")

        job = create_job(prompt="one-shot", schedule="30m")
        with caplog.at_level(logging.WARNING, logger="cron.jobs"):
            mark_job_run(job["id"], True, None, delivery_error=None)

        # Archive continued with a fresh list containing the new record.
        entries = json.loads(completed_file.read_text(encoding="utf-8"))
        assert [e["id"] for e in entries] == [job["id"]]

        # Corrupt original quarantined beside it, contents preserved.
        corrupt = sorted(completed_file.parent.glob("completed.json.corrupt.*"))
        assert len(corrupt) == 1
        assert corrupt[0].read_text(encoding="utf-8") == "{not valid json"
        assert any("corrupt" in r.getMessage() for r in caplog.records)
