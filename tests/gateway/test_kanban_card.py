"""Live status card for kanban notifications.

Vito 04.08: one card per task, edited in place — not a stream of pings, and it
must survive a gateway restart (the id lives on the subscription row, not in
adapter memory).
"""
import asyncio

from gateway.config import Platform
from gateway.run import GatewayRunner
from hermes_cli import kanban_db as kb


class CardAdapter:
    """Adapter that records sends and edits, and can be told to fail edits."""

    _no_edit_instances = set()

    def __getattribute__(self, name):
        if name == "edit_message" and id(self) in type(self)._no_edit_instances:
            raise AttributeError(name)
        return object.__getattribute__(self, name)

    def __init__(self, edit_ok=True, has_edit=True):
        self.sent = []
        self.edits = []
        self._edit_ok = edit_ok
        self._next_id = 100
        if not has_edit:
            # Platforms without edit support (the base contract allows it):
            # hasattr(adapter, "edit_message") must be False for them.
            self.__dict__["edit_message"] = None
            type(self)._no_edit_instances.add(id(self))

    class _Res:
        def __init__(self, success, message_id=None, error=None):
            self.success = success
            self.message_id = message_id
            self.error = error

    async def send(self, chat_id, text, metadata=None):
        self._next_id += 1
        self.sent.append({"chat_id": chat_id, "text": text, "metadata": metadata or {}})
        return self._Res(True, self._next_id)

    async def edit_message(self, chat_id, message_id, text, **kwargs):
        self.edits.append({"chat_id": chat_id, "message_id": message_id, "text": text})
        return self._Res(self._edit_ok, message_id if self._edit_ok else None)


def _make_runner(adapter):
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._running = True
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._kanban_sub_fail_counts = {}
    # Model the default gateway AFTER its dispatcher acquired the singleton
    # lock — without the handle the notifier owns no subscriptions and the
    # tick delivers nothing (mirrors tests/gateway/test_kanban_notifier.py).
    runner._kanban_dispatcher_lock_handle = object()
    return runner


def _sub_row(tid):
    conn = kb.connect()
    try:
        rows = kb.list_notify_subs(conn, tid)
        return rows[0] if rows else None
    finally:
        conn.close()


def _make_sub(tmp_env, title="card task"):
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title=title, assignee="worker")
        kb.add_notify_sub(conn, task_id=tid, platform="telegram", chat_id="chat-1")
        return tid
    finally:
        conn.close()


def test_first_card_is_sent_and_its_id_persisted(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "card1.db"))
    kb.init_db()
    tid = _make_sub(monkeypatch)
    adapter = CardAdapter()
    runner = _make_runner(adapter)
    sub = _sub_row(tid)

    asyncio.run(runner._kanban_deliver_card(adapter, sub, None, "первая карточка", {}))

    assert len(adapter.sent) == 1, "первая доставка должна быть обычной отправкой"
    assert adapter.edits == []
    stored = _sub_row(tid)
    assert stored["card_message_id"] == str(adapter.sent[0] and 101), (
        "id карточки обязан лечь в подписку — иначе рестарт гейтвея создаст вторую"
    )
    assert stored["card_text"] == "первая карточка"


def test_second_update_edits_the_same_card(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "card2.db"))
    kb.init_db()
    tid = _make_sub(monkeypatch)
    adapter = CardAdapter()
    runner = _make_runner(adapter)

    asyncio.run(runner._kanban_deliver_card(adapter, _sub_row(tid), None, "шаг 1", {}))
    asyncio.run(runner._kanban_deliver_card(adapter, _sub_row(tid), None, "шаг 2", {}))

    assert len(adapter.sent) == 1, "второе обновление не должно плодить сообщения"
    assert [e["text"] for e in adapter.edits] == ["шаг 2"]
    assert _sub_row(tid)["card_text"] == "шаг 2"


def test_unchanged_text_costs_nothing(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "card3.db"))
    kb.init_db()
    tid = _make_sub(monkeypatch)
    adapter = CardAdapter()
    runner = _make_runner(adapter)

    asyncio.run(runner._kanban_deliver_card(adapter, _sub_row(tid), None, "то же", {}))
    asyncio.run(runner._kanban_deliver_card(adapter, _sub_row(tid), None, "то же", {}))

    assert len(adapter.sent) == 1
    assert adapter.edits == [], "одинаковый текст не должен тратить правку"


def test_failed_edit_falls_back_to_a_fresh_card(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "card4.db"))
    kb.init_db()
    tid = _make_sub(monkeypatch)
    adapter = CardAdapter(edit_ok=False)
    runner = _make_runner(adapter)

    asyncio.run(runner._kanban_deliver_card(adapter, _sub_row(tid), None, "шаг 1", {}))
    asyncio.run(runner._kanban_deliver_card(adapter, _sub_row(tid), None, "шаг 2", {}))

    assert len(adapter.edits) == 1, "правка была попробована"
    assert len(adapter.sent) == 2, "провал правки => свежая карточка, а не тишина"
    assert _sub_row(tid)["card_message_id"] == "102", "новый id перезаписывает старый"


def test_adapter_without_edit_support_still_delivers(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "card5.db"))
    kb.init_db()
    tid = _make_sub(monkeypatch)
    adapter = CardAdapter(has_edit=False)
    runner = _make_runner(adapter)

    asyncio.run(runner._kanban_deliver_card(adapter, _sub_row(tid), None, "шаг 1", {}))
    asyncio.run(runner._kanban_deliver_card(adapter, _sub_row(tid), None, "шаг 2", {}))

    assert len(adapter.sent) == 2, "платформа без правки сообщений просто отправляет"


def test_progress_card_says_what_is_happening(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "card6.db"))
    kb.init_db()
    from gateway.kanban_watchers import _render_progress_card

    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="долгая задача", assignee="worker")
        task = kb.get_task(conn, tid)
    finally:
        conn.close()

    text = _render_progress_card(task, task_id=tid, now=int(__import__("time").time()) + 300)
    assert tid in text
    assert "долгая задача" in text
    assert "работает" in text
    assert "мин" in text, "пользователь должен видеть, сколько это уже идёт"


# ── SendResult checks (2026-08-05 incident: success=False marked 'sent') ─────


async def _run_one_notifier_tick(monkeypatch, runner):
    """One full notifier tick, same shape as tests/gateway/test_kanban_notifier.py."""
    real_sleep = asyncio.sleep

    async def fake_sleep(delay):
        if delay == 5:
            return None
        runner._running = False
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    await runner._kanban_notifier_watcher(interval=1)


class RejectingAdapter:
    """send() RETURNS success=False instead of raising — the telegram
    DM-topic guard shape ("requires a reply anchor") that silently lost
    the t_0fc6b0dd delivery."""

    def __init__(self):
        self.sent = []

    async def send(self, chat_id, text, metadata=None):
        self.sent.append({"chat_id": chat_id, "text": text, "metadata": metadata or {}})
        return CardAdapter._Res(
            False, error="Telegram DM topic send requires a reply anchor",
        )


def test_returned_send_failure_is_recorded_failed_not_sent(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "card7.db"))
    kb.init_db()
    from gateway.kanban_watchers import TERMINAL_KINDS

    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="lost delivery", assignee="worker")
        kb.add_notify_sub(conn, task_id=tid, platform="telegram", chat_id="chat-1")
        kb.complete_task(conn, tid, summary="готово")
    finally:
        conn.close()

    adapter = RejectingAdapter()
    runner = _make_runner(adapter)
    asyncio.run(_run_one_notifier_tick(monkeypatch, runner))

    assert len(adapter.sent) == 1, "отправка была попробована"
    conn = kb.connect()
    try:
        row = conn.execute(
            "SELECT status FROM kanban_notify_deliveries WHERE task_id = ?",
            (tid,),
        ).fetchone()
        assert row is not None and row[0] == "failed", (
            "success=False обязан лечь в леджер как 'failed', не 'sent'"
        )
        assert len(kb.list_notify_subs(conn, tid)) == 1, (
            "подписка не должна удаляться, пока событие не доставлено"
        )
        _, events = kb.unseen_events_for_sub(
            conn,
            task_id=tid,
            platform="telegram",
            chat_id="chat-1",
            kinds=list(TERMINAL_KINDS),
        )
        assert [ev.kind for ev in events] == ["completed"], (
            "курсор не должен уехать за недоставленное событие"
        )
    finally:
        conn.close()


class DmTopicCardAdapter(CardAdapter):
    """Telegram-style adapter that declares the sub's thread a DM topic lane."""

    def _get_dm_topic_info(self, chat_id, thread_id):
        return {"name": "General"}


def test_adhoc_user_topic_also_strips_thread_routing(tmp_path, monkeypatch, caplog):
    """The 2026-08-05 t_0fc6b0dd shape: ad-hoc USER topic, no operator config.

    _get_dm_topic_info knows only operator-declared topics, so the canonical
    helper returns a bare thread_id for an ad-hoc topic — which telegram's
    anchor guard refuses with a silent success=False. A private-looking chat
    id + thread with no anchor must strip to the root DM just like the
    operator-declared case.
    """
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "card9.db"))
    kb.init_db()
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="adhoc topic task", assignee="worker")
        kb.add_notify_sub(
            conn, task_id=tid, platform="telegram", chat_id="405154434",
            thread_id="701898",
        )
        kb.complete_task(conn, tid, summary="готово")
    finally:
        conn.close()

    adapter = CardAdapter()  # no _get_dm_topic_info — ad-hoc topic
    runner = _make_runner(adapter)
    with caplog.at_level("WARNING"):
        asyncio.run(_run_one_notifier_tick(monkeypatch, runner))

    assert len(adapter.sent) == 1
    meta = adapter.sent[0]["metadata"] or {}
    assert "thread_id" not in meta
    assert "direct_messages_topic_id" not in meta
    assert any(
        "root DM" in rec.getMessage() and str(tid) in rec.getMessage()
        for rec in caplog.records
    )


def test_dm_topic_sub_send_strips_invisible_thread_routing(tmp_path, monkeypatch, caplog):
    """Anchor-less DM-topic watcher sends must go to the ROOT DM.

    A bare thread_id trips telegram's anchor guard (silent success=False), and
    the canonical fallback (direct_messages_topic_id, no anchor) is accepted by
    the Bot API but renders nowhere the user looks (2026-08-05 evening cron
    incident). The only user-visible option without an anchor is the root DM.
    """
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "card8.db"))
    kb.init_db()
    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="dm topic task", assignee="worker")
        kb.add_notify_sub(
            conn, task_id=tid, platform="telegram", chat_id="405154434",
            thread_id="701898",
        )
        kb.complete_task(conn, tid, summary="готово")
    finally:
        conn.close()

    adapter = DmTopicCardAdapter()
    runner = _make_runner(adapter)
    with caplog.at_level("WARNING"):
        asyncio.run(_run_one_notifier_tick(monkeypatch, runner))

    assert len(adapter.sent) == 1
    meta = adapter.sent[0]["metadata"] or {}
    assert "direct_messages_topic_id" not in meta, (
        "анкорлесс DM-топик: direct_messages_topic_id уводит сообщение в "
        "невидимую полосу — маршрутизация должна быть срезана до корня лички"
    )
    assert "telegram_dm_topic_reply_fallback" not in meta
    assert "thread_id" not in meta
    assert any(
        "root DM" in rec.getMessage() and str(tid) in rec.getMessage()
        for rec in caplog.records
    ), "провал маршрутизации в топик должен логироваться WARNING'ом с task id"


class ArtifactRejectingAdapter(CardAdapter):
    """send_document() RETURNS success=False instead of raising."""

    def __init__(self):
        super().__init__()
        self.documents = []

    async def send_document(self, chat_id, file_path, metadata=None):
        self.documents.append(file_path)
        return CardAdapter._Res(False, error="upload rejected")


def test_artifact_send_failure_is_logged_with_task_id(tmp_path, monkeypatch, caplog):
    import logging

    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "card9.db"))
    kb.init_db()
    artifact = tmp_path / "report.txt"
    artifact.write_text("данные")

    conn = kb.connect()
    try:
        tid = kb.create_task(conn, title="artifact task", assignee="worker")
        task = kb.get_task(conn, tid)
    finally:
        conn.close()

    adapter = ArtifactRejectingAdapter()
    runner = _make_runner(adapter)
    with caplog.at_level(logging.WARNING, logger="gateway.run"):
        asyncio.run(runner._deliver_kanban_artifacts(
            adapter=adapter,
            chat_id="chat-1",
            metadata={},
            event_payload={"artifacts": [str(artifact)]},
            task=task,
        ))

    assert len(adapter.documents) == 1, "загрузка была попробована"
    warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert any(tid in m and "NOT sent" in m for m in warnings), (
        "провал загрузки артефакта обязан попасть в WARNING с id задачи, "
        f"а не потеряться молча; получили: {warnings}"
    )
