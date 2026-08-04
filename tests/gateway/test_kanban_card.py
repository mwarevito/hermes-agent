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
        def __init__(self, success, message_id=None):
            self.success = success
            self.message_id = message_id

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
