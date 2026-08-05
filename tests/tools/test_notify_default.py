"""A background job announces itself; nobody should have to poll for it.

Measured 2026-08-04: 101 process poll/wait calls in a day, ZERO
notify_on_complete — one kanban task spent 25 of its 80 minutes asking
"is it done yet?". The capability was there; the default was wrong.
"""
from tools import terminal_tool


def _call_kwargs(monkeypatch, args):
    """Run the tool's arg-dispatch and capture what reaches terminal_tool()."""
    captured = {}

    def fake_terminal_tool(**kwargs):
        captured.update(kwargs)
        return "ok"

    monkeypatch.setattr(terminal_tool, "terminal_tool", fake_terminal_tool)
    terminal_tool._handle_terminal(args)
    return captured


def test_background_job_notifies_by_default(monkeypatch):
    got = _call_kwargs(monkeypatch, {"command": "pytest -q", "background": True})
    assert got.get("notify_on_complete") is True, (
        "фоновая задача обязана сама сообщить о завершении — иначе воркер "
        "уходит в опрос по 60 секунд за ход модели"
    )


def test_foreground_stays_silent(monkeypatch):
    got = _call_kwargs(monkeypatch, {"command": "ls", "background": False})
    assert got.get("notify_on_complete") is False, (
        "переднему плану уведомление не нужно: результат и так возвращается"
    )


def test_explicit_false_still_wins(monkeypatch):
    got = _call_kwargs(
        monkeypatch,
        {"command": "tail -f app.log", "background": True, "notify_on_complete": False},
    )
    assert got.get("notify_on_complete") is False, (
        "явный отказ от уведомления должен побеждать дефолт (долгоживущие "
        "процессы, которые не завершаются)"
    )


def test_explicit_true_on_foreground_is_respected(monkeypatch):
    got = _call_kwargs(
        monkeypatch,
        {"command": "make build", "background": False, "notify_on_complete": True},
    )
    assert got.get("notify_on_complete") is True
