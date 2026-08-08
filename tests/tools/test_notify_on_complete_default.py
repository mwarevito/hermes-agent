"""A bounded background job announces itself unless silence was asked for.

Measured 2026-08-04: 101 ``process`` poll/wait calls in a single day and ZERO
``notify_on_complete`` — a model round-trip and up to 60 seconds of wall-clock
burned per "is it done yet?". The default now follows ``background``.

This has been live for days with no test naming it, so the version scanner could
not see it and an upgrade would have dropped it silently. The explicit-``false``
case is pinned just as hard: a default that cannot be overridden is not a
default, and long-lived servers must stay able to say nothing.
"""
import pytest

import tools.terminal_tool as tt


@pytest.fixture
def calls(monkeypatch):
    """Capture what _handle_terminal passes down, without running anything."""
    seen = {}

    def _fake(**kwargs):
        seen.update(kwargs)
        return {"status": "ok"}

    monkeypatch.setattr(tt, "terminal_tool", _fake)
    return seen


def _handle(args, calls):
    tt._handle_terminal(args)
    return calls


def test_background_defaults_to_notifying(calls):
    """The whole point: a bounded background job announces itself."""
    out = _handle({"command": "sleep 5", "background": True}, calls)
    assert out["notify_on_complete"] is True


def test_foreground_does_not_notify_by_default(calls):
    """A foreground command already returns its result — nothing to announce."""
    out = _handle({"command": "echo hi"}, calls)
    assert out["notify_on_complete"] is False


def test_explicit_false_survives_the_default(calls):
    """A default that cannot be overridden is not a default.

    Long-lived servers and watchers ask for silence on purpose.
    """
    out = _handle(
        {"command": "npm run dev", "background": True, "notify_on_complete": False},
        calls,
    )
    assert out["notify_on_complete"] is False


def test_explicit_true_in_the_foreground_is_honoured(calls):
    out = _handle({"command": "echo hi", "notify_on_complete": True}, calls)
    assert out["notify_on_complete"] is True


def test_background_false_is_treated_as_foreground(calls):
    out = _handle({"command": "echo hi", "background": False}, calls)
    assert out["notify_on_complete"] is False


def test_the_schema_still_documents_background_as_off_by_default(calls):
    """The default that changed is notify_on_complete, NOT background itself."""
    props = tt.TERMINAL_SCHEMA["parameters"]["properties"]
    assert props["background"]["default"] is False
