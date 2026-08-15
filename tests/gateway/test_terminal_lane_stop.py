"""Acceptance for the terminal-lane-stop-verb patch: «стоп» must also stop
the Claude Code lane.

13.08.2026 (hermes-ops infra/orchestration-deadlock-20260813.md §5б): /stop
interrupted the gateway turn, but the terminal-claimer is a separate launchd
process — it claimed the next card 4 seconds after the user said
«остановись». The patch makes every /stop path (and a bare stop word) ALSO
raise the durable lane-wide stop flag via tools/claude_code_core.

Pinned here:

* the gate: the flag is written ONLY under ``HERMES_TERMINAL_LANE_STOP=1``
  AND a HERMES_HOME that resolves to the personal ``~/.hermes`` — the live
  repo is shared by all seven gateways, and six of them must see no change;
* an exact bare «стоп» matches; «стоп» inside a longer phrase does not;
* the flag writer never breaks the /stop reply (failures are swallowed).
"""
import inspect
import json
import os

import pytest

from gateway.run import (
    GatewayRunner,
    _INTERRUPT_REASON_STOP,
    _is_bare_stop_word,
    _maybe_stop_terminal_lane,
    _terminal_lane_stop_enabled,
)
from gateway.session import SessionSource
from gateway.platforms.base import MessageEvent, MessageType, Platform
from tools import claude_code_core as core


def _source(uid="405154434"):
    return SessionSource(platform=Platform.TELEGRAM, chat_type="dm",
                         chat_id="405154434", user_id=uid)


def _event(text="/stop"):
    return MessageEvent(text=text, message_type=MessageType.TEXT,
                        source=_source())


PERSONAL_HOME = os.path.join(os.path.expanduser("~"), ".hermes")


@pytest.fixture()
def lane_env(monkeypatch, tmp_path):
    """Gate OPEN + control dir pointed at tmp (never the real lane)."""
    monkeypatch.setenv("HERMES_TERMINAL_LANE_STOP", "1")
    monkeypatch.setenv("HERMES_HOME", PERSONAL_HOME)
    control = tmp_path / "terminal-control"
    monkeypatch.setenv("KTC_CONTROL_DIR", str(control))
    return control


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------

def test_gate_closed_without_env_flag(monkeypatch):
    monkeypatch.delenv("HERMES_TERMINAL_LANE_STOP", raising=False)
    monkeypatch.setenv("HERMES_HOME", PERSONAL_HOME)
    assert _terminal_lane_stop_enabled() is False


def test_gate_open_only_for_the_personal_home(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_TERMINAL_LANE_STOP", "1")
    monkeypatch.setenv("HERMES_HOME", PERSONAL_HOME)
    assert _terminal_lane_stop_enabled() is True
    # A clinic/profile home (env leaks down inherited process lines — 23.07)
    # closes the gate even with the flag set.
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    assert _terminal_lane_stop_enabled() is False


def test_flag_written_only_under_the_gate(lane_env, monkeypatch, tmp_path):
    assert _maybe_stop_terminal_lane(_source(), reason="/stop") is True
    flag = json.loads((lane_env / "stop-all.json").read_text())
    assert flag["schema"] == "TERMINAL-STOP-V1"
    assert flag["action"] == "stop_all"
    assert flag["reason"] == "/stop"
    assert flag["requested_by"] == "telegram:405154434"
    # Foreign HERMES_HOME → no write.
    os.remove(str(lane_env / "stop-all.json"))
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    assert _maybe_stop_terminal_lane(_source()) is False
    assert not (lane_env / "stop-all.json").exists()


def test_flag_writer_never_raises(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_TERMINAL_LANE_STOP", "1")
    monkeypatch.setenv("HERMES_HOME", PERSONAL_HOME)
    # A FILE squats on the control-dir path: makedirs must fail — and the
    # helper must swallow it and answer False, not detonate the /stop reply.
    squatter = tmp_path / "not-a-dir"
    squatter.write_text("")
    monkeypatch.setenv("KTC_CONTROL_DIR", str(squatter / "sub"))
    assert _maybe_stop_terminal_lane(_source()) is False


# ---------------------------------------------------------------------------
# The bare stop word
# ---------------------------------------------------------------------------

def test_bare_stop_words_default_set(monkeypatch):
    monkeypatch.delenv("HERMES_STOP_WORDS", raising=False)
    for text in ("стоп", "Стоп", "СТОП!", "  стоп.  ", "остановись",
                 "Остановись!!!", "stop", "Stop…"):
        assert _is_bare_stop_word(text) is True, text


def test_stop_word_inside_a_phrase_does_not_trigger(monkeypatch):
    monkeypatch.delenv("HERMES_STOP_WORDS", raising=False)
    for text in ("стоп, я передумал", "давай стоп", "please stop",
                 "стоп слово", "не останавливайся", "", None, "стопудово"):
        assert _is_bare_stop_word(text) is False, repr(text)


def test_stop_words_are_env_configurable(monkeypatch):
    monkeypatch.setenv("HERMES_STOP_WORDS", "halt, тпру")
    assert _is_bare_stop_word("HALT") is True
    assert _is_bare_stop_word("тпру!") is True
    assert _is_bare_stop_word("стоп") is False  # default set replaced


# ---------------------------------------------------------------------------
# /stop paths write the flag
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_busy_stop_command_writes_the_flag(lane_env):
    runner = object.__new__(GatewayRunner)
    interrupted = []

    async def _fake_interrupt(session_key, source, *, interrupt_reason,
                              invalidation_reason):
        interrupted.append((session_key, interrupt_reason))

    runner._interrupt_and_clear_session = _fake_interrupt
    reply = await runner._busy_stop_command(_event(), "key-1", _source())
    assert interrupted == [("key-1", _INTERRUPT_REASON_STOP)]
    assert (lane_env / "stop-all.json").exists()
    assert reply  # the user still gets the stop confirmation


@pytest.mark.asyncio
async def test_busy_stop_command_writes_nothing_when_gate_closed(
        monkeypatch, tmp_path):
    monkeypatch.delenv("HERMES_TERMINAL_LANE_STOP", raising=False)
    control = tmp_path / "terminal-control"
    monkeypatch.setenv("KTC_CONTROL_DIR", str(control))
    runner = object.__new__(GatewayRunner)

    async def _fake_interrupt(session_key, source, *, interrupt_reason,
                              invalidation_reason):
        pass

    runner._interrupt_and_clear_session = _fake_interrupt
    await runner._busy_stop_command(_event(), "key-1", _source())
    assert not control.exists()


class _StoreEntry:
    def __init__(self, session_key):
        self.session_key = session_key


class _FakeStore:
    def __init__(self, session_key):
        self._key = session_key

    def get_or_create_session(self, source):
        return _StoreEntry(self._key)


@pytest.mark.asyncio
async def test_slash_stop_with_no_active_agent_still_stops_the_lane(lane_env):
    """The 13.08 shape exactly: nothing in _running_agents (the work lives in
    a separate launchd process), the user says /stop — the lane flag must be
    raised anyway."""
    runner = object.__new__(GatewayRunner)
    runner._running_agents = {}
    runner.session_store = _FakeStore("agent:main:telegram:dm:405154434:")
    runner._sibling_thread_run_keys = lambda source, key: []
    runner._is_user_authorized = lambda source: True
    runner.adapters = {}
    result = await runner._handle_stop_command(_event())
    assert (lane_env / "stop-all.json").exists()
    assert result  # the "no active task" reply still goes out


@pytest.mark.asyncio
async def test_slash_stop_with_running_agent_writes_the_flag(lane_env):
    runner = object.__new__(GatewayRunner)
    key = "agent:main:telegram:dm:405154434:"
    runner._running_agents = {key: object()}
    runner.session_store = _FakeStore(key)
    interrupted = []

    async def _fake_interrupt(session_key, source, *, interrupt_reason,
                              invalidation_reason):
        interrupted.append(session_key)

    runner._interrupt_and_clear_session = _fake_interrupt
    await runner._handle_stop_command(_event())
    assert interrupted == [key]
    assert (lane_env / "stop-all.json").exists()


# ---------------------------------------------------------------------------
# Wiring pins (the helpers above are unit-tested; these assert the call sites
# exist where the contract says they do)
# ---------------------------------------------------------------------------

def test_handle_message_carries_the_bare_stop_intercept():
    src = inspect.getsource(GatewayRunner._handle_message)
    assert "_is_bare_stop_word(" in src
    assert "_terminal_lane_stop_enabled()" in src
    assert "_busy_stop_command(" in src


def test_stop_handlers_carry_the_lane_stop_call():
    from gateway import slash_commands as sc
    assert "_maybe_stop_terminal_lane(" in inspect.getsource(
        sc.GatewaySlashCommandsMixin._handle_stop_command)
    assert "_maybe_stop_terminal_lane(" in inspect.getsource(
        GatewayRunner._busy_stop_command)


def test_flag_lands_where_the_core_reads_it(lane_env):
    """Producer/consumer agreement: the gateway writes through the same core
    API the claimer reads through."""
    _maybe_stop_terminal_lane(_source(), reason="/stop")
    flag = core.stop_all_active(core.control_dir())
    assert flag is not None and flag["reason"] == "/stop"
