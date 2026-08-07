"""Tests for process wait timeout-result clarity (not-an-error semantics)."""

import pytest

from tools.process_registry import ProcessRegistry


@pytest.fixture
def registry(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    return ProcessRegistry()


def _spawn_sleeper(registry, notify=False):
    session = registry.spawn_local("sleep 30", cwd="/tmp", task_id="t-waitclar")
    session.notify_on_complete = notify
    return session.id


class TestWaitTimeoutClarity:
    def test_wait_timeout_marks_process_running(self, registry):
        sid = _spawn_sleeper(registry)
        try:
            r = registry.wait(sid, timeout=1)
            assert r["status"] == "timeout"
            assert r["process_running"] is True
            assert "not an error" in r["timeout_note"]
            assert "Uptime" in r["timeout_note"]
        finally:
            registry.kill_process(sid)

    def test_wait_timeout_suggests_notify_when_unset(self, registry):
        sid = _spawn_sleeper(registry, notify=False)
        try:
            r = registry.wait(sid, timeout=1)
            assert "notify_on_complete=true" in r["timeout_note"]
        finally:
            registry.kill_process(sid)

    def test_wait_timeout_defers_to_notify_when_set(self, registry):
        sid = _spawn_sleeper(registry, notify=True)
        try:
            r = registry.wait(sid, timeout=1)
            assert "you will be notified on exit" in r["timeout_note"]
        finally:
            registry.kill_process(sid)

    def test_clamped_wait_keeps_clamp_note_and_running_semantics(self, registry, monkeypatch):
        # The knob that bounds a WAIT is TERMINAL_WAIT_MAX, not TERMINAL_TIMEOUT
        # (which bounds how long a foreground COMMAND may run). The assertion
        # below — clamped waits stay explanatory and non-erroring — is unchanged.
        monkeypatch.setenv("TERMINAL_WAIT_MAX", "1")
        sid = _spawn_sleeper(registry)
        try:
            r = registry.wait(sid, timeout=600)
            assert r["status"] == "timeout"
            assert "clamped" in r["timeout_note"]
            assert "not an error" in r["timeout_note"]
            assert r["process_running"] is True
        finally:
            registry.kill_process(sid)

    def test_foreground_command_timeout_does_not_cut_a_wait_short(self, registry, monkeypatch):
        """A short command timeout must not shorten a block on a running process.

        Bounding "how long may I wait for a background process" by the foreground
        command limit costs one model round-trip per limit-length. Measured
        2026-08-06: 48 waits of ~60 s to cover 45 minutes, in kanban worker
        sessions that then compacted 4x and lost their own context.
        """
        monkeypatch.setenv("TERMINAL_TIMEOUT", "1")
        monkeypatch.setenv("TERMINAL_WAIT_MAX", "900")
        sid = _spawn_sleeper(registry)
        try:
            r = registry.wait(sid, timeout=3)
            # Still running after 3 s, so this is a timeout — but a 3-second one
            # that we asked for, not a 1-second one imposed by the command limit.
            assert r["status"] == "timeout"
            assert "clamped" not in (r.get("timeout_note") or ""), (
                "запрошенное ожидание не должно резаться лимитом команды"
            )
        finally:
            registry.kill_process(sid)

    def test_exited_process_unaffected(self, registry):
        session = registry.spawn_local("true", cwd="/tmp", task_id="t-waitclar")
        r = registry.wait(session.id, timeout=10)
        assert r["status"] == "exited"
        assert "process_running" not in r
