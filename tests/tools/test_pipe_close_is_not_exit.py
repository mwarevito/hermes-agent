"""A closed stdout pipe is not a finished process.

Measured on the M1 on 2026-08-11: 20 of 20 background ``claude-hermes``
launches were reported back to the agent as ``status=exited,
exit_code=null, output=""`` within 5-28 seconds while the run was still
going — because the command redirected its own stdout (``claude … > file``),
our read end reached EOF immediately, and ``_reader_loop``'s ``finally``
marked the session exited after a 5-second ``wait()`` that had timed out.

The caller then read an empty result file and launched the same job a
second time (16:17:08 and 16:17:36, two live Claude Code sessions writing
into one file — the first one's seven minutes of work were overwritten).

``exit_code=None`` together with ``completion_reason="exited"`` is the
signature of that branch: a process that really exited always has an int.
"""

import subprocess
import time

import pytest

from tools.process_registry import ProcessRegistry


@pytest.fixture
def registry(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    return ProcessRegistry()


def _spawn_redirecting_sleeper(registry, tmp_path, seconds=30):
    """A live process whose stdout never reaches our pipe.

    ``> file`` is what every real Claude Code launch does, and it makes the
    read end EOF within milliseconds while the child runs for minutes.
    """
    out = tmp_path / "child.out"
    cmd = f"sleep {seconds} > {out} 2>&1"
    session = registry.spawn_local(cmd, cwd="/tmp", task_id="t-pipe-eof")
    return session.id


class TestPipeCloseIsNotExit:
    def test_live_process_is_not_reported_exited_when_its_pipe_closes(
        self, registry, tmp_path
    ):
        sid = _spawn_redirecting_sleeper(registry, tmp_path)
        try:
            # Well past the 5s wait() in the reader thread's finally block.
            time.sleep(8)
            r = registry.poll(sid)
            assert r["status"] == "running", (
                "a live child was reported as %r (exit_code=%r) — the pipe "
                "closing was mistaken for the process ending"
                % (r["status"], r.get("exit_code"))
            )
        finally:
            registry.kill_process(sid)

    def test_wait_does_not_return_exited_with_a_null_exit_code(
        self, registry, tmp_path
    ):
        sid = _spawn_redirecting_sleeper(registry, tmp_path)
        try:
            r = registry.wait(sid, timeout=8)
            assert not (
                r["status"] == "exited" and r.get("exit_code") is None
            ), "exited with exit_code=None is the false-completion signature"
            assert r["status"] in ("running", "timeout")
        finally:
            registry.kill_process(sid)

    def test_the_real_exit_is_still_reported(self, registry, tmp_path):
        """The fix must not trade a false exit for a missed one."""
        sid = _spawn_redirecting_sleeper(registry, tmp_path, seconds=2)
        deadline = time.time() + 25
        while time.time() < deadline:
            r = registry.poll(sid)
            if r["status"] == "exited":
                assert r["exit_code"] == 0
                return
            time.sleep(0.5)
        pytest.fail("the child exited but the session never reported it")
