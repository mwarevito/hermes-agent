"""process(action='wait') must report liveness while it blocks.

Blocking on a background process emitted no activity signal at all, so from
the outside a worker legitimately waiting on a 40-minute build and a wedged
worker were indistinguishable: both are silent.  ``_wait_for_process``
(tools/environments/base.py) already ticks every 10 s; these tests pin the
same contract onto the registry's wait loop.

The activity callback is THREAD-LOCAL, so the last test here pins *where* it
is resolved: a heartbeat resolved on a helper thread reads back ``None`` and
the tick silently does nothing.
"""

import threading
import time as real_time

import pytest

import tools.environments.base as env_base
from tools.process_registry import ProcessRegistry


# ---------------------------------------------------------------------------
# Driveable clock — lets a >60 s wait window run in milliseconds of real time
# ---------------------------------------------------------------------------


class _Clock:
    def __init__(self, start: float = 1000.0):
        self.now = start


class _FakeTime:
    """Stand-in for the ``time`` module whose ``monotonic()`` we drive.

    Everything else delegates to the real module, so ``time.time()`` /
    ``time.sleep()`` inside the code under test behave normally.
    """

    def __init__(self, real, clock: _Clock):
        self._real = real
        self._clock = clock

    def __getattr__(self, name):
        return getattr(self._real, name)

    def monotonic(self) -> float:
        return self._clock.now


class _AdvancingEvent:
    """Session completion event replacement: each wait() burns fake seconds."""

    def __init__(self, clock: _Clock):
        self._clock = clock

    def wait(self, timeout=None) -> bool:
        self._clock.now += timeout if timeout else 1.0
        return False

    def is_set(self) -> bool:
        return False

    def set(self) -> None:
        pass

    def clear(self) -> None:
        pass


@pytest.fixture
def registry(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    return ProcessRegistry()


@pytest.fixture
def activity_ticks():
    """Install a recording activity callback on THIS (the calling) thread.

    ``set_activity_callback`` is thread-local, exactly as the tool executor
    installs it on the worker thread that then calls into the registry.
    """
    ticks = []

    def _cb(message):
        # env_base.time is the fake clock when a test installs one, so the
        # recorded timestamps are in the same units as the wait window.
        ticks.append(
            (env_base.time.monotonic(), threading.current_thread().ident, message)
        )

    env_base.set_activity_callback(_cb)
    try:
        yield ticks
    finally:
        env_base.set_activity_callback(None)


def _install_fake_clock(monkeypatch, clock):
    monkeypatch.setattr("tools.process_registry.time", _FakeTime(real_time, clock))
    monkeypatch.setattr("tools.environments.base.time", _FakeTime(real_time, clock))


class TestWaitEmitsHeartbeat:
    def test_long_wait_ticks_at_least_once_a_minute(
        self, registry, activity_ticks, monkeypatch
    ):
        clock = _Clock()
        _install_fake_clock(monkeypatch, clock)

        session = registry.spawn_local("sleep 300", cwd="/tmp", task_id="t-hb")
        session._completion_event = _AdvancingEvent(clock)
        started = clock.now
        try:
            result = registry.wait(session.id, timeout=180)
            ended = clock.now
        finally:
            monkeypatch.undo()
            registry.kill_process(session.id)

        assert result["status"] == "timeout"
        assert activity_ticks, (
            "a 180 s block emitted no activity at all — indistinguishable "
            "from a wedged worker"
        )

        stamps = [t[0] for t in activity_ticks]
        gaps = [b - a for a, b in zip([started] + stamps, stamps + [ended])]
        assert max(gaps) <= 60.0, (
            f"activity gap of {max(gaps):.0f}s during the wait "
            f"(ticks at {stamps})"
        )

    def test_tick_message_names_the_wait_and_its_age(
        self, registry, activity_ticks, monkeypatch
    ):
        clock = _Clock()
        _install_fake_clock(monkeypatch, clock)

        session = registry.spawn_local("sleep 300", cwd="/tmp", task_id="t-hb-msg")
        session._completion_event = _AdvancingEvent(clock)
        try:
            registry.wait(session.id, timeout=90)
        finally:
            monkeypatch.undo()
            registry.kill_process(session.id)

        assert activity_ticks, "no activity tick to inspect"
        messages = [t[2] for t in activity_ticks]
        assert all("waiting for background process" in m for m in messages), messages
        assert any("s elapsed" in m for m in messages), messages

    def test_short_wait_on_the_real_clock_still_ticks(self, registry, activity_ticks):
        """Same contract without any clock mocking, so the fix cannot be an
        artefact of the fake clock."""
        session = registry.spawn_local("sleep 60", cwd="/tmp", task_id="t-hb-real")
        try:
            result = registry.wait(session.id, timeout=13)
        finally:
            registry.kill_process(session.id)

        assert result["status"] == "timeout"
        assert activity_ticks, "13 s of real blocking produced no activity tick"

    def test_wait_that_ends_immediately_does_not_tick(self, registry, activity_ticks):
        """A process that is already done must not fabricate a heartbeat."""
        session = registry.spawn_local("true", cwd="/tmp", task_id="t-hb-fast")
        result = registry.wait(session.id, timeout=10)

        assert result["status"] == "exited"
        assert activity_ticks == []


class TestActivityCallbackIsThreadLocal:
    def test_callback_is_resolved_on_the_calling_thread(
        self, registry, activity_ticks, monkeypatch
    ):
        """Resolving the callback off the caller's thread silently yields None.

        ``set_activity_callback`` stores into a ``threading.local``: a heartbeat
        that spawns its own thread and calls ``get_activity_callback()`` there
        gets ``None`` and does nothing, while every other assertion in this file
        would still look plausible.  Pin the resolution site itself.
        """
        resolved_on = []
        real_getter = env_base.get_activity_callback

        def _spy():
            resolved_on.append(threading.current_thread().ident)
            return real_getter()

        monkeypatch.setattr(env_base, "get_activity_callback", _spy)

        clock = _Clock()
        _install_fake_clock(monkeypatch, clock)
        caller_tid = threading.current_thread().ident

        session = registry.spawn_local("sleep 300", cwd="/tmp", task_id="t-hb-tid")
        session._completion_event = _AdvancingEvent(clock)
        try:
            registry.wait(session.id, timeout=90)
        finally:
            monkeypatch.undo()
            registry.kill_process(session.id)

        assert resolved_on, "wait() never consulted the activity callback"
        assert set(resolved_on) == {caller_tid}, (
            "activity callback resolved off the calling thread "
            f"(caller={caller_tid}, seen={set(resolved_on)}) — thread-local "
            "lookup there returns None and the heartbeat is a no-op"
        )
        assert activity_ticks, "callback was resolved but never fired"
        assert {t[1] for t in activity_ticks} == {caller_tid}
