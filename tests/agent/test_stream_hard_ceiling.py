"""Absolute ceiling on a single streaming provider call (plan item 7, 2026-08-11).

Every pre-existing streaming watchdog measures *transport liveness*, not
progress: ``last_chunk_time`` is refreshed by ANY frame the provider emits, so
a stream that drips content-free keepalive frames forever resets the stale
detector forever and the call is bounded by nothing.  Measured on the live
code before the fix: a 0.05s-interval ping stream ran until the test's own
generator fuse blew, with the stale detector never firing once.

These tests pin the two properties the ceiling must have:

* a dripping stream dies at a finite wall-clock ceiling measured from the
  START of the call (not from the last frame);
* a healthy long generation under the (generous, 1500s) default ceiling is
  NOT cut — a low ceiling would be worse than the disease.

Fuse discipline (2026-08-11, after a measured hang).  pytest-timeout is not
installed in the live venv, so every fake stream carries its own fuse — but
the fuse deadline is computed ONCE per test, in ``_StreamFuse``, and NEVER
inside the generator.  A deadline computed inside the generator restarts on
every reconnect, so the one failure mode these tests exist to catch (a call
that reconnects forever) trips no fuse at all: the suite ran 12+ minutes with
no output instead of failing.  ``hermes-deploy`` runs the tests before
restarting the gateways, so a hanging suite is a hanging deploy — the very
class of bug this file is about.  ``_StreamFuse`` therefore bounds each test
twice: by wall clock and by the number of provider streams the fake is allowed
to open.  Every test asserts on elapsed time, so a regression fails in bounded
time instead of hanging.
"""

from __future__ import annotations

import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


def _make_anthropic_agent(**kwargs):
    from run_agent import AIAgent

    defaults = dict(
        api_key="test-key",
        base_url="https://example.com/v1",
        model="claude-opus-4-7",
        quiet_mode=True,
        skip_context_files=True,
        skip_memory=True,
    )
    defaults.update(kwargs)
    agent = AIAgent(**defaults)
    agent.api_mode = "anthropic_messages"
    agent._anthropic_client = MagicMock()
    agent._anthropic_api_key = "test-anthropic-key"
    agent._create_request_anthropic_client = lambda *a, **k: agent._anthropic_client
    return agent


class _Fuse(Exception):
    """Raised by a fake stream when the test's own fuse blows."""


class _StreamFuse:
    """One fuse for the whole test, shared by every stream the fake opens.

    Two independent bounds, both computed OUTSIDE the generators:

    * ``deadline`` — a single wall-clock deadline for the test, fixed at
      construction.  A per-generator deadline is not a bound: each reconnect
      would start a fresh one and a reconnect loop would run forever.
    * ``max_streams`` — a cap on how many provider streams the fake will
      open, which trips long before the clock when the loop reconnects fast.
    """

    def __init__(self, seconds: float, max_streams: int = 4):
        self.deadline = time.monotonic() + seconds
        self.max_streams = max_streams
        self.opened = 0

    def opening(self) -> None:
        """Called by the fake provider each time a stream is opened."""
        self.opened += 1
        if self.opened > self.max_streams:
            raise _Fuse(
                f"fake provider opened {self.opened} streams (cap "
                f"{self.max_streams}) — the call reconnected without bound"
            )

    def check(self) -> None:
        """Called from inside a generator on every frame."""
        if time.monotonic() > self.deadline:
            raise _Fuse("test fuse blew — nothing bounded the call")


def _drip_stream(agent, *, fuse_seconds: float, interval: float = 0.05,
                 stop: threading.Event | None = None,
                 max_streams: int = 4) -> _StreamFuse:
    """A stream that emits content-free ping frames forever.

    Each frame refreshes ``last_chunk_time`` — the signal every existing
    watchdog measures — while the call makes zero progress.  Returns the
    test's fuse so the caller can assert on how many streams were opened.
    """

    fuse = _StreamFuse(fuse_seconds, max_streams=max_streams)

    def _gen():
        while True:
            if stop is not None and stop.is_set():
                raise _Fuse("stream aborted")
            fuse.check()
            time.sleep(interval)
            yield SimpleNamespace(type="ping")

    def _side_effect(*args, **kwargs):
        fuse.opening()
        cm = MagicMock()
        stream = MagicMock()
        stream.__iter__ = MagicMock(return_value=_gen())
        cm.__enter__ = MagicMock(return_value=stream)
        cm.__exit__ = MagicMock(return_value=False)
        return cm

    agent._anthropic_client.messages.stream.side_effect = _side_effect
    return fuse


def _final_message():
    msg = MagicMock()
    msg.content = []
    msg.stop_reason = "end_turn"
    msg.usage = SimpleNamespace(input_tokens=10, output_tokens=5)
    return msg


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_dripping_stream_dies_at_absolute_ceiling(monkeypatch):
    """Frames keep arriving, the answer never does — the call must still end.

    The stale timeout is set far above the fuse so a stale kill cannot be
    mistaken for the ceiling: only a start-relative ceiling can end this call.
    """
    monkeypatch.setenv("HERMES_STREAM_STALE_TIMEOUT", "60")
    monkeypatch.setenv("HERMES_STREAM_STALE_GIVEUP", "50")
    monkeypatch.setenv("HERMES_STREAM_HARD_TIMEOUT_SECONDS", "1")

    agent = _make_anthropic_agent()
    stop = threading.Event()
    _drip_stream(agent, fuse_seconds=12.0, stop=stop)
    agent._abort_request_anthropic_client = lambda *a, **k: stop.set()

    t0 = time.time()
    try:
        with pytest.raises(Exception) as excinfo:
            agent._interruptible_streaming_api_call({})
        elapsed = time.time() - t0
    finally:
        stop.set()

    assert not isinstance(excinfo.value, _Fuse), (
        "the fake stream's own fuse ended the call — nothing in the agent did"
    )
    assert elapsed < 6.0, f"ceiling took {elapsed:.1f}s — dripping stream not bounded"
    message = str(excinfo.value)
    assert "hard ceiling" in message, message
    assert "1s" in message, message


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_healthy_long_generation_is_not_cut_by_the_default_ceiling(monkeypatch):
    """A slow but progressing stream under the default ceiling completes.

    Guards the direction that would be worse than the bug: a ceiling low
    enough to chop a healthy long generation.
    """
    monkeypatch.delenv("HERMES_STREAM_HARD_TIMEOUT_SECONDS", raising=False)
    monkeypatch.setenv("HERMES_STREAM_STALE_TIMEOUT", "60")

    agent = _make_anthropic_agent()
    # One healthy stream is all this test needs; a second one means the call
    # reconnected, which is itself the failure — bound it rather than let it
    # generate 30 more frames per round forever.
    fuse = _StreamFuse(20.0, max_streams=1)

    def _gen():
        for _ in range(30):
            fuse.check()
            time.sleep(0.05)
            yield SimpleNamespace(type="ping")

    def _side_effect(*args, **kwargs):
        fuse.opening()
        cm = MagicMock()
        stream = MagicMock()
        stream.__iter__ = MagicMock(return_value=_gen())
        stream.get_final_message = MagicMock(return_value=_final_message())
        cm.__enter__ = MagicMock(return_value=stream)
        cm.__exit__ = MagicMock(return_value=False)
        return cm

    agent._anthropic_client.messages.stream.side_effect = _side_effect

    t0 = time.time()
    resp = agent._interruptible_streaming_api_call({})
    elapsed = time.time() - t0
    assert resp is not None
    assert elapsed < 20.0, f"healthy generation took {elapsed:.1f}s"
    assert fuse.opened == 1, f"healthy stream was reconnected ({fuse.opened} streams)"


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_ceiling_does_not_leave_a_reconnecting_orphan(monkeypatch):
    """The abandoned worker must not open another provider stream.

    The streaming worker retries transport errors itself, and the ceiling's
    abort looks exactly like one. If the ceiling does not flag the cancel, the
    orphaned daemon thread reconnects after the main thread already gave up —
    a runaway provider request nobody is waiting for, plus a second writer
    racing the retry's stream.
    """
    monkeypatch.setenv("HERMES_STREAM_STALE_TIMEOUT", "60")
    monkeypatch.setenv("HERMES_STREAM_STALE_GIVEUP", "50")
    monkeypatch.setenv("HERMES_STREAM_HARD_TIMEOUT_SECONDS", "1")
    # Pinned: with the worker's own retries at 0 this test would pass
    # vacuously — there would be no reconnect to suppress.
    monkeypatch.setenv("HERMES_STREAM_RETRIES", "2")

    agent = _make_anthropic_agent()
    stop = threading.Event()
    # Without the ceiling nothing here ever ends the call: the drip satisfies
    # every frame-relative watchdog and each reconnect used to get a brand-new
    # deadline.  One fuse for the whole test, built here, is what makes the
    # regression fail in seconds instead of hanging the suite.
    fuse = _StreamFuse(8.0, max_streams=3)

    def _gen():
        while True:
            if stop.is_set():
                # What the socket abort actually looks like to the worker — and
                # a class its own retry loop treats as transient and reconnects
                # on. A non-transport exception would exit the worker anyway
                # and prove nothing.
                raise ConnectionError("connection reset by peer")
            fuse.check()
            time.sleep(0.05)
            yield SimpleNamespace(type="ping")

    def _side_effect(*args, **kwargs):
        fuse.opening()
        cm = MagicMock()
        stream = MagicMock()
        stream.__iter__ = MagicMock(return_value=_gen())
        cm.__enter__ = MagicMock(return_value=stream)
        cm.__exit__ = MagicMock(return_value=False)
        return cm

    agent._anthropic_client.messages.stream.side_effect = _side_effect
    agent._abort_request_anthropic_client = lambda *a, **k: stop.set()

    t0 = time.time()
    try:
        with pytest.raises(Exception):
            agent._interruptible_streaming_api_call({})
        # Give an orphaned worker every chance to reconnect before we look.
        time.sleep(1.5)
        elapsed = time.time() - t0
    finally:
        stop.set()

    assert fuse.opened == 1, (
        f"worker reconnected after the ceiling gave up ({fuse.opened} streams "
        f"opened) — the call was abandoned but the request was not"
    )
    assert elapsed < 10.0, f"call took {elapsed:.1f}s — the ceiling did not bound it"


def test_shipped_default_ceiling_is_finite_and_generous():
    """The default must be ON and above every stale floor this module raises.

    An env-only default can be zeroed in a diff with no test noticing — that is
    exactly how a ceiling stops existing. 1200s is the largest stale floor
    ``openai_codex_stale_timeout_floor`` can produce, so the ceiling has to sit
    above it or it would clamp a deliberately-widened healthy timeout.
    """
    from agent import chat_completion_helpers as h

    default = h._STREAM_HARD_TIMEOUT_DEFAULT_SECONDS
    assert default > 0, "the ceiling is disabled by default — it does not exist"
    assert default != float("inf")
    assert default >= 1200.0, "default is below the largest stale floor"


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_ceiling_applies_with_no_env_var_set(monkeypatch):
    """The ceiling engages from its built-in default, not only when configured.

    Nobody sets HERMES_STREAM_HARD_TIMEOUT_SECONDS on the live boxes, so a
    ceiling that only works when the env var is present protects nothing. The
    default value is shrunk here (not the env var set) so this test exercises
    the same branch production takes.
    """
    from agent import chat_completion_helpers as h

    monkeypatch.delenv("HERMES_STREAM_HARD_TIMEOUT_SECONDS", raising=False)
    monkeypatch.setattr(h, "_STREAM_HARD_TIMEOUT_DEFAULT_SECONDS", 1.0)
    monkeypatch.setenv("HERMES_STREAM_STALE_TIMEOUT", "60")
    monkeypatch.setenv("HERMES_STREAM_STALE_GIVEUP", "50")

    agent = _make_anthropic_agent()
    stop = threading.Event()
    _drip_stream(agent, fuse_seconds=12.0, stop=stop)
    agent._abort_request_anthropic_client = lambda *a, **k: stop.set()

    t0 = time.time()
    try:
        with pytest.raises(Exception) as excinfo:
            agent._interruptible_streaming_api_call({})
        elapsed = time.time() - t0
    finally:
        stop.set()

    assert not isinstance(excinfo.value, _Fuse), (
        "the fake stream's own fuse ended the call — the default ceiling is off"
    )
    assert elapsed < 6.0, f"default ceiling took {elapsed:.1f}s"
    assert "hard ceiling" in str(excinfo.value), str(excinfo.value)


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_ceiling_is_disengageable(monkeypatch):
    """``HERMES_STREAM_HARD_TIMEOUT_SECONDS=0`` restores the old behaviour.

    An operator whose provider legitimately runs past any ceiling must be able
    to turn it off rather than be forced onto a shorter leash.
    """
    monkeypatch.setenv("HERMES_STREAM_STALE_TIMEOUT", "60")
    monkeypatch.setenv("HERMES_STREAM_HARD_TIMEOUT_SECONDS", "0")

    agent = _make_anthropic_agent()
    stop = threading.Event()
    _drip_stream(agent, fuse_seconds=3.0, stop=stop)
    agent._abort_request_anthropic_client = lambda *a, **k: stop.set()

    t0 = time.time()
    try:
        with pytest.raises(Exception) as excinfo:
            agent._interruptible_streaming_api_call({})
        elapsed = time.time() - t0
    finally:
        stop.set()

    # With the ceiling off, only the fake stream's own fuse ends the call.
    assert "hard ceiling" not in str(excinfo.value), str(excinfo.value)
    assert elapsed < 20.0, f"disengaged ceiling test took {elapsed:.1f}s"


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_repeated_stale_kills_do_not_abandon_a_recoverable_call(monkeypatch):
    """Two stalls in ONE call, each reconnecting — the call must still finish.

    Regression pin for the give-up-after-N-failed-kills idea removed on
    2026-08-11: counting stale kills and abandoning the call on the second one
    killed exactly this shape — a call whose third stream answered fine.  An
    abort that genuinely never takes effect is bounded by the absolute ceiling
    above, which is measured from the start of the call and is checked on the
    same poll loop as the stale branch.
    """
    monkeypatch.setenv("HERMES_STREAM_STALE_TIMEOUT", "0.4")
    monkeypatch.setenv("HERMES_STREAM_STALE_GIVEUP", "50")
    monkeypatch.setenv("HERMES_STREAM_HARD_TIMEOUT_SECONDS", "0")
    monkeypatch.setenv("HERMES_STREAM_RETRIES", "4")

    agent = _make_anthropic_agent()
    fuse = _StreamFuse(30.0, max_streams=3)
    aborted = threading.Event()

    def _gen(stalls: bool):
        if stalls:
            # Silent for longer than the stale timeout, then dies of the
            # abort — i.e. the kill DOES take effect and the worker retries.
            while not aborted.is_set():
                fuse.check()
                time.sleep(0.05)
            raise ConnectionError("connection reset by peer")
        for _ in range(3):
            fuse.check()
            time.sleep(0.05)
            yield SimpleNamespace(type="ping")

    def _side_effect(*args, **kwargs):
        fuse.opening()
        aborted.clear()
        healthy = fuse.opened >= 3
        cm = MagicMock()
        stream = MagicMock()
        stream.__iter__ = MagicMock(return_value=_gen(stalls=not healthy))
        stream.get_final_message = MagicMock(return_value=_final_message())
        cm.__enter__ = MagicMock(return_value=stream)
        cm.__exit__ = MagicMock(return_value=False)
        return cm

    agent._anthropic_client.messages.stream.side_effect = _side_effect
    agent._abort_request_anthropic_client = lambda *a, **k: aborted.set()

    t0 = time.time()
    resp = agent._interruptible_streaming_api_call({})
    elapsed = time.time() - t0

    assert resp is not None, "a call that recovers on its third stream was abandoned"
    assert fuse.opened == 3, f"expected 3 streams (2 stalls + 1 healthy), got {fuse.opened}"
    assert elapsed < 20.0, f"recovery took {elapsed:.1f}s"
