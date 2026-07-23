"""Regression tests: HERMES_CRON_SESSION must be context-scoped, not process-wide.

Incident (2026-07): the cron scheduler set ``os.environ["HERMES_CRON_SESSION"]``
process-wide while sharing its process with the gateway. After the first cron
tick, every later *interactive* session was treated as a cron session by the
approval system (cron_mode=deny), hard-blocking execute_code in live DMs.
"""

import contextvars
import pathlib

import pytest

from gateway.session_context import _UNSET, _VAR_MAP, get_session_env
from utils import env_var_enabled

VAR = "HERMES_CRON_SESSION"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv(VAR, raising=False)
    # Other tests in the suite may run cron job code directly in this thread's
    # context, leaving the marker set there. Reset to the never-set sentinel so
    # copied contexts start clean regardless of test order.
    token = _VAR_MAP[VAR].set(_UNSET)
    yield
    _VAR_MAP[VAR].reset(token)


def _run_in_fresh_context(fn):
    """Run *fn* in a copied context (simulates a separate handler context)."""
    ctx = contextvars.copy_context()
    return ctx.run(fn)


def test_cron_marker_visible_inside_job_context():
    def job():
        _VAR_MAP[VAR].set("1")
        return env_var_enabled(VAR)

    assert _run_in_fresh_context(job) is True


def test_cron_marker_does_not_leak_to_other_contexts():
    def job():
        _VAR_MAP[VAR].set("1")
        return env_var_enabled(VAR)

    assert _run_in_fresh_context(job) is True
    # A different (interactive) context must NOT observe the cron marker.
    assert _run_in_fresh_context(lambda: env_var_enabled(VAR)) is False
    # Neither must the current context.
    assert env_var_enabled(VAR) is False


def test_environ_fallback_still_works_when_var_never_set(monkeypatch):
    monkeypatch.setenv(VAR, "1")
    assert _run_in_fresh_context(lambda: env_var_enabled(VAR)) is True


def test_explicitly_cleared_context_suppresses_environ(monkeypatch):
    monkeypatch.setenv(VAR, "1")

    def cleared():
        _VAR_MAP[VAR].set("")
        return env_var_enabled(VAR)

    assert _run_in_fresh_context(cleared) is False


def test_get_session_env_registration():
    assert VAR in _VAR_MAP

    def job():
        _VAR_MAP[VAR].set("1")
        return get_session_env(VAR)

    assert _run_in_fresh_context(job) == "1"


def test_unmapped_env_vars_unaffected(monkeypatch):
    monkeypatch.setenv("HERMES_SOME_PLAIN_FLAG", "1")
    assert env_var_enabled("HERMES_SOME_PLAIN_FLAG") is True
    assert env_var_enabled("HERMES_SOME_MISSING_FLAG") is False


def test_scheduler_source_does_not_set_environ_cron_marker():
    """Pin: the scheduler must never reintroduce the process-wide env var."""
    src = (
        pathlib.Path(__file__).resolve().parents[2] / "cron" / "scheduler.py"
    ).read_text()
    assert 'os.environ["HERMES_CRON_SESSION"]' not in src
    assert '_VAR_MAP["HERMES_CRON_SESSION"].set("1")' in src
