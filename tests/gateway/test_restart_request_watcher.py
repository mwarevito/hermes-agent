"""Tests for the idle-restart request file mechanics (W1.1, 2026-08-05).

hermes-deploy's deferred restart used to be `sleep 75; launchctl kickstart`,
which killed any agent turn still running past the grace period — including
the very turn that ran the deploy (2026-08-03, twice on 2026-08-05).  The
gateway now polls $HERMES_HOME/restart-request-<profile>.json and restarts
itself only when idle, escalating to a graceful drain-restart if it is never
idle for RESTART_REQUEST_ESCALATION_AGE.
"""

import asyncio
import json
import os
import time
from unittest.mock import MagicMock

import pytest

import gateway.run as gateway_run
from tests.gateway.restart_test_helpers import make_restart_runner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_runner():
    runner, adapter = make_restart_runner()
    # The check must go through the same clean-restart entrypoint the
    # /restart command uses; mock it so no real stop() task is spawned.
    runner.request_restart = MagicMock(return_value=True)
    return runner


def _patch_home(monkeypatch, tmp_path, profile="default"):
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
    import hermes_cli.profiles as profiles_mod
    monkeypatch.setattr(
        profiles_mod, "get_active_profile_name", lambda: profile
    )
    return tmp_path / f"restart-request-{profile}.json"


def _write_request(path, payload=None):
    path.write_text(
        json.dumps(payload if payload is not None else
                   {"requested_at": time.time(), "source": "hermes-deploy"})
    )
    return path


# ---------------------------------------------------------------------------
# _restart_request_path
# ---------------------------------------------------------------------------

class TestRestartRequestPath:
    def test_default_profile_filename(self, monkeypatch, tmp_path):
        _patch_home(monkeypatch, tmp_path, profile="default")
        assert (
            gateway_run._restart_request_path()
            == tmp_path / "restart-request-default.json"
        )

    def test_profile_filename(self, monkeypatch, tmp_path):
        _patch_home(monkeypatch, tmp_path, profile="workbot")
        assert (
            gateway_run._restart_request_path()
            == tmp_path / "restart-request-workbot.json"
        )

    def test_profile_lookup_failure_falls_back_to_default(
        self, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)
        import hermes_cli.profiles as profiles_mod

        def _boom():
            raise RuntimeError("profiles unavailable")

        monkeypatch.setattr(profiles_mod, "get_active_profile_name", _boom)
        assert (
            gateway_run._restart_request_path()
            == tmp_path / "restart-request-default.json"
        )


# ---------------------------------------------------------------------------
# _check_restart_request — one tick
# ---------------------------------------------------------------------------

class TestCheckRestartRequest:
    def test_no_file_is_noop(self, monkeypatch, tmp_path):
        _patch_home(monkeypatch, tmp_path)
        runner = _make_runner()
        assert runner._check_restart_request() == "none"
        runner.request_restart.assert_not_called()

    def test_idle_gateway_restarts_and_consumes_file(
        self, monkeypatch, tmp_path
    ):
        path = _write_request(_patch_home(monkeypatch, tmp_path))
        runner = _make_runner()
        assert runner._running_agents == {}

        assert runner._check_restart_request() == "restart"

        # File must be gone BEFORE the restart fires, or the relaunched
        # gateway would consume it again and restart-loop.
        assert not path.exists()
        runner.request_restart.assert_called_once_with(
            detached=False, via_service=True
        )

    def test_active_turn_defers_and_keeps_file(self, monkeypatch, tmp_path):
        path = _write_request(_patch_home(monkeypatch, tmp_path))
        runner = _make_runner()
        runner._running_agents["telegram:123"] = MagicMock()

        assert runner._check_restart_request() == "deferred"

        assert path.exists()
        runner.request_restart.assert_not_called()

    def test_active_turn_defers_repeatedly_below_escalation_age(
        self, monkeypatch, tmp_path
    ):
        path = _write_request(_patch_home(monkeypatch, tmp_path))
        runner = _make_runner()
        runner._running_agents["telegram:123"] = MagicMock()

        for _ in range(3):
            assert runner._check_restart_request() == "deferred"
        assert path.exists()
        runner.request_restart.assert_not_called()

    def test_stale_request_escalates_despite_active_turn(
        self, monkeypatch, tmp_path, caplog
    ):
        path = _write_request(_patch_home(monkeypatch, tmp_path))
        old = time.time() - (gateway_run.RESTART_REQUEST_ESCALATION_AGE + 60)
        os.utime(path, (old, old))
        runner = _make_runner()
        runner._running_agents["telegram:123"] = MagicMock()

        with caplog.at_level("WARNING", logger="gateway.run"):
            assert runner._check_restart_request() == "escalated"

        assert not path.exists()
        # Escalation is still the graceful drain path (current turn gets the
        # drain window; not a mid-turn kill).
        runner.request_restart.assert_called_once_with(
            detached=False, via_service=True
        )
        assert any("ESCALATING" in r.message for r in caplog.records)

    def test_fresh_request_does_not_escalate(self, monkeypatch, tmp_path):
        _write_request(_patch_home(monkeypatch, tmp_path))
        runner = _make_runner()
        runner._running_agents["telegram:123"] = MagicMock()

        assert runner._check_restart_request(
            now=time.time() + gateway_run.RESTART_REQUEST_ESCALATION_AGE - 120
        ) == "deferred"
        runner.request_restart.assert_not_called()

    def test_broken_json_removed_and_logged_without_restart(
        self, monkeypatch, tmp_path, caplog
    ):
        path = _patch_home(monkeypatch, tmp_path)
        path.write_text("{not json!!!")
        runner = _make_runner()

        with caplog.at_level("WARNING", logger="gateway.run"):
            assert runner._check_restart_request() == "invalid"

        assert not path.exists()
        runner.request_restart.assert_not_called()
        assert any(
            "unreadable/invalid" in r.message for r in caplog.records
        )

    def test_non_object_json_treated_as_invalid(
        self, monkeypatch, tmp_path
    ):
        path = _patch_home(monkeypatch, tmp_path)
        path.write_text('"just a string"')
        runner = _make_runner()

        assert runner._check_restart_request() == "invalid"
        assert not path.exists()
        runner.request_restart.assert_not_called()

    def test_draining_gateway_ignores_request(self, monkeypatch, tmp_path):
        path = _write_request(_patch_home(monkeypatch, tmp_path))
        runner = _make_runner()
        runner._draining = True

        assert runner._check_restart_request() == "none"
        assert path.exists()
        runner.request_restart.assert_not_called()

    def test_already_restarting_gateway_ignores_request(
        self, monkeypatch, tmp_path
    ):
        path = _write_request(_patch_home(monkeypatch, tmp_path))
        runner = _make_runner()
        runner._restart_task_started = True

        assert runner._check_restart_request() == "none"
        assert path.exists()
        runner.request_restart.assert_not_called()


# ---------------------------------------------------------------------------
# _restart_request_watcher — loop behavior
# ---------------------------------------------------------------------------

class TestRestartRequestWatcher:
    @pytest.mark.asyncio
    async def test_watcher_consumes_request_and_exits(
        self, monkeypatch, tmp_path
    ):
        path = _write_request(_patch_home(monkeypatch, tmp_path))
        runner = _make_runner()

        await asyncio.wait_for(
            runner._restart_request_watcher(interval=0.01), timeout=2.0
        )

        assert not path.exists()
        runner.request_restart.assert_called_once_with(
            detached=False, via_service=True
        )

    @pytest.mark.asyncio
    async def test_watcher_waits_for_idle_then_restarts(
        self, monkeypatch, tmp_path
    ):
        path = _write_request(_patch_home(monkeypatch, tmp_path))
        runner = _make_runner()
        runner._running_agents["telegram:123"] = MagicMock()

        watcher = asyncio.create_task(
            runner._restart_request_watcher(interval=0.01)
        )
        await asyncio.sleep(0.05)
        assert not watcher.done()
        assert path.exists()
        runner.request_restart.assert_not_called()

        # Turn finishes -> next tick restarts.
        runner._running_agents.clear()
        await asyncio.wait_for(watcher, timeout=2.0)
        assert not path.exists()
        runner.request_restart.assert_called_once()

    @pytest.mark.asyncio
    async def test_watcher_exits_when_gateway_stops(
        self, monkeypatch, tmp_path
    ):
        _patch_home(monkeypatch, tmp_path)
        runner = _make_runner()

        watcher = asyncio.create_task(
            runner._restart_request_watcher(interval=0.01)
        )
        await asyncio.sleep(0.05)
        runner._running = False
        await asyncio.wait_for(watcher, timeout=2.0)
        runner.request_restart.assert_not_called()

    @pytest.mark.asyncio
    async def test_watcher_survives_tick_exception(
        self, monkeypatch, tmp_path
    ):
        _patch_home(monkeypatch, tmp_path)
        runner = _make_runner()
        calls = {"n": 0}

        def _flaky():
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("boom")
            runner._running = False
            return "none"

        runner._check_restart_request = _flaky

        await asyncio.wait_for(
            runner._restart_request_watcher(interval=0.01), timeout=2.0
        )
        assert calls["n"] >= 2
