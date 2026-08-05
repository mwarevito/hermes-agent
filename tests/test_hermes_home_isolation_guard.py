"""Regression tests for the session-level HERMES_HOME isolation guard.

The guard (``tests/conftest.py::_isolate_live_hermes_home_for_session``)
runs at conftest import — before pytest collects/imports any test module —
and replaces a live-pointing (or unset) HERMES_HOME with a fresh tempdir.

Incident 2026-08-05: a bare ``pytest`` run with HERMES_HOME unset wrote 189
junk sessions into the live ``~/.hermes/state.db`` (via the import-time
``hermes_state.DEFAULT_DB_PATH`` constant) and rolled the live ``errors.log``
twice in 10 minutes (via ``hermes_cli/main.py``'s import-time
``setup_logging``). These tests pin both the guard's decision table and the
downstream effect (frozen import-time paths do not point at the live home).
"""

import os
import sys
from pathlib import Path

from tests.conftest import _isolate_live_hermes_home_for_session


def _live_home() -> Path:
    if sys.platform == "win32":
        lad = os.environ.get("LOCALAPPDATA", "").strip()
        base = Path(lad) if lad else Path.home() / "AppData" / "Local"
        return (base / "hermes").resolve()
    return (Path.home() / ".hermes").resolve()


class TestGuardDecisionTable:
    def test_unset_hermes_home_is_replaced_with_tempdir(self, monkeypatch):
        monkeypatch.delenv("HERMES_HOME", raising=False)
        monkeypatch.delenv("HERMES_TESTS_ALLOW_LIVE_HOME", raising=False)
        _isolate_live_hermes_home_for_session()
        val = os.environ.get("HERMES_HOME", "")
        assert val, "guard must set HERMES_HOME when it was unset"
        assert Path(val).resolve() != _live_home()
        assert Path(val).is_dir()
        # Standard subdirs pre-created so import-time mkdir-free readers work.
        assert (Path(val) / "logs").is_dir()
        assert (Path(val) / "sessions").is_dir()

    def test_live_home_value_is_replaced(self, monkeypatch):
        monkeypatch.setenv("HERMES_HOME", str(_live_home()))
        monkeypatch.delenv("HERMES_TESTS_ALLOW_LIVE_HOME", raising=False)
        _isolate_live_hermes_home_for_session()
        assert Path(os.environ["HERMES_HOME"]).resolve() != _live_home()

    def test_custom_value_is_respected(self, monkeypatch, tmp_path):
        custom = tmp_path / "custom-hermes-home"
        custom.mkdir()
        monkeypatch.setenv("HERMES_HOME", str(custom))
        monkeypatch.delenv("HERMES_TESTS_ALLOW_LIVE_HOME", raising=False)
        _isolate_live_hermes_home_for_session()
        assert os.environ["HERMES_HOME"] == str(custom)

    def test_allow_flag_bypasses_guard_when_unset(self, monkeypatch):
        monkeypatch.delenv("HERMES_HOME", raising=False)
        monkeypatch.setenv("HERMES_TESTS_ALLOW_LIVE_HOME", "1")
        _isolate_live_hermes_home_for_session()
        assert "HERMES_HOME" not in os.environ or not os.environ["HERMES_HOME"]

    def test_allow_flag_bypasses_guard_when_live(self, monkeypatch):
        live = str(_live_home())
        monkeypatch.setenv("HERMES_HOME", live)
        monkeypatch.setenv("HERMES_TESTS_ALLOW_LIVE_HOME", "1")
        _isolate_live_hermes_home_for_session()
        assert os.environ["HERMES_HOME"] == live


class TestImportTimeConsumersAreIsolated:
    def test_default_db_path_not_live(self):
        """hermes_state.DEFAULT_DB_PATH is frozen at import; with the guard
        active at conftest load, the freeze must have captured a tempdir."""
        import hermes_state

        db = Path(str(hermes_state.DEFAULT_DB_PATH)).resolve()
        assert not str(db).startswith(str(_live_home()) + os.sep), (
            f"DEFAULT_DB_PATH points inside the live home: {db}"
        )

    def test_effective_hermes_home_env_not_live(self):
        """Whole-session env sanity: no test in this process should see a
        live-pointing HERMES_HOME (per-test fixture sets its own tempdir)."""
        val = os.environ.get("HERMES_HOME", "")
        assert val, "HERMES_HOME must be set during tests"
        assert Path(val).resolve() != _live_home()
