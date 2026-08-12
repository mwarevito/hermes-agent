"""The API-server adapter must close the writable SessionDBs it cached.

Sibling of #37011/#38803. ``disconnect()`` already closes the ResponseStore
connection, because the reconnect loop in ``gateway.run`` builds a FRESH
adapter on every retry and each abandoned instance otherwise keeps its SQLite
descriptors open until the process dies.

``_open_and_cache_session_db`` opens a **writable** ``SessionDB`` on
``state.db`` and parks it in the per-adapter ``_session_dbs`` map — one per
profile home. That map was never drained on ``disconnect()``, so every
adapter generation left a live writer (db + WAL fds, a registered entry in
``sqlite_safe_read._live_connections``, and a WAL writer slot) behind.

The assertion is a COUNTER, not a spot check: N connect/serve/disconnect
cycles must not increase the number of live tracked connections to state.db.
"""

import asyncio
from pathlib import Path

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.api_server import APIServerAdapter


def _live_conn_count(db_path: Path) -> int:
    """Live (unclosed) tracked SQLite connections to *db_path*, exactly."""
    from hermes_cli.sqlite_safe_read import _key, _live_connections, _live_lock

    with _live_lock:
        return _live_connections.get(_key(db_path), 0)


@pytest.fixture
def isolated_home(tmp_path, monkeypatch):
    home = tmp_path / "hermes-home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    import hermes_constants
    import hermes_state

    monkeypatch.setattr(hermes_constants, "get_hermes_home", lambda: home)
    monkeypatch.setattr(hermes_state, "_default_db_path", lambda: home / "state.db")
    return home


def test_disconnect_closes_cached_session_dbs(isolated_home):
    """Cycling the adapter must not accumulate writable state.db connections."""
    db_path = isolated_home / "state.db"

    async def cycle() -> None:
        adapter = APIServerAdapter(PlatformConfig(enabled=True))
        db = adapter._ensure_session_db()
        assert db is not None, "adapter did not open a SessionDB — test is not exercising the path"
        await adapter.disconnect()

    asyncio.run(cycle())
    baseline = _live_conn_count(db_path)

    for _ in range(4):
        asyncio.run(cycle())

    after = _live_conn_count(db_path)
    assert after == baseline, (
        f"live state.db connections grew {baseline} -> {after} over 4 "
        f"adapter connect/disconnect cycles; disconnect() leaks the "
        f"SessionDB cached in _session_dbs"
    )
