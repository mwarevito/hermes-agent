"""A reader thread's read-only connection must die with the thread.

Production incident 2026-08-11 19:37 BST: the personal gateway hit the
launchd default limit of 256 file descriptors.  ``lsof`` at the moment of
suffocation showed 105 of them on ``~/.hermes/state.db`` (54) and its
``-wal`` (51), almost all read-only, with fd numbers spread from 10 to 252
— i.e. accumulated steadily since the previous restart two days earlier.
The bot silently fell back to a weak model and reported a FALSE "Provider
authentication failed" (Codex was logged in; ``auth.json`` simply could not
be opened).

Root cause: :meth:`SessionDB._get_read_conn` opens ONE read-only connection
per *thread* and parks it in ``threading.local``.  A strong set
(``_read_conns``) deliberately holds it so a dying thread cannot let the GC
reclaim it un-closed — but nothing ever closes it either.  The set is
drained only by ``SessionDB.close()``, and the gateway's SessionDB lives as
long as the process.  Every short-lived worker thread that runs a single
recall/browse query therefore burns two descriptors (db + -wal) for the
rest of the process's life.

Second half of the same defect: the drain in ``close()`` cannot work.  The
connections were opened with sqlite3's default ``check_same_thread=True``,
so ``conn.close()`` from the draining thread raises ``ProgrammingError``;
``close()`` swallows it, the connection is left to the GC (an *un-closed*
free — the exact POSIX-advisory-lock hazard ``hermes_cli.sqlite_safe_read``
exists to prevent) and the live-connection registry drifts upward for good,
which permanently disables byte-probe protection for that database.

Measured on hermes-dev before the fix: 12 short-lived reader threads ->
``_live_connections`` 1 -> 13, +24 open descriptors, and after
``db.close()`` the registry still read 12.
"""

import gc
import os
import threading

import pytest

from hermes_cli import sqlite_safe_read as ssr
from hermes_state import SessionDB


THREADS = 12


def _fd_count() -> int:
    """Open descriptors of this process (macOS + Linux both expose /dev/fd)."""
    return len(os.listdir("/dev/fd"))


@pytest.fixture()
def db(tmp_path):
    d = SessionDB(db_path=tmp_path / "state.db")
    d.create_session(session_id="s1", source="cli", model="m")
    d.append_message("s1", role="user", content="hello graphiti world")
    d.append_message("s1", role="assistant", content="the neo4j daemon is healthy")
    yield d
    d.close()


def _tracked(db) -> int:
    return ssr._live_connections.get(str(db.db_path.resolve()), 0)


@pytest.mark.requires_wal
def test_short_lived_reader_threads_do_not_accumulate_connections(db):
    """N throwaway reader threads must not leave N connections behind."""
    assert db._wal_active, "read-path split only engages under WAL"

    # Warm-up read from a throwaway thread first. The very first read
    # connection materialises the WAL ``-shm`` mapping and parks one
    # descriptor on SQLite's per-inode "unused fd" list (the unix VFS keeps
    # a closed-but-lock-holding fd around to reuse, rather than calling
    # close() and cancelling this process's POSIX locks). Both are one-time,
    # O(1) costs — measuring after them keeps this test about the per-thread
    # leak and nothing else. Measured: warm-up costs 1 fd, the next 12
    # threads cost 0.
    warm = threading.Thread(target=lambda: db.get_messages("s1"))
    warm.start()
    warm.join()

    base_tracked = _tracked(db)
    base_fds = _fd_count()

    for _ in range(THREADS):
        t = threading.Thread(target=lambda: db.get_messages("s1"))
        t.start()
        t.join()

    # No gc.collect(): release must be deterministic at thread death, not
    # dependent on a collection cycle that a long-lived gateway may not run
    # before it runs out of descriptors.
    grown_tracked = _tracked(db) - base_tracked
    grown_fds = _fd_count() - base_fds
    assert grown_tracked == 0, (
        f"{THREADS} dead reader threads left {grown_tracked} tracked "
        f"connections open (registry {base_tracked} -> {_tracked(db)})"
    )
    assert not db._read_conns, (
        f"{len(db._read_conns)} read connections still parked in the strong set"
    )
    assert grown_fds == 0, f"{THREADS} dead reader threads leaked {grown_fds} descriptors"


@pytest.mark.requires_wal
def test_dead_reader_thread_leaves_db_usable(db):
    """Closing on thread death must not disturb other threads' reads."""
    results = {}

    def read(key):
        results[key] = len(db.get_messages("s1"))

    for i in range(3):
        t = threading.Thread(target=read, args=(i,))
        t.start()
        t.join()

    assert results == {0: 2, 1: 2, 2: 2}
    # The owning thread (this one) still reads fine afterwards.
    assert len(db.get_messages("s1")) == 2
    assert db.search_messages("graphiti", limit=5)


@pytest.mark.requires_wal
def test_close_drains_a_still_running_threads_read_connection(db):
    """``close()`` must really close reader connections, not silently fail.

    The reader thread is still alive when ``close()`` runs, so the drain has
    to close the connection cross-thread.  With sqlite3's default same-thread
    guard that raises ``ProgrammingError``, ``close()`` swallows it, and the
    descriptor survives until the GC frees it *without* a close.
    """
    opened = threading.Event()
    release = threading.Event()
    errors = []

    def worker():
        try:
            db.get_messages("s1")
            opened.set()
            release.wait(10)
        except Exception as exc:  # pragma: no cover - surfaced via assert
            errors.append(exc)
            opened.set()

    t = threading.Thread(target=worker)
    t.start()
    assert opened.wait(10), "reader thread never opened its connection"
    assert not errors, errors
    assert _tracked(db) >= 2, "expected a writer + a reader connection"

    db.close()
    assert _tracked(db) == 0, (
        "close() left tracked connections behind — the cross-thread "
        f"conn.close() failed silently (registry = {_tracked(db)})"
    )

    release.set()
    t.join(10)
    assert not t.is_alive()
    gc.collect()
