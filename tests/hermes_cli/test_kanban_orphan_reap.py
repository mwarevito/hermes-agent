"""Tests: what a killed worker leaves running must die, and only the right pid may be signalled.

The money-losing shape observed on 2026-08-11: run 34 of ``t_a6c52bb1`` was
killed by the runtime cap, and the ``claude-hermes -c --model opus`` process it
had started (pid 53706, registry ``session_key`` ``20260811_161505_13208e`` =
that run's ``owner_session_id``) kept running and kept burning tokens. The
reclaim signals the worker's process GROUP, but the worker's own children are
spawned with ``start_new_session=True`` (tools/process_registry.py), so they
lead their own groups and ``killpg(worker)`` provably never reaches them.

The dangerous half of the fix is the refusals: ``claim_lock`` holds the
DISPATCHER's pid, which under an in-gateway dispatcher is the gateway itself
(live value ``MacBookPro.mynet:81235`` = the supervisor of all 7 bots plus the
terminal-claimer). A killer that trusts the wrong pid takes down the whole
host, so the refusal tests here are worth more than the feature.
"""

from __future__ import annotations

import json
import logging
import os
import signal as _signal_mod
import subprocess
import sys
import time
from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb


# ---------------------------------------------------------------------------
# fixtures / helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def kanban_home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_KANBAN_HOME", str(home))
    monkeypatch.setenv("HERMES_KANBAN_CRASH_GRACE_SECONDS", "0")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    db_path = kb.kanban_db_path(board="default")
    kb._INITIALIZED_PATHS.discard(str(db_path.resolve()))
    kb.init_db()
    return home


@pytest.fixture
def conn(kanban_home):
    with kb.connect() as c:
        yield c


def _pid_gone(pid: int) -> bool:
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def _wait_gone(pid: int, timeout: float = 15.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _pid_gone(pid):
            return True
        time.sleep(0.1)
    return _pid_gone(pid)


def _died_by_signal(proc: subprocess.Popen, timeout: float = 15.0):
    """Exit code of a process THIS test owns, or None if it survived.

    ``os.kill(pid, 0)`` is not usable for these: the test process is their
    parent and never waits, so a killed child lingers as a zombie whose pid
    still answers the liveness probe. Only ``Popen.wait`` settles it.
    """
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        return None


def _start_time(pid: int):
    from gateway.status import get_process_start_time
    return get_process_start_time(int(pid))


def _kill_quiet(pid) -> None:
    if not pid:
        return
    try:
        os.kill(int(pid), _signal_mod.SIGKILL)
    except OSError:
        pass


def _spawn_leaf(*extra_argv: str) -> subprocess.Popen:
    """A live process in its OWN session — what a worker's child looks like.

    ``extra_argv`` lands verbatim in the process command line, which is how the
    "never signal anything that looks like a gateway" refusal is exercised
    against a real live pid instead of a mock.
    """
    return subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(300)", *extra_argv],
        start_new_session=True,
    )


def _spawn_sigterm_deaf_leaf() -> subprocess.Popen:
    """A live process that IGNORES SIGTERM — only SIGKILL ends it.

    Every orphan in the other tests dies on the first SIGTERM, which is why the
    escalation was never exercised: deleting the SIGKILL branch left the suite
    fully green (mutant M9, 2026-08-12).
    """
    code = (
        "import signal, sys, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "sys.stdout.write('ready\\n'); sys.stdout.flush()\n"
        "time.sleep(300)\n"
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", code],
        start_new_session=True, stdout=subprocess.PIPE, text=True,
    )
    assert proc.stdout.readline().strip() == "ready", (
        "the SIGTERM-deaf helper never installed its handler"
    )
    return proc


def _recording_kill(log: list, *, deliver: bool = False):
    """A ``kill`` seam that records every signal, optionally delivering it."""
    def _kill(pid, sig):
        log.append((int(pid), int(sig)))
        if deliver:
            os.kill(int(pid), sig)
    return _kill


def _dead_pid() -> int:
    """A pid that is confirmed dead and reaped (so no zombie keeps it 'alive')."""
    p = subprocess.Popen([sys.executable, "-c", "raise SystemExit(0)"])
    p.wait(timeout=30)
    assert _wait_gone(p.pid), "helper process did not actually go away"
    return p.pid


def _registry_row(
    pid: int,
    session_key: str,
    *,
    host_start_time="auto",
    command="claude-hermes -c --model opus",
):
    return {
        "session_id": f"proc-{pid}",
        "command": command,
        "pid": int(pid),
        "pid_scope": "host",
        "host_start_time": (
            _start_time(pid) if host_start_time == "auto" else host_start_time
        ),
        "cwd": "/tmp",
        "started_at": time.time(),
        # The live prod value of this key is the literal string "default" for
        # every row — it is NOT a task id, which is why session_key is the only
        # usable link back to a run.
        "task_id": "default",
        "session_key": session_key,
        "watcher_interval": 0,
        "notify_on_complete": False,
        "watch_patterns": [],
    }


def _write_registry(home: Path, rows) -> Path:
    path = home / "processes.json"
    path.write_text(json.dumps(rows), encoding="utf-8")
    return path


def _read_registry(home: Path):
    path = home / "processes.json"
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))


def _new_task(conn, *, cap=None, assignee="worker", title=None):
    """A ``todo`` task id, created BEFORE its worker exists.

    The worker's argv has to carry the task id (that is the identity proof the
    tree walk demands), so the id must exist before the process is spawned.

    The title is unique per call: ``create_task`` de-duplicates by title inside
    a 1800s window, so two "orphan" cards in one test are the SAME card and a
    test meaning "another task's worker" silently compares an id with itself.
    """
    return kb.create_task(
        conn, title=title or f"orphan-{time.time_ns()}",
        assignee=assignee, max_runtime_seconds=cap,
    )


def _running_task(
    conn, *, worker_pid, owner_session=None, cap=None, elapsed=1800,
    tid=None, assignee="worker",
):
    """A ``running`` task whose run pid is ``worker_pid`` and whose claim is ours."""
    now = int(time.time())
    if tid is None:
        tid = _new_task(conn, cap=cap, assignee=assignee)
    kb.claim_task(conn, tid)
    kb._set_worker_pid(conn, tid, worker_pid)
    with kb.write_txn(conn):
        conn.execute(
            "UPDATE tasks SET started_at = ?, claim_expires = ? WHERE id = ?",
            (now - elapsed, now - 1, tid),
        )
        conn.execute(
            "UPDATE task_runs SET started_at = ?, claim_expires = ?, worker_pid = ? "
            "WHERE id = (SELECT current_run_id FROM tasks WHERE id = ?)",
            (now - elapsed, now - 1, worker_pid, tid),
        )
    run_id = conn.execute(
        "SELECT current_run_id FROM tasks WHERE id = ?", (tid,)
    ).fetchone()[0]
    if owner_session:
        kb.stamp_run_owner_session(conn, run_id, owner_session)
    return tid, run_id


def _spawn_worker_with_detached_child(home: Path, task_id=None, *, via_env=False):
    """A worker whose child leads its OWN session — the pid killpg cannot reach.

    ``task_id`` is planted the way a real worker carries it: in the argv
    (``chat -q "work kanban task <id>"``) or, with ``via_env``, in
    ``HERMES_KANBAN_TASK``. Without it the process is indistinguishable from any
    other python on the box, and the tree walk must refuse it — which is the
    whole point of the identity gate, and why these tests now spawn a worker
    that can actually be identified.
    """
    code = (
        "import subprocess, sys, time\n"
        "p = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)'],\n"
        "                     start_new_session=True)\n"
        "open(sys.argv[1], 'w').write(str(p.pid))\n"
        "time.sleep(300)\n"
    )
    marker = home / f"child-{time.time_ns()}.pid"
    argv = [sys.executable, "-c", code, str(marker)]
    env = dict(os.environ)
    if task_id and via_env:
        env["HERMES_KANBAN_TASK"] = str(task_id)
    elif task_id:
        argv.extend(["chat", "-q", f"work kanban task {task_id}"])
    worker = subprocess.Popen(argv, start_new_session=True, env=env)
    child_pid = None
    for _ in range(400):
        if marker.exists() and marker.read_text().strip():
            child_pid = int(marker.read_text().strip())
            break
        time.sleep(0.05)
    assert child_pid, "worker never reported its grandchild pid"
    # The premise of the whole bug, asserted rather than assumed.
    assert os.getpgid(child_pid) != os.getpgid(worker.pid), (
        "grandchild shares the worker's process group — this test would then "
        "pass for the wrong reason (killpg would reach it)"
    )
    return worker, child_pid


# ---------------------------------------------------------------------------
# the defect
# ---------------------------------------------------------------------------

@pytest.mark.live_system_guard_bypass
def test_detached_grandchild_dies_with_the_reclaimed_worker(conn, kanban_home):
    """A killed worker must not leave a live descendant behind.

    ``live_system_guard_bypass``: the point of this test is real signal
    delivery to a process that has just been reparented to init, which is
    exactly what conftest's subtree guard refuses. Everything the reap can aim
    at here is this test's own grandchild plus an empty tmp registry.

    Before the fix the grandchild survives: it is in its own process group, so
    the reclaim's ``killpg(worker)`` never reaches it, and nothing else looks
    for it. In production that survivor is a running Claude burning tokens.
    """
    tid = _new_task(conn)
    worker, child_pid = _spawn_worker_with_detached_child(kanban_home, tid)
    try:
        _running_task(conn, worker_pid=worker.pid, tid=tid)
        assert kb.reclaim_task(conn, tid, reason="operator") is True
        worker.wait(timeout=20)
        assert _wait_gone(child_pid), (
            f"grandchild {child_pid} survived the reclaim of worker "
            f"{worker.pid} — it leads its own process group, so killpg(worker) "
            f"never reached it and no orphan reap looked for it"
        )
    finally:
        _kill_quiet(child_pid)
        _kill_quiet(worker.pid)


@pytest.mark.live_system_guard_bypass
def test_max_runtime_kill_also_reaps_the_detached_grandchild(conn, kanban_home):
    """``enforce_max_runtime`` is the path that leaked pid 53706 on 2026-08-11.

    ``live_system_guard_bypass``: same reason as the reclaim twin above — the
    orphan is reparented to init before it is signalled.
    """
    tid = _new_task(conn, cap=60)
    worker, child_pid = _spawn_worker_with_detached_child(kanban_home, tid)
    try:
        _running_task(conn, worker_pid=worker.pid, cap=60, elapsed=3600, tid=tid)
        assert kb.enforce_max_runtime(conn) == [tid]
        worker.wait(timeout=20)
        assert _wait_gone(child_pid), (
            f"grandchild {child_pid} survived the runtime-cap kill of worker "
            f"{worker.pid} — the exact shape that left a live claude-hermes "
            f"running after run 34 was capped"
        )
    finally:
        _kill_quiet(child_pid)
        _kill_quiet(worker.pid)


def test_registry_orphan_of_an_already_dead_worker_is_killed_and_pruned(conn, kanban_home):
    """When the worker is already gone, ``processes.json`` is the only handle.

    The process tree is useless here: the descendants were reparented to init
    the moment the worker died. The registry row survives, keyed by the run's
    session — and it must be removed only after the process is confirmed dead.
    """
    leaf = _spawn_leaf()
    sid = "20260812_101010_abcdef"
    try:
        tid, _run = _running_task(conn, worker_pid=_dead_pid(), owner_session=sid)
        _write_registry(kanban_home, [_registry_row(leaf.pid, sid)])

        assert kb.reclaim_task(conn, tid, reason="operator") is True
        rc = _died_by_signal(leaf)
        assert rc is not None and rc < 0, (
            f"registered orphan {leaf.pid} of a dead worker survived the "
            f"reclaim (wait() returned {rc!r}; a signal death is negative)"
        )
        assert _read_registry(kanban_home) == [], (
            "the confirmed-dead orphan row is still in processes.json"
        )
    finally:
        _kill_quiet(leaf.pid)


def test_orphan_registered_under_the_pre_compaction_session_is_still_found(conn, kanban_home):
    """``owner_session_id`` rotates on compaction; registry rows keep the old id.

    ``carry_run_owner_session`` re-stamps the run to the post-compaction
    session, so matching registry rows against the run's CURRENT owner alone
    silently finds nothing for every process the worker started before it
    compacted. The whole session lineage is the correct key.
    """
    from hermes_state import SessionDB

    old_sid = "20260812_090000_oldold"
    new_sid = "20260812_093000_newnew"
    db = SessionDB(db_path=kanban_home / "state.db")
    try:
        db.create_session(old_sid, source="cli")
        db.end_session(old_sid, "compression")
        db.create_session(new_sid, source="cli", parent_session_id=old_sid)
    finally:
        db.close()

    leaf = _spawn_leaf()
    try:
        tid, _run = _running_task(conn, worker_pid=_dead_pid(), owner_session=new_sid)
        _write_registry(kanban_home, [_registry_row(leaf.pid, old_sid)])

        assert kb.reclaim_task(conn, tid, reason="operator") is True
        rc = _died_by_signal(leaf)
        assert rc is not None and rc < 0, (
            f"orphan {leaf.pid} registered under the pre-compaction session "
            f"{old_sid} was not matched to run owner {new_sid} "
            f"(wait() returned {rc!r})"
        )
    finally:
        _kill_quiet(leaf.pid)


def test_reap_reports_what_it_found_and_what_is_left(conn, kanban_home, caplog):
    """Silence is not allowed: the counts must reach the log and the event."""
    leaf = _spawn_leaf()
    sid = "20260812_111111_counts"
    try:
        tid, _run = _running_task(conn, worker_pid=_dead_pid(), owner_session=sid)
        _write_registry(kanban_home, [_registry_row(leaf.pid, sid)])
        with caplog.at_level(logging.INFO, logger="hermes_cli.kanban_db"):
            assert kb.reclaim_task(conn, tid, reason="operator") is True
        text = caplog.text.lower()
        assert "orphan" in text, f"no orphan reap line in the log: {caplog.text!r}"
        for token in ("found", "terminated", "remaining"):
            assert token in text, f"log line does not report {token!r}: {caplog.text!r}"

        row = conn.execute(
            "SELECT payload FROM task_events WHERE task_id = ? AND kind = 'reclaimed'",
            (tid,),
        ).fetchone()
        payload = json.loads(row[0])
        assert payload.get("orphans_found") == 1, payload
        assert payload.get("orphans_terminated") == 1, payload
        assert payload.get("orphans_remaining") == 0, payload
    finally:
        _kill_quiet(leaf.pid)


# ---------------------------------------------------------------------------
# the refusals — worth more than the feature
# ---------------------------------------------------------------------------

def test_a_gateway_process_is_never_signalled_as_an_orphan(conn, kanban_home):
    """A candidate whose command says ``gateway run`` is the supervisor, not an orphan."""
    gw = _spawn_leaf("--profile", "workbot", "gateway", "run")
    sid = "20260812_120000_gwguard"
    signalled: list[tuple[int, int]] = []
    try:
        _write_registry(kanban_home, [
            _registry_row(
                gw.pid, sid,
                command="python -m hermes_cli.main --profile workbot gateway run",
            ),
        ])
        info = kb._reap_worker_orphans(
            None, {sid}, snapshot=[],
            kill=lambda p, s: signalled.append((int(p), int(s))),
        )
        assert gw.pid not in [p for p, _ in signalled], (
            f"a 'gateway run' process ({gw.pid}) was signalled — this is how a "
            f"reaper takes down all 7 bots"
        )
        assert not _pid_gone(gw.pid), "the gateway-shaped process was killed"
        assert any(
            str(gw.pid) in entry and "gateway" in entry
            for entry in info.get("orphan_refusals", [])
        ), info
        assert [r["pid"] for r in _read_registry(kanban_home)] == [gw.pid], (
            "a live, refused row must stay in the registry"
        )
    finally:
        _kill_quiet(gw.pid)


def test_pid_one_is_never_signalled(conn, kanban_home):
    """pid 1 is init/launchd. It is never a worker's orphan."""
    sid = "20260812_130000_pidone"
    signalled: list[tuple[int, int]] = []
    _write_registry(kanban_home, [_registry_row(1, sid, host_start_time=None)])
    info = kb._reap_worker_orphans(
        None, {sid}, snapshot=[],
        kill=lambda p, s: signalled.append((int(p), int(s))),
    )
    assert 1 not in [p for p, _ in signalled], f"pid 1 was signalled: {signalled!r}"
    assert any("1:" in e for e in info.get("orphan_refusals", [])), info
    assert [r["pid"] for r in _read_registry(kanban_home)] == [1], (
        "a refused row must not be silently dropped from the registry"
    )


def test_start_time_mismatch_refuses_and_says_so(conn, kanban_home, caplog):
    """A recycled pid must be refused, not killed — and the refusal must be visible."""
    leaf = _spawn_leaf()
    sid = "20260812_140000_recycle"
    signalled: list[tuple[int, int]] = []
    try:
        bogus = (_start_time(leaf.pid) or 0) + 5000
        _write_registry(
            kanban_home, [_registry_row(leaf.pid, sid, host_start_time=bogus)]
        )
        with caplog.at_level(logging.INFO, logger="hermes_cli.kanban_db"):
            info = kb._reap_worker_orphans(
                None, {sid}, snapshot=[],
                kill=lambda p, s: signalled.append((int(p), int(s))),
            )
        assert leaf.pid not in [p for p, _ in signalled], (
            f"pid {leaf.pid} was signalled despite a start-time mismatch"
        )
        assert not _pid_gone(leaf.pid)
        assert any(
            str(leaf.pid) in entry and "start_time" in entry
            for entry in info.get("orphan_refusals", [])
        ), info
        assert "start_time" in caplog.text, caplog.text
        assert [r["pid"] for r in _read_registry(kanban_home)] == [leaf.pid], (
            "a refused row must not be dropped from the registry"
        )
    finally:
        _kill_quiet(leaf.pid)


def test_own_pid_and_parent_pid_are_never_signalled(conn, kanban_home):
    """Self and ancestors are off-limits — a reaper must not kill its own host."""
    sid = "20260812_150000_selfguard"
    signalled: list[tuple[int, int]] = []
    _write_registry(kanban_home, [
        _registry_row(os.getpid(), sid, host_start_time=None),
        _registry_row(os.getppid(), sid, host_start_time=None),
    ])
    info = kb._reap_worker_orphans(
        None, {sid}, snapshot=[],
        kill=lambda p, s: signalled.append((int(p), int(s))),
    )
    assert signalled == [], f"the reaper signalled itself or its parent: {signalled!r}"
    assert len(info.get("orphan_refusals", [])) == 2, info


def test_a_gateway_worker_pid_is_never_signalled(conn, kanban_home):
    """The catastrophic case: ``worker_pid`` naming the gateway itself.

    ``claim_lock`` holds the dispatcher's pid and the dispatcher runs in the
    gateway, so any confusion between the two ends with the supervisor of all
    seven bots being SIGKILLed. Refuse on the command line, loudly.
    """
    gw = _spawn_leaf("--profile", "workbot", "gateway", "run")
    signalled: list[tuple[int, int]] = []
    try:
        info = kb._terminate_reclaimed_worker(
            gw.pid, kb._claimer_id(),
            signal_fn=lambda p, s: signalled.append((int(p), int(s))),
        )
        assert signalled == [], (
            f"the reaper signalled a 'gateway run' pid: {signalled!r}"
        )
        assert not _pid_gone(gw.pid), "the gateway process was killed"
        assert info.get("refused") == "gateway_process", info
        assert info.get("termination_attempted") is False, info
    finally:
        _kill_quiet(gw.pid)


# ---------------------------------------------------------------------------
# worker identity — the tree walk has to be EARNED (hole 1, 2026-08-12)
# ---------------------------------------------------------------------------

@pytest.mark.live_system_guard_bypass
def test_the_tree_of_an_unidentifiable_pid_is_not_walked(conn, kanban_home):
    """``task_runs.worker_pid`` naming a stranger must not enumerate its children.

    The only place the orphan reap was WORSE than the code it replaced: the old
    reclaim signalled one process group, the new one additionally walks the
    tree of whatever that number points at. There is no recorded start time for
    a worker (``task_runs`` has no such column), so the generic refusal check
    returns None for any unrelated live process — a recycled pid on a terminal
    or a browser would have had its children handed to the reaper.
    """
    tid = _new_task(conn)
    worker, child_pid = _spawn_worker_with_detached_child(kanban_home, None)
    try:
        assert kb._snapshot_worker_descendants(worker.pid, task_id=tid) == [], (
            f"the process tree of pid {worker.pid} was walked even though "
            f"nothing ties it to task {tid} — it could be anyone's process"
        )
        assert not _pid_gone(child_pid), "the stranger's child was touched"
    finally:
        _kill_quiet(child_pid)
        _kill_quiet(worker.pid)


@pytest.mark.live_system_guard_bypass
def test_the_tree_of_a_pid_carrying_another_tasks_id_is_not_walked(conn, kanban_home):
    """Identity is per-task, not "looks like some kanban worker"."""
    mine = _new_task(conn)
    theirs = _new_task(conn)
    worker, child_pid = _spawn_worker_with_detached_child(kanban_home, theirs)
    try:
        assert kb._snapshot_worker_descendants(worker.pid, task_id=mine) == [], (
            f"a worker of task {theirs} was accepted as the worker of {mine}"
        )
    finally:
        _kill_quiet(child_pid)
        _kill_quiet(worker.pid)


@pytest.mark.live_system_guard_bypass
@pytest.mark.parametrize("via_env", [False, True])
def test_a_worker_that_proves_its_task_is_walked(conn, kanban_home, via_env):
    """The positive half: argv OR ``HERMES_KANBAN_TASK`` both identify a worker.

    Without this the safe answer ("never walk anything") would also pass the
    refusal tests while quietly disabling the feature.
    """
    tid = _new_task(conn)
    worker, child_pid = _spawn_worker_with_detached_child(
        kanban_home, tid, via_env=via_env,
    )
    try:
        found = kb._snapshot_worker_descendants(worker.pid, task_id=tid)
        assert child_pid in [c["pid"] for c in found], (
            f"the detached child {child_pid} of an identified worker was not "
            f"snapshotted (found {found!r})"
        )
    finally:
        _kill_quiet(child_pid)
        _kill_quiet(worker.pid)


@pytest.mark.live_system_guard_bypass
def test_an_unidentifiable_worker_leaks_its_child_loudly(conn, kanban_home, caplog):
    """The deliberate degradation, asserted so nobody "fixes" it quietly.

    When identity cannot be proven the child SURVIVES the reclaim. That is the
    chosen trade: a leaked process costs money, a wrong tree walk costs someone
    else's session. It must be loud.
    """
    tid = _new_task(conn)
    worker, child_pid = _spawn_worker_with_detached_child(kanban_home, None)
    try:
        _running_task(conn, worker_pid=worker.pid, tid=tid)
        with caplog.at_level(logging.INFO, logger="hermes_cli.kanban_db"):
            assert kb.reclaim_task(conn, tid, reason="operator") is True
        worker.wait(timeout=20)
        assert not _pid_gone(child_pid), (
            "the child of an unidentifiable worker was killed anyway"
        )
        assert "identity" in caplog.text and str(worker.pid) in caplog.text, (
            f"the refusal to walk the tree was not reported: {caplog.text!r}"
        )
        row = conn.execute(
            "SELECT payload FROM task_events WHERE task_id = ? AND kind = 'reclaimed'",
            (tid,),
        ).fetchone()
        payload = json.loads(row[0])
        assert payload.get("orphan_tree_source") == "none", payload
    finally:
        _kill_quiet(child_pid)
        _kill_quiet(worker.pid)


# ---------------------------------------------------------------------------
# the refusal predicate itself — tested directly, not through a lucky ancestor
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("pid", [1, 0, -1, -12345])
def test_low_and_negative_pids_are_refused_by_the_pid_check_itself(pid):
    """``pid <= 1`` must be its OWN reason, not an accident of ancestry.

    ``test_pid_one_is_never_signalled`` passed for the wrong mechanism (mutant
    M4, 2026-08-12): deleting the ``pid <= 1`` branch kept it green because
    launchd is an ancestor of the test process, so pid 1 was refused as
    ``self_or_ancestor``. On a host where the reaper is not a descendant of pid
    1 — a container, a differently-supervised process — that cover disappears
    and ``kill(0, …)`` means "our whole process group". ``self_pids=set()``
    removes the accidental cover.
    """
    reason = kb._signal_refusal_reason(pid, self_pids=set())
    assert reason == "pid_not_signalable", (
        f"pid {pid} was refused as {reason!r}, not by the pid range check"
    )


def test_strict_refuses_a_live_pid_whose_command_line_cannot_be_read(
    kanban_home, monkeypatch,
):
    """``strict`` is the difference the orphan reap relies on (mutant M10).

    Flipping it to False left the suite green: no test distinguished the two
    modes. The worker kill deliberately stays non-strict (failing closed there
    holds the claim forever), so the modes must diverge, provably.
    """
    leaf = _spawn_leaf()
    try:
        start = _start_time(leaf.pid)
        monkeypatch.setattr(kb, "_process_command_line", lambda pid: "")
        assert kb._signal_refusal_reason(
            leaf.pid, expected_start=start, self_pids=set(), strict=True,
        ) == "cmdline_unreadable"
        assert kb._signal_refusal_reason(
            leaf.pid, expected_start=start, self_pids=set(), strict=False,
        ) is None, "the non-strict worker-kill path must NOT fail closed"
    finally:
        _kill_quiet(leaf.pid)


def test_an_orphan_with_no_recorded_start_time_is_refused_not_killed(
    conn, kanban_home,
):
    """Hole 2: the recycled-pid guard only existed for rows that carried a time.

    ``_host_pid_is_ours`` in ``tools/process_registry.py`` already documents
    where this ends — "a recycled number landed on a desktop browser's session
    leader → Firefox dying". A legacy registry row, or one whose psutil probe
    failed at snapshot time, arrives with ``host_start_time=None``; before this
    it sailed through every check and was killed.
    """
    leaf = _spawn_leaf()
    sid = "20260812_160000_nostart"
    signalled: list = []
    try:
        _write_registry(
            kanban_home, [_registry_row(leaf.pid, sid, host_start_time=None)]
        )
        info = kb._reap_worker_orphans(
            None, {sid}, snapshot=[], kill=_recording_kill(signalled),
        )
        assert signalled == [], (
            f"pid {leaf.pid} was signalled with no recorded start time: "
            f"{signalled!r} — that pid could be anything by now"
        )
        assert not _pid_gone(leaf.pid)
        assert any(
            str(leaf.pid) in e and "start_time_unknown" in e
            for e in info.get("orphan_refusals", [])
        ), info
        assert [r["pid"] for r in _read_registry(kanban_home)] == [leaf.pid], (
            "a refused row must keep its registry entry — it is the last handle"
        )
    finally:
        _kill_quiet(leaf.pid)


# ---------------------------------------------------------------------------
# the central invariant: a row leaves the registry ONLY when its pid is dead
# ---------------------------------------------------------------------------

def test_a_live_refused_orphan_keeps_its_row_while_a_dead_sibling_is_pruned(
    conn, kanban_home,
):
    """The invariant the whole card exists for (mutant M8, 2026-08-12).

    Dropping a row WITHOUT confirming the process is dead is, in the words of
    the code that was reverted on 2026-08-11, "strictly worse than doing
    nothing": the survivor keeps running AND disappears from ``process(kill)``,
    so the last handle on it is gone.

    No previous test could see it. Every scenario had either only dead
    candidates or only live ones, and ``_prune_dead_registry_rows`` returns
    early on ``not dead_pids`` — so removing the per-pid death check left ten
    of ten tests green. One session with BOTH is the counterexample.
    """
    gw = _spawn_leaf("--profile", "workbot", "gateway", "run")
    dead = _dead_pid()
    sid = "20260812_170000_mixed"
    signalled: list = []
    try:
        _write_registry(kanban_home, [
            _registry_row(dead, sid, host_start_time=None),
            _registry_row(
                gw.pid, sid,
                command="python -m hermes_cli.main --profile workbot gateway run",
            ),
        ])
        info = kb._reap_worker_orphans(
            None, {sid}, snapshot=[], kill=_recording_kill(signalled),
        )
        assert signalled == [], f"a refused/dead candidate was signalled: {signalled!r}"
        assert not _pid_gone(gw.pid), "the live refused process was killed"
        left = sorted(r["pid"] for r in _read_registry(kanban_home))
        assert left == [gw.pid], (
            f"registry rows left {left!r}, expected only the LIVE refused pid "
            f"{gw.pid} — dropping the row of a process that is still running "
            f"destroys the last handle on it"
        )
        assert info["orphan_rows_pruned"] == 1, info
    finally:
        _kill_quiet(gw.pid)


# ---------------------------------------------------------------------------
# escalation
# ---------------------------------------------------------------------------

def test_an_orphan_that_ignores_sigterm_is_escalated_to_sigkill(conn, kanban_home):
    """Mutant M9: deleting the SIGKILL branch was invisible to every test.

    A Claude/Codex run installs its own SIGTERM handler; "it exited on the
    first TERM in the lab" is not a reason to trust that in production.
    """
    deaf = _spawn_sigterm_deaf_leaf()
    sid = "20260812_180000_deaf"
    signalled: list = []
    try:
        _write_registry(kanban_home, [_registry_row(deaf.pid, sid)])
        info = kb._reap_worker_orphans(
            None, {sid}, snapshot=[],
            kill=_recording_kill(signalled, deliver=True),
        )
        sigs = [s for p, s in signalled if p == deaf.pid]
        assert int(_signal_mod.SIGTERM) in sigs, signalled
        assert int(_signal_mod.SIGKILL) in sigs, (
            f"a SIGTERM-deaf orphan was never escalated to SIGKILL: {signalled!r}"
        )
        rc = _died_by_signal(deaf)
        assert rc == -int(_signal_mod.SIGKILL), (
            f"the SIGTERM-deaf orphan survived (wait() -> {rc!r})"
        )
        assert info["orphans_terminated"] == 1, info
        assert info["orphans_remaining"] == 0, info
        assert _read_registry(kanban_home) == [], info
    finally:
        _kill_quiet(deaf.pid)


# ---------------------------------------------------------------------------
# which file did we actually read? (holes 4 and 5, and the silence)
# ---------------------------------------------------------------------------

def test_the_assignees_registry_is_searched_not_the_dispatchers(conn, kanban_home):
    """Hole 4/5: a worker's rows are in ITS profile home, not the dispatcher's.

    A worker is spawned with ``HERMES_HOME=resolve_profile_env(assignee)`` and
    ``tools.process_registry`` binds ``CHECKPOINT_PATH`` inside that process, so
    a ``workbot`` card's processes are registered in
    ``<root>/profiles/workbot/processes.json``. The dispatcher tick used to read
    its OWN home — a different file, whose emptiness reads as "no orphans".
    On the live hermes-infra board that is 1 card of 38.
    """
    profile_home = kanban_home / "profiles" / "workbot"
    profile_home.mkdir(parents=True)

    orphan = _spawn_leaf()          # registered in the workbot profile
    decoy = _spawn_leaf()           # registered in the dispatcher's own home
    sid = "20260812_190000_profile"
    try:
        _write_registry(profile_home, [_registry_row(orphan.pid, sid)])
        _write_registry(kanban_home, [_registry_row(decoy.pid, sid)])

        tid, _run = _running_task(
            conn, worker_pid=_dead_pid(), owner_session=sid, assignee="workbot",
        )
        assert kb.reclaim_task(conn, tid, reason="operator") is True

        rc = _died_by_signal(orphan)
        assert rc is not None and rc < 0, (
            f"the orphan registered under the assignee's profile home survived "
            f"(wait() -> {rc!r}) — the dispatcher read its own processes.json"
        )
        assert not _pid_gone(decoy.pid), (
            "a row from the DISPATCHER's registry was reaped for a workbot card"
        )
        assert _read_registry(profile_home) == []
        assert [r["pid"] for r in _read_registry(kanban_home)] == [decoy.pid]
    finally:
        _kill_quiet(orphan.pid)
        _kill_quiet(decoy.pid)


def test_a_missing_registry_is_reported_not_read_as_empty(conn, kanban_home, caplog):
    """"Found 0" and "never opened the file" are different facts.

    Before this, a missing ``processes.json`` returned ``[]`` without one log
    line, and the summary went on to report found/terminated/remaining as if
    the source had been consulted.
    """
    sid = "20260812_200000_nosource"
    tid, _run = _running_task(conn, worker_pid=_dead_pid(), owner_session=sid)
    assert not (kanban_home / "processes.json").exists()

    with caplog.at_level(logging.INFO, logger="hermes_cli.kanban_db"):
        assert kb.reclaim_task(conn, tid, reason="operator") is True

    row = conn.execute(
        "SELECT payload FROM task_events WHERE task_id = ? AND kind = 'reclaimed'",
        (tid,),
    ).fetchone()
    payload = json.loads(row[0])
    assert payload.get("orphan_registry_source") == "absent", payload
    assert payload.get("orphan_registry_path", "").endswith("processes.json"), payload
    assert "does not exist" in caplog.text, caplog.text
    assert "registry source absent" in caplog.text, caplog.text


def test_an_unreadable_registry_is_reported_as_unreadable(conn, kanban_home, caplog):
    """A corrupt file must not be indistinguishable from an empty one."""
    sid = "20260812_210000_corrupt"
    (kanban_home / "processes.json").write_text("{not json", encoding="utf-8")
    tid, _run = _running_task(conn, worker_pid=_dead_pid(), owner_session=sid)

    with caplog.at_level(logging.INFO, logger="hermes_cli.kanban_db"):
        assert kb.reclaim_task(conn, tid, reason="operator") is True

    row = conn.execute(
        "SELECT payload FROM task_events WHERE task_id = ? AND kind = 'reclaimed'",
        (tid,),
    ).fetchone()
    payload = json.loads(row[0])
    assert payload.get("orphan_registry_source") == "unreadable", payload
    assert "cannot read" in caplog.text, caplog.text


# ---------------------------------------------------------------------------
# the crash path — the more common abnormal ending (hole 3)
# ---------------------------------------------------------------------------

def test_a_worker_that_died_on_its_own_still_gets_its_orphans_reaped(
    conn, kanban_home,
):
    """``detect_crashed_workers`` closed the run and reaped nothing.

    The runtime cap was fixed first because that is where the live evidence
    was, but a worker far more often ends itself — nonzero exit, killed by a
    signal, the rate-limit sentinel, a clean exit with no terminal kanban call.
    All of those come through ``detect_crashed_workers``, which nulled
    ``worker_pid`` and called ``_end_run`` (clearing ``current_run_id``), after
    which nothing could tie a ``processes.json`` row to the run that spawned it.
    """
    leaf = _spawn_leaf()
    sid = "20260812_220000_crashpath"
    try:
        tid, run_id = _running_task(
            conn, worker_pid=_dead_pid(), owner_session=sid,
        )
        _write_registry(kanban_home, [_registry_row(leaf.pid, sid)])

        assert kb.detect_crashed_workers(conn) == [tid]

        rc = _died_by_signal(leaf)
        assert rc is not None and rc < 0, (
            f"the orphan {leaf.pid} of a self-terminated worker survived "
            f"(wait() -> {rc!r}) — the crash path never reaped anything"
        )
        assert _read_registry(kanban_home) == []

        row = conn.execute(
            "SELECT payload FROM task_events "
            "WHERE task_id = ? AND kind = 'orphans_reaped'",
            (tid,),
        ).fetchone()
        assert row is not None, "the crash-path reap left no record on the board"
        payload = json.loads(row[0])
        assert payload["orphans_terminated"] == 1, payload
        # No tree to walk: the worker is dead by definition on this path.
        assert payload["orphan_tree_source"] == "none", payload
    finally:
        _kill_quiet(leaf.pid)


def test_the_crash_path_reap_never_breaks_crash_accounting(
    conn, kanban_home, monkeypatch,
):
    """A failing reap must not cost the task its release.

    The reap runs after the main txn and is a best-effort cleanup; the release
    to ``ready`` is the thing the board depends on.
    """
    def _boom(*_a, **_kw):
        raise RuntimeError("registry on fire")

    monkeypatch.setattr(kb, "_reap_worker_orphans", _boom)
    tid, _run = _running_task(
        conn, worker_pid=_dead_pid(), owner_session="20260812_230000_boom",
    )
    assert kb.detect_crashed_workers(conn) == [tid]
    status = conn.execute(
        "SELECT status FROM tasks WHERE id = ?", (tid,),
    ).fetchone()[0]
    assert status == "ready", status


def test_merely_mentioning_the_task_id_is_not_proof_of_being_its_worker(
    conn, kanban_home,
):
    """"Contains t_xxxxxxxx" is not "is the worker of t_xxxxxxxx".

    A task id is ten characters. A worker asked to inspect this card, a grep,
    an editor with the card's file open — all of them carry the id in their
    command line. Only the exact phrase ``_default_spawn`` writes counts, and
    ``HERMES_KANBAN_TASK`` is the spawner-independent second proof.
    """
    tid = _new_task(conn)
    mentioner = _spawn_leaf("--note", f"please look at {tid} later")
    try:
        assert kb._snapshot_worker_descendants(mentioner.pid, task_id=tid) == [], (
            f"a process that merely mentions {tid} was accepted as its worker"
        )
        assert kb._worker_identity_refusal(
            mentioner.pid, tid,
        ) == "not_this_tasks_worker"
    finally:
        _kill_quiet(mentioner.pid)


def test_the_spawn_prompt_and_the_identity_marker_cannot_drift(conn, monkeypatch):
    """The worker's argv is the identity proof — so one string, two readers.

    If ``_default_spawn`` ever words its prompt differently from what
    ``_worker_identity_refusal`` looks for, the reap loses its only pid-side
    proof and silently degrades to "never walk a tree" — green tests, dead
    feature.
    """
    captured: dict = {}

    class _FakePopen:
        def __init__(self, argv, **kwargs):
            captured["argv"] = list(argv)
            self.pid = 424242

    monkeypatch.setattr(kb.subprocess, "Popen", _FakePopen)
    tid = _new_task(conn)
    task = kb.get_task(conn, tid)
    kb._default_spawn(task, str(kanban_dir := "/tmp"))
    assert kanban_dir  # keeps the walrus honest under -O

    cmdline = " ".join(captured["argv"])
    marker = kb.WORKER_ARGV_TASK_MARKER.format(task_id=tid)
    assert marker in cmdline, (
        f"the spawned worker's argv does not contain {marker!r}: {cmdline!r}"
    )
