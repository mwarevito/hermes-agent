"""Acceptance for the terminal-lane STOP contract (claude-code-cancel patch).

Two halves:

* CORE (tools/claude_code_core.py, stdlib-only, standalone): the stop-flag
  API — atomic writes, TERMINAL-STOP-V1 schema, fail-CLOSED reads (a flag
  file that exists but cannot be parsed is an ACTIVE flag), resume_lane as
  the single sanctioned deletion — and ``run_claude(should_abort=…)``: a
  truthy probe kills the WHOLE process group through the same ladder the
  deadline uses, an absent probe keeps the previous behavior. These tests
  import only the core and MUST run anywhere the core imports (they are the
  locally-runnable acceptance of the patch).

* TOOL (tools/claude_code_tool.py): the new cancel / no_claude / resume_lane
  actions. The tool pulls in tools.registry (gateway environment), so this
  half SKIPS cleanly where that environment is unavailable and runs in full
  inside the pinned repo (hermes-deploy step 2c / hermes-patch-verify).

Canon: hermes-ops infra/orchestration-deadlock-20260813.md §5б («сказал
„остановись" — карточку взяли через 4 секунды; глагола отмены нет»).
"""
import json
import os
import stat
import textwrap
import time

import pytest

import tools.claude_code_core as core

try:  # the tool needs the hermes-agent environment (tools.registry etc.)
    import tools.claude_code_tool as cct
    _TOOL_IMPORT_ERROR = None
except Exception as exc:  # pragma: no cover - exercised outside the repo only
    cct = None
    _TOOL_IMPORT_ERROR = exc

requires_tool = pytest.mark.skipif(
    cct is None,
    reason="tools.claude_code_tool needs the hermes-agent environment: %r"
           % (_TOOL_IMPORT_ERROR,))


# ---------------------------------------------------------------------------
# CORE — flag files
# ---------------------------------------------------------------------------

def test_core_version_carries_the_stop_contract():
    assert core.CORE_VERSION == "1.2.0"
    assert core.TERMINAL_STOP_SCHEMA == "TERMINAL-STOP-V1"


def test_control_dir_env_seam_and_default(tmp_path):
    assert str(core.control_dir({"KTC_CONTROL_DIR": str(tmp_path)})) == str(tmp_path)
    default = str(core.control_dir({}))
    assert default.endswith(os.path.join(".hermes", "kanban", "terminal-control"))


def test_write_stop_flag_schema_and_private_dir(tmp_path):
    ctrl = tmp_path / "ctl"
    path = core.write_stop_flag(ctrl, "stop_all", requested_by="telegram:405154434",
                                reason="/stop")
    assert path.name == "stop-all.json"
    flag = json.loads(path.read_text())
    assert flag["schema"] == "TERMINAL-STOP-V1"
    assert flag["action"] == "stop_all"
    assert flag["requested_by"] == "telegram:405154434"
    assert flag["reason"] == "/stop"
    assert flag["card"] is None and flag["cascade_from"] is None
    assert flag["at"]  # ISO stamp present
    # No tmp leftovers, and the directory is private (no group/other bits).
    assert sorted(p.name for p in ctrl.iterdir()) == ["stop-all.json"]
    assert stat.S_IMODE(os.stat(str(ctrl)).st_mode) & 0o077 == 0


def test_write_stop_flag_is_atomic_same_directory_replace(tmp_path, monkeypatch):
    ctrl = tmp_path / "ctl"
    seen = {}
    real_replace = os.replace

    def spy(src, dst):
        seen["src"], seen["dst"] = src, dst
        return real_replace(src, dst)

    monkeypatch.setattr(core.os, "replace", spy)
    core.write_stop_flag(ctrl, "cancel", card="t_12345678", reason="передумали")
    # tmp file lived in the SAME directory as the target: os.replace is only
    # atomic within a filesystem, and same-dir is the guarantee of that.
    assert os.path.dirname(seen["src"]) == os.path.dirname(seen["dst"])
    assert seen["dst"].endswith("t_12345678.cancel.json")


def test_per_card_flag_names_and_cascade_from(tmp_path):
    ctrl = tmp_path / "ctl"
    p1 = core.write_stop_flag(ctrl, "cancel", card="t_aaaa0001", board="hermes-infra",
                              reason="стоп", cascade_from="t_ffff0000")
    p2 = core.write_stop_flag(ctrl, "no_claude", card="t_aaaa0001", reason="руками")
    assert p1.name == "t_aaaa0001.cancel.json"
    assert p2.name == "t_aaaa0001.no-claude.json"
    flag = json.loads(p1.read_text())
    assert flag["cascade_from"] == "t_ffff0000"
    assert flag["board"] == "hermes-infra"


def test_write_stop_flag_refuses_caller_bugs(tmp_path):
    with pytest.raises(ValueError):
        core.write_stop_flag(tmp_path, "cancel")  # per-card action, no card
    with pytest.raises(ValueError):
        core.write_stop_flag(tmp_path, "cancel", card="../../etc/passwd")
    with pytest.raises(ValueError):
        core.write_stop_flag(tmp_path, "explode", card="t_12345678")
    # None of the refusals may leave files behind.
    assert not os.path.isdir(str(tmp_path / "does-not-exist"))
    assert [p.name for p in tmp_path.iterdir() if p.is_file()] == []


def test_read_stop_flags_absent_is_none(tmp_path):
    flags = core.read_stop_flags(tmp_path / "never-created", "t_12345678")
    assert flags == {"stop_all": None, "cancel": None, "no_claude": None}


def test_read_stop_flags_reads_back_what_was_written(tmp_path):
    ctrl = tmp_path / "ctl"
    core.write_stop_flag(ctrl, "cancel", card="t_12345678", reason="хватит")
    core.write_stop_flag(ctrl, "stop_all", reason="/stop")
    flags = core.read_stop_flags(ctrl, "t_12345678")
    assert flags["cancel"]["reason"] == "хватит"
    assert flags["stop_all"]["action"] == "stop_all"
    assert flags["no_claude"] is None
    # A different card sees only the lane-wide flag.
    other = core.read_stop_flags(ctrl, "t_87654321")
    assert other["cancel"] is None and other["stop_all"] is not None


def test_broken_json_flag_is_active_fail_closed(tmp_path):
    ctrl = tmp_path / "ctl"
    ctrl.mkdir()
    (ctrl / "stop-all.json").write_text("{torn write, not json")
    (ctrl / "t_12345678.cancel.json").write_text("")
    active = core.stop_all_active(ctrl)
    assert active is not None and active["unreadable"] is True
    assert active["reason"] == "unreadable" and active["action"] == "stop_all"
    flags = core.read_stop_flags(ctrl, "t_12345678")
    assert flags["cancel"]["unreadable"] is True
    # Non-dict JSON is just as unreadable as garbage.
    (ctrl / "stop-all.json").write_text("[1, 2, 3]")
    assert core.stop_all_active(ctrl)["unreadable"] is True


def test_unsafe_card_id_never_reaches_the_filesystem(tmp_path):
    flags = core.read_stop_flags(tmp_path, "../../outside")
    assert flags["cancel"] is None and flags["no_claude"] is None


def test_resume_lane_removes_only_stop_all(tmp_path):
    ctrl = tmp_path / "ctl"
    core.write_stop_flag(ctrl, "stop_all", reason="/stop")
    core.write_stop_flag(ctrl, "cancel", card="t_12345678", reason="стоп")
    assert core.resume_lane(ctrl) is True
    assert core.stop_all_active(ctrl) is None
    # The per-card cancel is untouched: re-opening the lane is not
    # un-cancelling a card.
    assert core.read_stop_flags(ctrl, "t_12345678")["cancel"] is not None
    assert core.resume_lane(ctrl) is False  # nothing left to remove


# ---------------------------------------------------------------------------
# CORE — run_claude(should_abort=…)
# ---------------------------------------------------------------------------

def _fake_launcher(tmp_path, body):
    path = tmp_path / "fake-launcher"
    path.write_text("#!/bin/sh\n" + textwrap.dedent(body))
    path.chmod(0o755)
    return str(path)


def test_should_abort_kills_the_whole_process_group(tmp_path):
    pidfile = tmp_path / "grandchild.pid"
    launcher = _fake_launcher(tmp_path, f"""
        sleep 300 &
        echo $! > {pidfile}
        wait
    """)
    calls = []

    def probe():
        calls.append(1)
        # First tick: keep going. Second tick: stop, with a reason.
        return None if len(calls) < 2 else "стоп по флагу"

    t0 = time.time()
    res = core.run_claude([launcher], str(tmp_path), str(tmp_path / "out"),
                          timeout_seconds=120, poll_interval=0.2,
                          term_grace_seconds=2, kill_grace_seconds=3,
                          should_abort=probe)
    assert res["aborted"] is True
    assert res["abort_reason"] == "стоп по флагу"
    assert res["timed_out"] is False  # an abort is not a deadline
    assert time.time() - t0 < 30
    # The backgrounded grandchild shares the pgid and must be dead too —
    # a cancel that leaves orphans is the claimer's original sin (§4в 11.08).
    gpid = int(pidfile.read_text().strip())
    time.sleep(0.3)
    with pytest.raises(ProcessLookupError):
        os.kill(gpid, 0)


def test_should_abort_flag_driven_probe_end_to_end(tmp_path):
    """The intended wiring: probe = read the stop flags. A pre-raised
    stop-all aborts the run on the first poll tick."""
    ctrl = tmp_path / "ctl"
    core.write_stop_flag(ctrl, "stop_all", reason="/stop")
    launcher = _fake_launcher(tmp_path, "sleep 300\n")

    def probe():
        flag = core.stop_all_active(ctrl)
        return flag and ("stop-all: %s" % (flag.get("reason") or "?"))

    res = core.run_claude([launcher], str(tmp_path), str(tmp_path / "out"),
                          timeout_seconds=120, poll_interval=0.2,
                          term_grace_seconds=2, kill_grace_seconds=3,
                          should_abort=probe)
    assert res["aborted"] is True
    assert res["abort_reason"] == "stop-all: /stop"


def test_no_should_abort_keeps_previous_behavior(tmp_path):
    launcher = _fake_launcher(tmp_path, "echo done\nexit 0\n")
    res = core.run_claude([launcher], str(tmp_path), str(tmp_path / "out"),
                          timeout_seconds=30, poll_interval=0.2)
    assert res["rc"] == 0 and res["timed_out"] is False
    # The result still carries the (inactive) abort fields — consumers can
    # read them unconditionally.
    assert res["aborted"] is False and res["abort_reason"] is None


def test_falsy_probe_never_kills_the_run(tmp_path):
    launcher = _fake_launcher(tmp_path, "sleep 1\nexit 0\n")
    res = core.run_claude([launcher], str(tmp_path), str(tmp_path / "out"),
                          timeout_seconds=30, poll_interval=0.2,
                          should_abort=lambda: None)
    assert res["rc"] == 0 and res["aborted"] is False


def test_crashing_probe_is_not_a_stop_order(tmp_path):
    launcher = _fake_launcher(tmp_path, "sleep 1\nexit 0\n")

    def probe():
        raise RuntimeError("the probe exploded")

    res = core.run_claude([launcher], str(tmp_path), str(tmp_path / "out"),
                          timeout_seconds=30, poll_interval=0.2,
                          should_abort=probe)
    assert res["rc"] == 0 and res["aborted"] is False


def test_timeout_still_reports_timed_out_not_aborted(tmp_path):
    launcher = _fake_launcher(tmp_path, "sleep 300\n")
    res = core.run_claude([launcher], str(tmp_path), str(tmp_path / "out"),
                          timeout_seconds=1, poll_interval=0.2,
                          term_grace_seconds=2, kill_grace_seconds=3,
                          should_abort=lambda: None)
    assert res["timed_out"] is True and res["aborted"] is False


# ---------------------------------------------------------------------------
# TOOL — cancel / no_claude / resume_lane (needs the hermes-agent env)
# ---------------------------------------------------------------------------

@pytest.fixture()
def fake_kanban(tmp_path):
    """Fake hermes CLI: records one JSON argv line per call, exit 0."""
    log = tmp_path / "calls.log"
    path = tmp_path / "hermes"
    path.write_text(textwrap.dedent("""\
        #!/usr/bin/env python3
        import json, sys
        with open({log!r}, "a") as f:
            f.write(json.dumps(sys.argv[1:]) + "\\n")
        if "show" in sys.argv:
            print(json.dumps({{"task": {{"status": "running"}}}}))
    """).format(log=str(log)))
    path.chmod(0o755)
    return {"bin": str(path), "log": log}


@pytest.fixture()
def family_kanban_root(tmp_path):
    """A board with a small family: t_aaaa0001 (running) → t_bbbb0002 (ready),
    t_cccc0003 (running); t_bbbb0002 → t_dddd0004 (todo). Real schema via
    hermes_cli.kanban_db.connect (a hand-rolled subset dies in migrations —
    measured 2026-08-13)."""
    root = tmp_path / "kanban"
    board_dir = root / "boards" / "hermes-infra"
    board_dir.mkdir(parents=True)
    from hermes_cli import kanban_db as _kb
    conn = _kb.connect(db_path=board_dir / "kanban.db")
    for tid, status in (("t_aaaa0001", "running"), ("t_bbbb0002", "ready"),
                        ("t_cccc0003", "running"), ("t_dddd0004", "todo")):
        conn.execute(
            "INSERT INTO tasks (id, title, status, created_at) VALUES (?, ?, ?, 0)",
            (tid, tid, status))
    for parent, child in (("t_aaaa0001", "t_bbbb0002"),
                          ("t_aaaa0001", "t_cccc0003"),
                          ("t_bbbb0002", "t_dddd0004")):
        conn.execute(
            "INSERT INTO task_links (parent_id, child_id) VALUES (?, ?)",
            (parent, child))
    conn.commit()
    conn.close()
    return str(root)


@pytest.fixture()
def cancel_env(monkeypatch, tmp_path, fake_kanban, family_kanban_root):
    control = tmp_path / "terminal-control"
    monkeypatch.setenv("KTC_CONTROL_DIR", str(control))
    monkeypatch.setenv("HERMES_CLAUDE_CODE_KANBAN_BIN", fake_kanban["bin"])
    monkeypatch.setenv("HERMES_CLAUDE_CODE_KANBAN_ROOT", family_kanban_root)
    monkeypatch.delenv("HERMES_KANBAN_DB", raising=False)
    return control


def _calls(fake_kanban):
    try:
        lines = fake_kanban["log"].read_text().splitlines()
    except OSError:
        return []
    return [json.loads(line) for line in lines if line.strip()]


@requires_tool
def test_cancel_cascade_blocks_queue_children_not_running(
        cancel_env, fake_kanban):
    out = json.loads(cct.handle_claude_code(
        {"action": "cancel", "card_id": "t_aaaa0001", "reason": "передумали"},
        session_id="sess-1"))
    # Every family member got a durable flag…
    assert sorted(out["flagged"]) == ["t_aaaa0001", "t_bbbb0002",
                                     "t_cccc0003", "t_dddd0004"]
    for tid in out["flagged"]:
        flag = core.read_stop_flags(cancel_env, tid)["cancel"]
        assert flag and flag["reason"] == "передумали"
        assert flag["requested_by"] == "session:sess-1"
        if tid != "t_aaaa0001":
            assert flag["cascade_from"] == "t_aaaa0001"
    # …queue-state cards were blocked HERE with the [cancelled] reason…
    assert sorted(out["blocked"]) == ["t_bbbb0002", "t_dddd0004"]
    blocks = [c for c in _calls(fake_kanban) if "block" in c]
    assert sorted(c[c.index("block") + 1] for c in blocks) == \
        ["t_bbbb0002", "t_dddd0004"]
    assert all("[cancelled] передумали" in " ".join(c) for c in blocks)
    # …and RUNNING cards were NOT blocked (Batch-4: the claimer closes what it
    # claimed) — only flagged + commented.
    assert sorted(out["kill_pending"]) == ["t_aaaa0001", "t_cccc0003"]
    comments = [c for c in _calls(fake_kanban) if "comment" in c]
    assert sorted(c[c.index("comment") + 1] for c in comments) == \
        ["t_aaaa0001", "t_cccc0003"]
    assert all("остановит клеймер" in " ".join(c) for c in comments)
    assert not out.get("errors")


@requires_tool
def test_cancel_without_cascade_touches_only_the_card(cancel_env, fake_kanban):
    out = json.loads(cct.handle_claude_code(
        {"action": "cancel", "card_id": "t_bbbb0002", "cascade": False,
         "reason": "только эту"}, session_id=""))
    assert out["flagged"] == ["t_bbbb0002"]
    assert out["blocked"] == ["t_bbbb0002"]
    assert core.read_stop_flags(cancel_env, "t_dddd0004")["cancel"] is None


@requires_tool
def test_cancel_all_raises_the_lane_flag(cancel_env, fake_kanban):
    out = json.loads(cct.handle_claude_code(
        {"action": "cancel", "all": True, "reason": "стоп всё"},
        session_id="sess-9"))
    assert out["cancelled"] == "all"
    flag = core.stop_all_active(cancel_env)
    assert flag and flag["reason"] == "стоп всё"
    assert flag["requested_by"] == "session:sess-9"
    # No cards were touched: the flag IS the stop.
    assert out["blocked"] == [] and out["kill_pending"] == []
    assert _calls(fake_kanban) == []


@requires_tool
def test_cancel_needs_a_card_or_all(cancel_env):
    out = json.loads(cct.handle_claude_code({"action": "cancel"}, session_id=""))
    assert out.get("error_type") == "claude_code_bad_card"


@requires_tool
def test_no_claude_flags_and_reassigns_cascade(cancel_env, fake_kanban):
    out = json.loads(cct.handle_claude_code(
        {"action": "no_claude", "card_id": "t_aaaa0001"}, session_id="sess-2"))
    assert sorted(out["flagged"]) == ["t_aaaa0001", "t_bbbb0002",
                                     "t_cccc0003", "t_dddd0004"]
    assert sorted(out["reassigned"]) == sorted(out["flagged"])
    for tid in out["flagged"]:
        assert core.read_stop_flags(cancel_env, tid)["no_claude"] is not None
    assigns = [c for c in _calls(fake_kanban) if "assign" in c]
    assert sorted(c[c.index("assign") + 1] for c in assigns) == \
        sorted(out["flagged"])
    assert all(c[-1] == "default" for c in assigns)


@requires_tool
def test_no_claude_reports_failed_reassign_honestly(
        cancel_env, fake_kanban, monkeypatch):
    class _Failed:
        returncode = 1
        stdout = ""
        stderr = "assign exploded"

    monkeypatch.setattr(cct, "_kanban", lambda *a, **k: _Failed())
    out = json.loads(cct.handle_claude_code(
        {"action": "no_claude", "card_id": "t_bbbb0002", "cascade": False},
        session_id=""))
    # The flag is up (durable half succeeded) but the reassign result is not
    # invented: rc=1 lands in errors, reassigned stays empty.
    assert out["flagged"] == ["t_bbbb0002"] and out["reassigned"] == []
    assert "assign rc=1" in out["errors"]["t_bbbb0002"]


@requires_tool
def test_resume_lane_action(cancel_env):
    core.write_stop_flag(cancel_env, "stop_all", reason="/stop")
    out = json.loads(cct.handle_claude_code({"action": "resume_lane"},
                                            session_id=""))
    assert out["resumed"] is True
    assert core.stop_all_active(cancel_env) is None
    again = json.loads(cct.handle_claude_code({"action": "resume_lane"},
                                              session_id=""))
    assert again["resumed"] is False


@requires_tool
def test_status_shows_active_flags(cancel_env, fake_kanban):
    core.write_stop_flag(cancel_env, "cancel", card="t_aaaa0001",
                         reason="передумали")
    out = json.loads(cct.handle_claude_code(
        {"action": "status", "card_id": "t_aaaa0001"}, session_id=""))
    assert out["stop_flags"]["cancel"]["reason"] == "передумали"


@requires_tool
def test_unknown_action_names_all_verbs(cancel_env):
    out = json.loads(cct.handle_claude_code({"action": "explode"},
                                            session_id=""))
    assert out.get("error_type") == "claude_code_bad_action"
    for verb in ("run", "status", "cancel", "no_claude", "resume_lane"):
        assert verb in out.get("error", "")
