"""Acceptance for tools/claude_code_core.py — the single Claude Code launch
contract (extracted 2026-08-12 from the Givi runner; canon: hermes-ops
infra/false-completion-20260811.md, порядок работ п.8/п.9).

These tests are the patch's acceptance file: hermes-patch-verify runs it
whole. They pin the CONTRACT, not the implementation: argv shape, marker
gating, kill semantics, tail-only verdict scan, py3.9/stdlib import hygiene.
"""
import ast
import json
import os
import signal
import stat
import subprocess
import sys
import textwrap
import time

import pytest

import tools.claude_code_core as core


# ---------------------------------------------------------------------------
# argv building / session ids
# ---------------------------------------------------------------------------

def test_build_argv_minimal_shape_and_terminator():
    argv = core.build_argv("/x/launcher", "do things")
    assert argv[0] == "/x/launcher"
    assert argv[1] == "-p"
    assert argv[-2:] == ["--", "do things"]
    assert "--output-format" in argv and argv[argv.index("--output-format") + 1] == "text"


def test_build_argv_prompt_looking_like_flag_stays_behind_terminator():
    argv = core.build_argv("/x/l", "--dangerously-skip-permissions")
    # Everything after `--` is prompt text, never a flag.
    assert argv[-1] == "--dangerously-skip-permissions"
    assert argv[argv.index("--") + 1] == argv[-1]
    assert argv.count("--") == 1


def test_build_argv_full_options():
    sid = core.mint_claude_session_id()
    argv = core.build_argv(
        "/x/l", "p", allowed_tools=["Read", "Edit"],
        append_system_prompt="sys", model="opus", effort="high",
        claude_session_id=sid, max_budget_usd=2.5,
    )
    assert argv[argv.index("--allowedTools") + 1] == "Read,Edit"
    assert argv[argv.index("--append-system-prompt") + 1] == "sys"
    assert argv[argv.index("--model") + 1] == "opus"
    assert argv[argv.index("--effort") + 1] == "high"
    assert argv[argv.index("--session-id") + 1] == sid
    assert argv[argv.index("--max-budget-usd") + 1] == "2.5"
    # options end before the terminator
    assert argv.index("--max-budget-usd") < argv.index("--")


def test_build_argv_resume_and_fork():
    sid = core.mint_claude_session_id()
    argv = core.build_argv("/x/l", "p", resume_session_id=sid, fork_session=True)
    assert argv[argv.index("--resume") + 1] == sid
    assert "--fork-session" in argv
    assert "--session-id" not in argv


def test_build_argv_session_and_resume_are_mutually_exclusive():
    a, b = core.mint_claude_session_id(), core.mint_claude_session_id()
    with pytest.raises(ValueError):
        core.build_argv("/x/l", "p", claude_session_id=a, resume_session_id=b)


def test_build_argv_rejects_non_uuid_session_ids():
    with pytest.raises(ValueError):
        core.build_argv("/x/l", "p", claude_session_id="$(rm -rf /)")
    with pytest.raises(ValueError):
        core.build_argv("/x/l", "p", resume_session_id="not-a-uuid")


def test_build_argv_fork_requires_resume():
    with pytest.raises(ValueError):
        core.build_argv("/x/l", "p", fork_session=True)


def test_build_argv_budget_formatting_and_bounds():
    assert core.build_argv("/x/l", "p", max_budget_usd=5)[-4:-2] == \
        ["--max-budget-usd", "5"]
    assert "0.5" == core.build_argv("/x/l", "p", max_budget_usd=0.5)[
        core.build_argv("/x/l", "p", max_budget_usd=0.5).index("--max-budget-usd") + 1]
    for bad in (0, -1, 1001, "ten"):
        with pytest.raises(ValueError):
            core.build_argv("/x/l", "p", max_budget_usd=bad)


def test_parse_iso_ts_naive_is_local_time():
    """gateway_routing rows are naive-LOCAL; forcing UTC shifts every
    notify-inference window by the host's UTC offset (caught 12.08)."""
    from datetime import datetime as dt
    naive = "2026-08-12T17:26:17.598010"
    assert core.parse_iso_ts(naive) == dt.fromisoformat(naive).timestamp()
    assert core.parse_iso_ts("2026-08-12T17:26:17Z") == \
        core.parse_iso_ts("2026-08-12T17:26:17+00:00")
    assert core.parse_iso_ts("garbage") is None
    assert core.parse_iso_ts(None) is None
    assert core.parse_iso_ts(123) is None


def test_mint_and_recognize_session_ids():
    sid = core.mint_claude_session_id()
    assert core.is_claude_session_id(sid)
    assert not core.is_claude_session_id("")
    assert not core.is_claude_session_id(None)
    assert not core.is_claude_session_id("zzzz-not-a-uuid")


# ---------------------------------------------------------------------------
# resume_decision
# ---------------------------------------------------------------------------

def _prev(status="timeout", sid=True, resumed_from=None):
    d = {"status": status}
    if sid:
        d["claude_session_id"] = core.mint_claude_session_id()
    if resumed_from:
        d["resumed_from"] = resumed_from
    return d


def test_resume_only_after_timeout_with_live_workspace():
    d = core.resume_decision(_prev(), workspace_exists=True)
    assert d["resume"] is True and core.is_claude_session_id(d["claude_session_id"])


@pytest.mark.parametrize("prev,ws", [
    (_prev(sid=False), True),                 # no session id recorded
    (_prev(status="error"), True),            # error is not resumable
    (_prev(status="ok"), True),               # nothing to resume
    (_prev(), False),                         # workspace gone
    (_prev(resumed_from="job-1"), True),      # max one resume per chain
    (None, True),                             # no previous result at all
])
def test_resume_refused(prev, ws):
    d = core.resume_decision(prev, workspace_exists=ws)
    assert d["resume"] is False
    assert d["claude_session_id"] is None
    assert d["reason"]  # refusals always carry a reason


# ---------------------------------------------------------------------------
# verdict — evidence over exit codes
# ---------------------------------------------------------------------------

def _outdir(tmp_path, out=b"", err=b""):
    d = tmp_path / "done"
    d.mkdir(exist_ok=True)
    (d / "output.txt").write_bytes(out)
    (d / "stderr.txt").write_bytes(err)
    return str(d)


def test_verdict_ok_on_clean_output(tmp_path):
    assert core.verdict("ok", _outdir(tmp_path, b"all done\n")) == "ok"


def test_verdict_failed_when_status_not_ok(tmp_path):
    assert core.verdict("error", _outdir(tmp_path, b"fine")) == "failed"
    assert core.verdict("timeout", _outdir(tmp_path, b"fine")) == "failed"


def test_verdict_suspect_on_fatal_wearing_success_clothes(tmp_path):
    # The 2026-08-08 case verbatim: exit 0 around a missing key.
    out = _outdir(tmp_path, b"FATAL: TEST_ANTHROPIC_API_KEY is not set\n")
    assert core.verdict("ok", out) == "suspect"


def test_verdict_unknown_when_output_unreadable(tmp_path):
    assert core.verdict("ok", None) == "unknown"
    assert core.verdict("ok", str(tmp_path / "nope")) == "unknown"


def test_verdict_scans_tail_not_head(tmp_path):
    pad = b"x" * (core.VERDICT_SCAN_BYTES + 8192)
    tail_marker = _outdir(tmp_path, pad + b"\nTraceback (most recent call last)\n")
    assert core.verdict("ok", tail_marker) == "suspect"
    head_marker = _outdir(tmp_path, b"FATAL: boom\n" + pad)
    assert core.verdict("ok", head_marker) == "ok"  # tail-only by design


# ---------------------------------------------------------------------------
# lane-busy detection
# ---------------------------------------------------------------------------

def test_busy_needs_both_rc75_and_marker(tmp_path):
    with_marker = _outdir(tmp_path, b"", b"Claude Code lane still busy after 120s; giving up.\n")
    assert core.launcher_was_busy(75, with_marker) is True
    assert core.launcher_was_busy(0, with_marker) is False
    assert core.launcher_was_busy(1, with_marker) is False
    bare = _outdir(tmp_path, b"claude exited 75 for its own reasons", b"")
    assert core.launcher_was_busy(75, bare) is False


def test_busy_marker_on_either_stream_is_enough(tmp_path):
    stdout_only = _outdir(tmp_path, b"CLAUDE_CODE_LANE_BUSY: still busy\n", b"")
    assert core.launcher_was_busy(75, stdout_only) is True


def test_busy_missing_outdir_is_not_busy_and_does_not_raise(tmp_path):
    assert core.launcher_was_busy(75, str(tmp_path / "missing")) is False


def test_busy_in_text_variant():
    assert core.lane_busy_in_text(75, "", "…lane still busy…") is True
    assert core.lane_busy_in_text(75, "CLAUDE_CODE_LANE_BUSY: x", None) is True
    assert core.lane_busy_in_text(75, "quiet", "quiet") is False
    assert core.lane_busy_in_text(0, "CLAUDE_CODE_LANE_BUSY: x", "") is False


# ---------------------------------------------------------------------------
# lane slot layout + probe
# ---------------------------------------------------------------------------

def test_lane_slot_paths_hermes_scheme():
    assert core.lane_slot_paths("/t/l", 1) == ["/t/l"]
    assert core.lane_slot_paths("/t/l", 2) == ["/t/l", "/t/l.2"]
    assert core.lane_slot_paths("/t/l", 3) == ["/t/l", "/t/l.2", "/t/l.3"]


def test_lane_slot_paths_givi_scheme_differs():
    assert core.lane_slot_paths("/t/l", 1, scheme="givi") == ["/t/l"]
    assert core.lane_slot_paths("/t/l", 2, scheme="givi") == ["/t/l.1", "/t/l.2"]


def test_lane_slot_paths_unknown_scheme():
    with pytest.raises(ValueError):
        core.lane_slot_paths("/t/l", 2, scheme="wat")


def test_lane_free_probe(tmp_path):
    base = str(tmp_path / "lock")
    assert core.lane_free(base, 2) is True          # nothing exists
    os.makedirs(base)
    (tmp_path / "lock" / "pid").write_text(str(os.getpid()))
    assert core.lane_free(base, 2) is True          # slot 2 still free
    slot2 = tmp_path / "lock.2"
    slot2.mkdir()
    (slot2 / "pid").write_text(str(os.getpid()))
    assert core.lane_free(base, 2) is False         # both held by a live pid
    (slot2 / "pid").write_text("999999999")         # dead holder → reusable
    assert core.lane_free(base, 2) is True


def test_lane_free_probe_creates_nothing(tmp_path):
    base = str(tmp_path / "lock")
    core.lane_free(base, 2)
    assert not os.path.exists(base) and not os.path.exists(base + ".2")


# ---------------------------------------------------------------------------
# workspaces
# ---------------------------------------------------------------------------

def _git(repo, *args):
    return subprocess.run(["/usr/bin/git", "-C", repo, *args],
                          capture_output=True, text=True, timeout=60)


@pytest.fixture()
def tiny_repo(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["/usr/bin/git", "init", "-q", str(repo)],
                   capture_output=True, timeout=60)
    _git(str(repo), "config", "user.email", "t@t")
    _git(str(repo), "config", "user.name", "t")
    (repo / "a.txt").write_text("a\n")
    _git(str(repo), "add", ".")
    _git(str(repo), "commit", "-qm", "init")
    return str(repo)


def test_carry_untracked_env_copies_with_owner_only_perms(tmp_path, tiny_repo):
    (tmp_path / "repo" / ".env").write_text("SECRET=1\n")
    ws = tmp_path / "ws"
    ws.mkdir()
    msgs = []
    core.carry_untracked_env(tiny_repo, str(ws), log_fn=msgs.append)
    dst = ws / ".env"
    assert dst.read_text() == "SECRET=1\n"
    assert stat.S_IMODE(os.stat(dst).st_mode) == 0o600
    assert not os.path.islink(dst)  # copy, never a symlink back to the repo
    assert any("carried" in m for m in msgs)


def test_carry_untracked_env_never_clobbers_and_is_silent_on_absence(tmp_path, tiny_repo):
    ws = tmp_path / "ws"
    ws.mkdir()
    (ws / ".env").write_text("MINE=1\n")
    core.carry_untracked_env(tiny_repo, str(ws))          # no repo .env: silent
    (tmp_path / "repo" / ".env").write_text("THEIRS=1\n")
    core.carry_untracked_env(tiny_repo, str(ws))
    assert (ws / ".env").read_text() == "MINE=1\n"        # existing dst wins


def test_prepare_workspace_scratch(tmp_path):
    ws, branch, base = core.prepare_workspace(
        "j1", "scratch", scratch_root=str(tmp_path / "scratch"))
    assert os.path.isdir(ws) and branch is None and base is None


def test_prepare_workspace_worktree_and_reuse(tmp_path, tiny_repo):
    roots = dict(worktrees_root=str(tmp_path / "wt"), ws_prefix="cc-",
                 branch_prefix="cc/")
    ws, branch, base = core.prepare_workspace("j2", "worktree", repo=tiny_repo,
                                              **roots)
    assert os.path.isdir(ws) and branch == "cc/j2"
    assert base  # no origin → falls back to local HEAD and says so
    # second call: reuse, not failure
    ws2, branch2, base2 = core.prepare_workspace("j2", "worktree",
                                                 repo=tiny_repo, **roots)
    assert ws2 == ws and base2 == "(reused worktree)"


def test_prepare_workspace_rejects_impostor_dir(tmp_path, tiny_repo):
    roots = dict(worktrees_root=str(tmp_path / "wt"))
    os.makedirs(os.path.join(roots["worktrees_root"], "cc-j3"))
    with pytest.raises(RuntimeError):
        core.prepare_workspace("j3", "worktree", repo=tiny_repo, **roots)


def test_capture_diff_sees_new_files(tmp_path, tiny_repo):
    ws, _, _ = core.prepare_workspace("j4", "worktree", repo=tiny_repo,
                                      worktrees_root=str(tmp_path / "wt"))
    with open(os.path.join(ws, "new.txt"), "w") as f:
        f.write("hello\n")
    out = tmp_path / "out"
    out.mkdir()
    status = core.capture_diff(ws, str(out))
    patch = (out / "diff.patch").read_bytes()
    assert b"new.txt" in patch and "new.txt" in status


def test_capture_diff_never_raises_outside_a_repo(tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    core.capture_diff(str(tmp_path), str(out))  # not a repo — must not raise
    assert (out / "diff.patch").exists()


# ---------------------------------------------------------------------------
# validate_job
# ---------------------------------------------------------------------------

GIVI_LIKE_POLICY = {
    "allowed_repo_roots": (),  # filled per-test
    "denied_repo_substrings": ("/.hermes/", "/hermes-agent/"),
    "roots_reason": "repo must live under ~/coding or ~/givi-workspace",
}


def test_validate_minimal_scratch_defaults():
    norm, err = core.validate_job({"prompt": "do"}, {})
    assert err is None
    assert norm["mode"] == "scratch"
    assert norm["timeout_seconds"] == 1800
    assert norm["allowed_tools"] == list(core.DEFAULT_POLICY["default_tools"])


@pytest.mark.parametrize("job,frag", [
    ("notadict", "must be an object"),
    ({"prompt": ""}, "non-empty"),
    ({"prompt": "x\x00y"}, "NUL"),
    ({"prompt": "x", "mode": "weird"}, "mode"),
    ({"prompt": "x", "timeout_seconds": 10}, "timeout_seconds"),
    ({"prompt": "x", "timeout_seconds": "600"}, "timeout_seconds"),
    ({"prompt": "x", "allowed_tools": ["Sudo"]}, "allowed_tools"),
    ({"prompt": "x", "model": "Bad Model!"}, "model failed"),
    ({"prompt": "x", "effort": "ultra"}, "effort"),
    ({"prompt": "x", "notify": {"thread_id": "5"}}, "notify"),
    ({"prompt": "x", "notify": {"chat_id": "abc"}}, "chat_id"),
])
def test_validate_rejections(job, frag):
    norm, err = core.validate_job(job, {})
    assert norm is None and frag in err


def test_validate_prompt_size_cap():
    norm, err = core.validate_job({"prompt": "я" * 60_000}, {})  # 120K utf-8
    assert norm is None and "utf-8 bytes" in err


def test_validate_deny_before_exists(tmp_path):
    """A denied path must be refused as DENIED even when it does not exist —
    the deny answer must not leak path-existence information and must not
    depend on it (pinned ordering from the Givi runner's tests)."""
    pol = dict(GIVI_LIKE_POLICY)
    pol["allowed_repo_roots"] = (str(tmp_path) + os.sep,)
    ghost = str(tmp_path / ".hermes" / "nope")
    norm, err = core.validate_job(
        {"prompt": "x", "mode": "worktree", "repo": ghost}, pol)
    assert norm is None
    assert "denied" in err and "policy source" in err and "may not edit" in err
    assert "does not exist" not in err


def test_validate_worktree_happy_path(tmp_path):
    repo = tmp_path / "r"
    repo.mkdir()
    subprocess.run(["/usr/bin/git", "init", "-q", str(repo)],
                   capture_output=True, timeout=60)
    pol = {"allowed_repo_roots": (str(tmp_path) + os.sep,)}
    norm, err = core.validate_job(
        {"prompt": "x", "mode": "worktree", "repo": str(repo)}, pol)
    assert err is None and norm["repo"] == os.path.realpath(str(repo))


def test_validate_budget_and_resume_are_opt_in():
    sid = core.mint_claude_session_id()
    job = {"prompt": "x", "max_budget_usd": 3,
           "resume_claude_session_id": sid}
    norm, err = core.validate_job(job, {})           # flags off → ignored
    assert err is None
    assert "max_budget_usd" not in norm and "resume_claude_session_id" not in norm
    pol = {"allow_budget": True, "allow_resume": True}
    norm, err = core.validate_job(job, pol)
    assert err is None
    assert norm["max_budget_usd"] == 3.0
    assert norm["resume_claude_session_id"] == sid
    bad, err2 = core.validate_job({"prompt": "x", "max_budget_usd": True}, pol)
    assert bad is None and "max_budget_usd" in err2
    bad, err3 = core.validate_job(
        {"prompt": "x", "resume_claude_session_id": "nope"}, pol)
    assert bad is None and "resume_claude_session_id" in err3


# ---------------------------------------------------------------------------
# run_claude — the launch itself (real processes, tight timeouts)
# ---------------------------------------------------------------------------

def _fake_launcher(tmp_path, body):
    path = tmp_path / "fake-launcher"
    path.write_text("#!/bin/sh\n" + textwrap.dedent(body))
    path.chmod(0o755)
    return str(path)


def test_run_claude_happy_path_streams_to_files(tmp_path):
    launcher = _fake_launcher(tmp_path, """
        echo "work done"
        echo "warning" >&2
        exit 3
    """)
    out = str(tmp_path / "out")
    res = core.run_claude([launcher], str(tmp_path), out, timeout_seconds=30)
    assert res["rc"] == 3
    assert res["timed_out"] is False and res["kill_failed"] is False
    with open(res["out_path"]) as f:
        assert "work done" in f.read()
    with open(res["err_path"]) as f:
        assert "warning" in f.read()
    assert res["started_at"] and res["duration_seconds"] < 30


def test_run_claude_timeout_kills_the_whole_process_group(tmp_path):
    pidfile = tmp_path / "grandchild.pid"
    launcher = _fake_launcher(tmp_path, f"""
        sleep 300 &
        echo $! > {pidfile}
        wait
    """)
    out = str(tmp_path / "out")
    t0 = time.time()
    res = core.run_claude([launcher], str(tmp_path), out, timeout_seconds=1,
                          poll_interval=0.2, term_grace_seconds=2,
                          kill_grace_seconds=3)
    assert res["timed_out"] is True
    assert time.time() - t0 < 30
    # the backgrounded grandchild shares the pgid and must be dead too —
    # this is exactly what the claimer's subprocess.run(kill) never did
    gpid = int(pidfile.read_text().strip())
    time.sleep(0.3)
    with pytest.raises(ProcessLookupError):
        os.kill(gpid, 0)


def test_run_claude_poll_cb_ticks_and_may_not_kill_the_run(tmp_path):
    launcher = _fake_launcher(tmp_path, "sleep 1\nexit 0\n")
    ticks = []

    def cb(elapsed):
        ticks.append(elapsed)
        raise RuntimeError("status callback exploded")

    res = core.run_claude([launcher], str(tmp_path), str(tmp_path / "o"),
                          timeout_seconds=30, poll_interval=0.2, poll_cb=cb)
    assert res["rc"] == 0 and res["timed_out"] is False
    assert ticks  # ticked at least once despite raising every time


def test_run_claude_rlimit_caps_runaway_output(tmp_path):
    launcher = _fake_launcher(tmp_path, """
        yes 0123456789 2>/dev/null | head -c 1000000
        exit 0
    """)
    out = str(tmp_path / "out")
    res = core.run_claude([launcher], str(tmp_path), out, timeout_seconds=30,
                          max_file_bytes=4096)
    assert os.path.getsize(res["out_path"]) <= 4096


def test_write_result_json_guarantees_output_txt(tmp_path):
    out = str(tmp_path / "d")
    path = core.write_result_json(out, {"status": "ok"})
    assert json.load(open(path))["status"] == "ok"
    assert os.path.exists(os.path.join(out, "output.txt"))


# ---------------------------------------------------------------------------
# import hygiene — the contract that keeps system-python consumers alive
# ---------------------------------------------------------------------------

_STDLIB_OK = {
    "json", "os", "re", "shutil", "signal", "subprocess", "time", "uuid",
    "datetime", "__future__",
}


def _core_module_ast():
    with open(core.__file__.rstrip("c")) as f:
        return ast.parse(f.read())


def test_core_imports_are_stdlib_only():
    tree = _core_module_ast()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                assert a.name.split(".")[0] in _STDLIB_OK | {"resource"}, a.name
        elif isinstance(node, ast.ImportFrom):
            assert (node.module or "").split(".")[0] in _STDLIB_OK, node.module


def test_core_reads_no_environment_at_module_level():
    tree = _core_module_ast()
    for node in tree.body:  # top level only — functions may take env-derived args
        for sub in ast.walk(node):
            if isinstance(sub, ast.Attribute) and sub.attr in ("environ", "getenv"):
                in_def = isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef,
                                           ast.ClassDef))
                assert in_def, "os.environ read at import time"


def test_core_importable_by_system_python3():
    """The claimer and the Givi runner run under /usr/bin/python3 (3.9)."""
    py = "/usr/bin/python3"
    if not os.path.exists(py):
        pytest.skip("no system python3")
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(core.__file__)))
    r = subprocess.run(
        [py, "-c",
         "import sys; sys.path.insert(0, %r); "
         "import tools.claude_code_core as c; print(c.CORE_VERSION)"
         % repo_root],
        capture_output=True, text=True, timeout=60,
        env={"PATH": "/usr/bin:/bin"},
    )
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == core.CORE_VERSION
