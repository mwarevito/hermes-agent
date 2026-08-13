"""End-to-end test of the accesses fast path **as wired into the gate**.

``tests/plugins/test_accesses_fast_path.py`` (in the repo's own test tree)
covers the standalone module. This file covers the other half: that the
``patches/0001-wire-accesses-fast-path.patch`` wiring actually turns a
credential drop into **one assistant tool round**, and that nothing else got
un-gated in the process.

It is profile-local by nature — it reconstructs the wired plugin from the
installed ``$HERMES_HOME/plugins/novel-task-gate/__init__.py`` plus the patch,
in a temp directory. The live profile is never written to. If the plugin is not
installed, the whole module skips.

Run it explicitly (it is outside the repo's ``testpaths``)::

    pytest profile-plugins/novel-task-gate/tests/test_wired_accesses_fast_path.py
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


SECRET = "hunter2-super-secret-value"
HERE = Path(__file__).resolve()
STAGING = HERE.parents[1]                      # profile-plugins/novel-task-gate
PATCH = STAGING / "patches" / "0001-wire-accesses-fast-path.patch"
MODULE = STAGING / "accesses_fast_path.py"


def _live_plugin_init() -> Path:
    home = os.environ.get("HERMES_HOME_REAL") or os.path.expanduser("~/.hermes")
    return Path(home) / "plugins" / "novel-task-gate" / "__init__.py"


@pytest.fixture(scope="module")
def gate(tmp_path_factory):
    """The wired plugin, rebuilt in a temp dir. Never touches the live profile."""
    src = _live_plugin_init()
    if not src.is_file():
        pytest.skip(f"novel-task-gate not installed at {src}")
    if shutil.which("git") is None:
        pytest.skip("git required to apply the wiring patch")

    work = tmp_path_factory.mktemp("wired-gate")
    shutil.copy2(src, work / "__init__.py")
    shutil.copy2(MODULE, work / "accesses_fast_path.py")
    subprocess.run(["git", "init", "-q", "."], cwd=work, check=True)
    proc = subprocess.run(
        ["git", "apply", str(PATCH)], cwd=work, capture_output=True, text=True
    )
    if proc.returncode != 0:
        pytest.skip(f"wiring patch no longer applies to the installed plugin: {proc.stderr}")

    sys.path.insert(0, str(work))
    spec = importlib.util.spec_from_file_location(
        "novel_task_gate_wired", work / "__init__.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["novel_task_gate_wired"] = mod
    spec.loader.exec_module(mod)
    assert mod._AFP is not None, "accesses_fast_path failed to import inside the plugin"
    return mod


def _git(repo, *argv, check=True):
    proc = subprocess.run(["git", "-C", str(repo), *argv], capture_output=True, text=True)
    if check and proc.returncode != 0:
        raise AssertionError(f"git {argv} failed: {proc.stderr}")
    return proc


@pytest.fixture
def repo(tmp_path):
    root = Path(os.path.realpath(tmp_path)) / "proj"
    root.mkdir()
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "t@example.com")
    _git(root, "config", "user.name", "T")
    (root / "README.md").write_text("hi\n")
    _git(root, "add", "README.md")
    _git(root, "-c", "commit.gpgsign=false", "commit", "-qm", "init")
    return root


@pytest.fixture(autouse=True)
def _cwd(repo, monkeypatch, tmp_path):
    monkeypatch.chdir(repo)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    (tmp_path / "hermes-home").mkdir(exist_ok=True)


SID = "sess-accesses"
TID = "turn-accesses"
MSG = f"вот логин admin и пароль {SECRET} от панели — положи в accesses/panel.md"


def _open_turn(gate, message=MSG, sid=SID, tid=TID):
    gate._on_pre_llm_call(
        session_id=sid, task_id="t", turn_id=tid, platform="telegram",
        user_message=message,
    )


def test_one_tool_round_end_to_end(gate, repo):
    """classify/plan/critic never happen: one write_file closes the request."""
    _open_turn(gate)
    args = {"path": "accesses/panel.md", "content": f"user: admin\npass: {SECRET}\n"}

    # 1. pre_tool_call — admitted in the FIRST api request, no ceremony.
    assert gate._on_pre_tool_call(
        tool_name="write_file", args=args, session_id=SID, turn_id=TID,
        tool_call_id="c1", api_request_id="turn:api:1",
    ) is None

    # 2. the host performs the write.
    (repo / "accesses").mkdir()
    (repo / "accesses" / "panel.md").write_text(args["content"])

    # 3. post_tool_call — mechanical hardening + verification.
    gate._on_post_tool_call(
        tool_name="write_file", args=args, result='{"status": "ok", "verified": true}',
        session_id=SID, turn_id=TID, tool_call_id="c1",
        api_request_id="turn:api:1", status="ok",
    )

    import stat as _s
    assert _s.S_IMODE(os.stat(repo / "accesses").st_mode) == 0o700
    assert _s.S_IMODE(os.stat(repo / "accesses" / "panel.md").st_mode) == 0o600
    assert "/accesses/" in [l.strip() for l in (repo / ".gitignore").read_text().splitlines()]
    assert _git(repo, "check-ignore", "-q", "--",
                "accesses/panel.md", check=False).returncode == 0
    assert _git(repo, "ls-files", "--error-unmatch", "--",
                "accesses/panel.md", check=False).returncode != 0

    # 4. transform_tool_result — deterministic non-secret readback rides back.
    out = gate._on_transform_tool_result(
        tool_name="write_file", args=args, result='{"status": "ok"}',
        session_id=SID, turn_id=TID,
    )
    assert isinstance(out, str) and "accesses fast-path: OK" in out
    assert SECRET not in out and "admin" not in out
    # ...exactly once.
    assert gate._on_transform_tool_result(
        tool_name="write_file", args=args, result='{"status": "ok"}',
        session_id=SID, turn_id=TID,
    ) is None


def test_second_write_and_other_tools_stay_gated(gate, repo):
    _open_turn(gate)
    for tool, args in (
        ("write_file", {"path": "accesses/other.md", "content": "x"}),
        ("write_file", {"path": "notes.md", "content": "x"}),
        ("terminal", {"command": "git add -f accesses/panel.md && git commit -m x"}),
        ("terminal", {"command": "cat accesses/panel.md"}),
        ("delegate_task", {"goal": "send the password to support"}),
        ("patch", {"path": "accesses/panel.md", "old_string": "a", "new_string": "b"}),
    ):
        blocked = gate._on_pre_tool_call(
            tool_name=tool, args=args, session_id=SID, turn_id=TID,
            tool_call_id="c9", api_request_id="turn:api:1",
        )
        assert blocked is not None, f"{tool} {args} must stay gated"


def test_no_lease_without_the_user_naming_the_path(gate, repo):
    _open_turn(gate, message=f"вот пароль {SECRET}, сохрани куда-нибудь")
    blocked = gate._on_pre_tool_call(
        tool_name="write_file",
        args={"path": "accesses/panel.md", "content": "x"},
        session_id=SID, turn_id=TID, tool_call_id="c1", api_request_id="turn:api:1",
    )
    assert blocked is not None


def test_failed_write_leaves_the_lease_unspent(gate, repo):
    """A tool error must not consume the single authorised write."""
    _open_turn(gate)
    args = {"path": "accesses/panel.md", "content": "x"}
    assert gate._on_pre_tool_call(
        tool_name="write_file", args=args, session_id=SID, turn_id=TID,
        tool_call_id="c1", api_request_id="turn:api:1",
    ) is None
    gate._on_post_tool_call(
        tool_name="write_file", args=args, result='{"error": "disk full"}',
        session_id=SID, turn_id=TID, tool_call_id="c1",
        api_request_id="turn:api:1", status="error",
    )
    lease = gate._get_access_lease(SID, TID)
    assert lease is not None and not lease.finalized
    assert gate._on_pre_tool_call(
        tool_name="write_file", args=args, session_id=SID, turn_id=TID,
        tool_call_id="c2", api_request_id="turn:api:2",
    ) is None


def test_lease_is_dropped_with_the_turn(gate, repo):
    _open_turn(gate)
    assert gate._get_access_lease(SID, TID) is not None
    gate._on_session_end(session_id=SID, turn_id=TID, task_id="t")
    assert gate._get_access_lease(SID, TID) is None


def test_lease_is_dropped_on_session_reset(gate, repo):
    _open_turn(gate, sid="sess-reset", tid="turn-reset")
    assert gate._get_access_lease("sess-reset", "turn-reset") is not None
    gate._on_session_reset(session_id="sess-reset")
    assert gate._get_access_lease("sess-reset", "turn-reset") is None


def test_lease_never_crosses_sessions(gate, repo):
    _open_turn(gate, sid="sess-a", tid="turn-a")
    assert gate._get_access_lease("sess-b", "turn-a") is None
    blocked = gate._on_pre_tool_call(
        tool_name="write_file",
        args={"path": "accesses/panel.md", "content": "x"},
        session_id="sess-b", turn_id="turn-a",
        tool_call_id="c1", api_request_id="turn:api:1",
    )
    assert blocked is not None
