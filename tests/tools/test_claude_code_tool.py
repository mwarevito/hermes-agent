"""Acceptance for tools/claude_code_tool.py — the typed claude_code tool.

Pins the exposure gate (hidden without an explicit opt-in — the live repo is
shared by all seven gateways), the card wire format, the honest session_wake
flag (an UPDATE that matched nothing must not read as a stamped wake-up), and
the status view over file-backed run artifacts.
"""
import json
import os
import sqlite3
import textwrap

import pytest

import tools.claude_code_core as core
import tools.claude_code_tool as cct
from tools.registry import registry


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def launcher(tmp_path):
    path = tmp_path / "claude-hermes"
    path.write_text("#!/bin/sh\nexit 0\n")
    path.chmod(0o755)
    return str(path)


@pytest.fixture()
def fake_kanban(tmp_path):
    """Fake hermes CLI: appends one JSON argv line per call, answers
    create/show. JSON-per-line because a real --body carries newlines."""
    log = tmp_path / "calls.log"
    show_json = tmp_path / "show.json"
    show_json.write_text(json.dumps(
        {"task": {"status": "done", "assignee": "terminal",
                  "result": "ok", "updated_at": "2026-08-12"}}))
    path = tmp_path / "hermes"
    path.write_text(textwrap.dedent("""\
        #!/usr/bin/env python3
        import json, sys
        with open({log!r}, "a") as f:
            f.write(json.dumps(sys.argv[1:]) + "\\n")
        if "create" in sys.argv:
            print("Created t_deadbeef")
        elif "show" in sys.argv:
            print(open({show!r}).read())
    """).format(log=str(log), show=str(show_json)))
    path.chmod(0o755)
    return {"bin": str(path), "log": log}


@pytest.fixture()
def kanban_root(tmp_path):
    root = tmp_path / "kanban"
    board_dir = root / "boards" / "hermes-infra"
    board_dir.mkdir(parents=True)
    db = board_dir / "kanban.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE tasks (id TEXT PRIMARY KEY, session_id TEXT)")
    conn.execute("INSERT INTO tasks (id) VALUES ('t_deadbeef')")
    conn.commit()
    conn.close()
    return str(root)


@pytest.fixture()
def state_db(tmp_path):
    db = tmp_path / "state.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE gateway_routing (scope TEXT NOT NULL DEFAULT '',"
                 " session_key TEXT NOT NULL, entry_json TEXT NOT NULL,"
                 " updated_at REAL NOT NULL, PRIMARY KEY (scope, session_key))")
    entry = {
        "session_id": "sess-123",
        "origin": {"chat_id": "405154434", "chat_type": "dm",
                   "thread_id": None},
    }
    conn.execute("INSERT INTO gateway_routing VALUES ('', 'k1', ?, 1.0)",
                 (json.dumps(entry),))
    conn.commit()
    conn.close()
    return str(db)


@pytest.fixture()
def env(monkeypatch, launcher, fake_kanban, kanban_root, state_db):
    monkeypatch.setenv("HERMES_CLAUDE_CODE_TOOL", "1")
    monkeypatch.setenv("HERMES_CLAUDE_CODE_LAUNCHER", launcher)
    monkeypatch.setenv("HERMES_CLAUDE_CODE_KANBAN_BIN", fake_kanban["bin"])
    monkeypatch.setenv("HERMES_CLAUDE_CODE_KANBAN_ROOT", kanban_root)
    monkeypatch.setenv("HERMES_CLAUDE_CODE_STATE_DB", state_db)
    return fake_kanban


def _calls(fake_kanban):
    """Recorded CLI invocations as argv lists."""
    try:
        lines = fake_kanban["log"].read_text().splitlines()
    except OSError:
        return []
    return [json.loads(line) for line in lines if line.strip()]


def _flag(argv, name):
    """Value following a flag in a recorded argv."""
    return argv[argv.index(name) + 1]


# ---------------------------------------------------------------------------
# registration + exposure gate
# ---------------------------------------------------------------------------

def test_registered_in_terminal_toolset():
    entry = registry.get_entry("claude_code")
    assert entry is not None
    assert entry.schema["name"] == "claude_code"
    assert entry.check_fn is cct.check_claude_code_requirements


def test_hidden_without_opt_in(monkeypatch, launcher):
    monkeypatch.delenv("HERMES_CLAUDE_CODE_TOOL", raising=False)
    monkeypatch.setenv("HERMES_CLAUDE_CODE_LAUNCHER", launcher)
    assert cct.check_claude_code_requirements() is False


def test_hidden_without_launcher(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_CLAUDE_CODE_TOOL", "1")
    monkeypatch.setenv("HERMES_CLAUDE_CODE_LAUNCHER", str(tmp_path / "missing"))
    assert cct.check_claude_code_requirements() is False


def test_exposed_with_flag_and_launcher(monkeypatch, launcher):
    monkeypatch.setenv("HERMES_CLAUDE_CODE_TOOL", "1")
    monkeypatch.setenv("HERMES_CLAUDE_CODE_LAUNCHER", launcher)
    assert cct.check_claude_code_requirements() is True


# ---------------------------------------------------------------------------
# payload wire format
# ---------------------------------------------------------------------------

def test_payload_roundtrip():
    payload = {"schema": cct.PAYLOAD_SCHEMA, "job_id": "cc-1", "prompt": "p"}
    body = "human text\n\n" + cct._payload_block(payload)
    assert cct.extract_payload(body) == payload


@pytest.mark.parametrize("body", [
    None, "", "no payload here",
    cct.PAYLOAD_BEGIN + "\n{not json}\n" + cct.PAYLOAD_END,
    cct.PAYLOAD_BEGIN + "\n" + json.dumps({"schema": "other"}) + "\n" + cct.PAYLOAD_END,
])
def test_payload_rejects_garbage(body):
    assert cct.extract_payload(body) is None


# ---------------------------------------------------------------------------
# action=run
# ---------------------------------------------------------------------------

def test_run_validation_error_is_typed(env):
    out = json.loads(cct.handle_claude_code({"action": "run", "prompt": ""}))
    assert "error" in out


def test_run_files_card_and_wires_wakeup(env):
    args = {
        "action": "run",
        "prompt": "--flag-looking prompt with `backticks` and $(subst)",
        "timeout_seconds": 900,
        "note": "контракт-тест",
        "max_budget_usd": 2,
    }
    out = json.loads(cct.handle_claude_code(args, session_id="sess-123"))
    assert out["filed"] is True and out["card"] == "t_deadbeef"
    assert out["lane"] == "terminal" and out["session_wake"] is True
    assert out["chat_notify"] is True

    calls = _calls(env)
    create = next(c for c in calls if "create" in c)
    assert _flag(create, "--assignee") == "terminal"
    assert _flag(create, "--max-runtime") == \
        str(900 + cct.CARD_RUNTIME_MARGIN_SECONDS)

    # the payload rides the card body verbatim — prompt is data, never shell
    payload = cct.extract_payload(_flag(create, "--body"))
    assert payload["prompt"] == args["prompt"]
    assert payload["timeout_seconds"] == 900
    assert payload["max_budget_usd"] == 2.0
    assert payload["filed_by_session"] == "sess-123"

    sub = next(c for c in calls if "notify-subscribe" in c)
    assert "t_deadbeef" in sub
    assert _flag(sub, "--chat-id") == "405154434"

    # session really stamped on the task row
    db = os.path.join(os.environ["HERMES_CLAUDE_CODE_KANBAN_ROOT"],
                      "boards", "hermes-infra", "kanban.db")
    conn = sqlite3.connect(db)
    sid = conn.execute("SELECT session_id FROM tasks WHERE id='t_deadbeef'"
                       ).fetchone()[0]
    conn.close()
    assert sid == "sess-123"


def test_run_without_session_is_honest_about_no_wakeup(env):
    out = json.loads(cct.handle_claude_code({"action": "run", "prompt": "p"}))
    assert out["filed"] is True
    assert out["session_wake"] is False and out["chat_notify"] is False
    assert not any("notify-subscribe" in c for c in _calls(env))


def test_run_wake_flag_false_when_stamp_matches_nothing(env, monkeypatch):
    """The card id must exist in the board db for session_wake to be claimed —
    an UPDATE that changed 0 rows is not a wired wake-up."""
    db = os.path.join(os.environ["HERMES_CLAUDE_CODE_KANBAN_ROOT"],
                      "boards", "hermes-infra", "kanban.db")
    conn = sqlite3.connect(db)
    conn.execute("DELETE FROM tasks")
    conn.commit()
    conn.close()
    out = json.loads(cct.handle_claude_code(
        {"action": "run", "prompt": "p"}, session_id="sess-123"))
    assert out["filed"] is True and out["session_wake"] is False


def test_run_denies_live_hermes_checkout(env, tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_CLAUDE_CODE_REPO_ROOTS", str(tmp_path) + os.sep)
    ghost = str(tmp_path / ".hermes" / "hermes-agent")
    out = json.loads(cct.handle_claude_code(
        {"action": "run", "prompt": "p", "repo": ghost}))
    assert "error" in out and "denied" in out["error"]


# ---------------------------------------------------------------------------
# action=status
# ---------------------------------------------------------------------------

def test_status_requires_card_id(env):
    out = json.loads(cct.handle_claude_code({"action": "status"}))
    assert "error" in out


def test_status_merges_task_and_run_artifacts(env, kanban_root):
    runs = os.path.join(kanban_root, "terminal-runs", "t_deadbeef", "run-01")
    os.makedirs(runs)
    core.write_result_json(runs, {"status": "ok", "verdict": "ok",
                                  "claude_session_id": "abc"})
    with open(os.path.join(runs, "output.txt"), "w") as f:
        f.write("line1\nfinal line\n")
    out = json.loads(cct.handle_claude_code(
        {"action": "status", "card_id": "t_deadbeef"}))
    assert out["task"]["status"] == "done"
    assert out["runs"][0]["result"]["verdict"] == "ok"
    assert out["runs"][0]["output_tail"] == "final line"


def test_unknown_action_is_typed_error(env):
    out = json.loads(cct.handle_claude_code({"action": "explode"}))
    assert "error" in out
