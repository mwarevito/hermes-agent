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
def signing_env(tmp_path, monkeypatch):
    dotenv = tmp_path / "hermes.env"
    dotenv.write_text("HERMES_CLAUDE_CODE_SIGNING_KEY=test-signing-key\n")
    monkeypatch.setenv("HERMES_CLAUDE_CODE_DOTENV", str(dotenv))
    return str(dotenv)


@pytest.fixture()
def claimer_state(tmp_path):
    """A healthy, schema-matching claimer state file (the handshake target)."""
    import time as _t
    p = tmp_path / "claimer.json"
    p.write_text(json.dumps({
        "updated_at": _t.time(), "lane": "terminal", "pid": 4321,
        "payload_schema": cct.PAYLOAD_SCHEMA, "core": "1.0.0",
    }))
    return p


@pytest.fixture()
def env(monkeypatch, launcher, fake_kanban, kanban_root, state_db,
        claimer_state, signing_env):
    monkeypatch.setenv("HERMES_CLAUDE_CODE_TOOL", "1")
    monkeypatch.setenv("HERMES_CLAUDE_CODE_LAUNCHER", launcher)
    monkeypatch.setenv("HERMES_CLAUDE_CODE_KANBAN_BIN", fake_kanban["bin"])
    monkeypatch.setenv("HERMES_CLAUDE_CODE_KANBAN_ROOT", kanban_root)
    monkeypatch.setenv("HERMES_CLAUDE_CODE_STATE_DB", state_db)
    monkeypatch.setenv("HERMES_CLAUDE_CODE_CLAIMER_STATE", str(claimer_state))
    # signing_env fixture already set HERMES_CLAUDE_CODE_DOTENV with the key.
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


def test_hidden_when_hermes_home_is_not_personal(monkeypatch, launcher, tmp_path):
    """Flag can leak down an inherited env line into a clinic gateway; binding
    to the personal ~/.hermes stops the tool (with host Bash) appearing there."""
    monkeypatch.setenv("HERMES_CLAUDE_CODE_TOOL", "1")
    monkeypatch.setenv("HERMES_CLAUDE_CODE_LAUNCHER", launcher)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes" / "profiles" / "workbot"))
    assert cct.check_claude_code_requirements() is False


def test_exposed_with_flag_and_launcher(monkeypatch, launcher):
    monkeypatch.setenv("HERMES_CLAUDE_CODE_TOOL", "1")
    monkeypatch.setenv("HERMES_CLAUDE_CODE_LAUNCHER", launcher)
    monkeypatch.delenv("HERMES_HOME", raising=False)
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
    assert out["chat_notify"] is True and out["wake_wired"] is True
    assert "разбудит" in out["note"]

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
    # payload is signed with the key the claimer will verify against
    assert cct.core.payload_signature_valid(payload, "test-signing-key")

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


@pytest.mark.parametrize("ghost_rel", [".hermes/x", "hermes-agent", "hermes-ops"])
def test_run_denylist_matches_the_givi_runner(env, tmp_path, monkeypatch, ghost_rel):
    """hermes-ops and hermes-agent are denied on the lane too — a job that could
    Edit the policy source could rewrite the rule that admits it."""
    monkeypatch.setenv("HERMES_CLAUDE_CODE_REPO_ROOTS", str(tmp_path) + os.sep)
    out = json.loads(cct.handle_claude_code(
        {"action": "run", "prompt": "p", "repo": str(tmp_path / ghost_rel)}))
    assert "error" in out and "denied" in out["error"]


def test_run_bash_is_not_a_default_tool(env):
    """A job gets a host shell only when it asks; Bash is never silently added."""
    cct.handle_claude_code({"action": "run", "prompt": "p"})
    create = next(c for c in _calls(env) if "create" in c)
    payload = cct.extract_payload(_flag(create, "--body"))
    assert "Bash" not in payload["allowed_tools"]
    assert "Read" in payload["allowed_tools"]


def test_run_refuses_without_signing_key(env, monkeypatch, tmp_path):
    """No key = the lane cannot authenticate the job; don't file an unrunnable card."""
    monkeypatch.delenv("HERMES_CLAUDE_CODE_SIGNING_KEY", raising=False)
    empty = tmp_path / "empty.env"
    empty.write_text("FOO=bar\n")
    monkeypatch.setenv("HERMES_CLAUDE_CODE_DOTENV", str(empty))
    out = json.loads(cct.handle_claude_code({"action": "run", "prompt": "p"}))
    assert "error" in out and "signing key" in out["error"]
    assert not any("create" in c for c in _calls(env))  # nothing filed


def test_run_refuses_when_no_live_claimer(env, monkeypatch, tmp_path):
    """No live consumer that speaks our schema = the card would sit unrun."""
    monkeypatch.setenv("HERMES_CLAUDE_CODE_CLAIMER_STATE", str(tmp_path / "gone.json"))
    out = json.loads(cct.handle_claude_code({"action": "run", "prompt": "p"}))
    assert "error" in out and "claimer" in out["error"].lower()


def test_run_refuses_on_schema_skew(env, monkeypatch, claimer_state):
    """A claimer speaking a different payload schema would mis-read the job."""
    claimer_state.write_text(json.dumps({
        "updated_at": __import__("time").time(), "payload_schema": "claude-code-job-v0",
    }))
    out = json.loads(cct.handle_claude_code({"action": "run", "prompt": "p"}))
    assert "error" in out and "skew" in out["error"].lower()


def test_run_honest_note_when_wake_half_wired(env, monkeypatch):
    """session stamped but no subscription (origin unresolved) → do NOT promise
    a wake-up; the notifier fires only inside the subscription loop."""
    monkeypatch.setattr(cct, "_resolve_origin", lambda cfg, sid: None)
    out = json.loads(cct.handle_claude_code(
        {"action": "run", "prompt": "p"}, session_id="sess-123"))
    assert out["session_wake"] is True and out["chat_notify"] is False
    assert out["wake_wired"] is False
    assert "разбудит" not in out["note"] and "сам" in out["note"]


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
