"""Acceptance for tools/claude_code_tool.py — the typed claude_code tool.

Pins the exposure gate (hidden without an explicit opt-in — the live repo is
shared by all seven gateways), the card wire format, the honest session_wake
flag (an UPDATE that matched nothing must not read as a stamped wake-up), and
the status view over file-backed run artifacts.

Since 13.08.2026 it also pins the WAKE LADDER: where the completion address
comes from when the filing session has no gateway_routing row (a kanban worker
never does), that the dispatcher's HERMES_KANBAN_DB pin decides which board the
tool's own writes land on, and that a job nobody can be notified about says so
in the log and on the card instead of in a quiet response field.
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
    # The REAL schema, not a hand-rolled subset: the tool reaches this db
    # through hermes_cli.kanban_db.connect(), whose additive migration pass
    # dies on a `tasks` that exists without the live columns (measured
    # 2026-08-13: "no such column: assignee").
    from hermes_cli import kanban_db as _kb
    conn = _kb.connect(db_path=db)
    conn.execute("INSERT INTO tasks (id, title, status, created_at) "
                 "VALUES ('t_deadbeef', 't', 'ready', 0)")
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
    # The wake ladder reads the ambient session/worker environment, so a test
    # inherited from a real gateway or worker shell would otherwise resolve a
    # LIVE chat address and both pass for the wrong reason and (worse) write a
    # subscription pointing at Vito's DM from a unit test.
    for _leak in ("HERMES_KANBAN_DB", "HERMES_KANBAN_BOARD",
                  "HERMES_KANBAN_TASK", "HERMES_SESSION_PLATFORM",
                  "HERMES_SESSION_CHAT_ID", "HERMES_SESSION_CHAT_TYPE",
                  "HERMES_SESSION_THREAD_ID", "HERMES_SESSION_USER_ID",
                  "HERMES_SESSION_KEY", "HERMES_SESSION_MESSAGE_ID"):
        monkeypatch.delenv(_leak, raising=False)
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


def _subs(card, board="hermes-infra"):
    """(platform, chat_id) of every notification subscription on ``card``.

    Read from the board DB rather than from a recorded CLI call: since the
    address comes from the shared kanban ladder, the durable row is the only
    thing that decides whether anyone is woken."""
    pinned = os.environ.get("HERMES_KANBAN_DB", "").strip()
    db = pinned or os.path.join(
        os.environ["HERMES_CLAUDE_CODE_KANBAN_ROOT"], "boards", board,
        "kanban.db")
    if not os.path.exists(db):
        return []
    conn = sqlite3.connect(db)
    try:
        return [tuple(r) for r in conn.execute(
            "SELECT platform, chat_id FROM kanban_notify_subs "
            "WHERE task_id = ? ORDER BY created_at", (card,))]
    except sqlite3.Error:
        return []
    finally:
        conn.close()


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
    assert "придёт в этот разговор" in out["note"]

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

    # The subscription is a ROW, not a CLI call: the address now comes from the
    # shared kanban ladder (kanban_tools._maybe_auto_subscribe) with the
    # gateway_routing lookup as its last rung, and both write through
    # kanban_db.add_notify_sub. Assert the durable fact, not the transport.
    assert out["notify_via"] == "telegram:405154434"
    assert _subs("t_deadbeef") == [("telegram", "405154434")]

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
    assert _subs("t_deadbeef") == []


def test_run_worker_inherits_the_address_of_its_own_card(env, monkeypatch):
    """A kanban worker has no gateway_routing row at all — 13.08.2026 that made
    every one of eight filed cards land with nobody subscribed. Its own card
    knows the conversation, and that is the rung the tool now uses."""
    db = os.path.join(os.environ["HERMES_CLAUDE_CODE_KANBAN_ROOT"],
                      "boards", "hermes-infra", "kanban.db")
    from hermes_cli import kanban_db as _kb
    conn = _kb.connect(db_path=__import__("pathlib").Path(db))
    try:
        _kb.add_notify_sub(conn, task_id="t_parent01", platform="telegram",
                           chat_id="405154434", chat_type="dm",
                           notifier_profile="default")
    finally:
        conn.close()
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_parent01")
    # no session in gateway_routing: exactly a worker's situation
    out = json.loads(cct.handle_claude_code(
        {"action": "run", "prompt": "p"}, session_id="20260813_144528_e6f3a1"))
    assert out["chat_notify"] is True and out["wake_wired"] is True
    assert _subs("t_deadbeef") == [("telegram", "405154434")]


def test_run_subscribed_card_is_not_declared_undeliverable(env, monkeypatch,
                                                           caplog):
    """A subscription without a stamped session_id still DELIVERS.

    The notifier loop (gateway/kanban_watchers.py) walks ``kanban_notify_subs``
    and never reads ``tasks.session_id``, so conflating the two claims stamped a
    permanent "⚠ БЕЗ ПРОБУЖДЕНИЯ … проверять руками" onto a card whose
    completion would in fact arrive — and sent the model back to polling."""
    import logging
    db = os.path.join(os.environ["HERMES_CLAUDE_CODE_KANBAN_ROOT"],
                      "boards", "hermes-infra", "kanban.db")
    from hermes_cli import kanban_db as _kb
    conn = _kb.connect(db_path=__import__("pathlib").Path(db))
    try:
        _kb.add_notify_sub(conn, task_id="t_parent01", platform="telegram",
                           chat_id="405154434", chat_type="dm",
                           notifier_profile="default")
    finally:
        conn.close()
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_parent01")
    with caplog.at_level(logging.WARNING):
        out = json.loads(cct.handle_claude_code({"action": "run", "prompt": "p"}))
    assert out["chat_notify"] is True
    assert out["session_wake"] is False
    assert out["wake_wired"] is True
    assert "warning" not in out
    assert not out["note"].startswith("⚠")
    assert not any("NOT wired" in r.getMessage() for r in caplog.records)
    assert not any("comment" in c for c in _calls(env))


def test_run_unwired_job_is_loud(env, caplog, monkeypatch):
    """Silence was the whole defect: the only 13.08 signal was a false-valued
    field inside a success payload. A job nobody can hear about must shout."""
    import logging
    monkeypatch.setattr(cct, "_resolve_origin", lambda cfg, sid: None)
    with caplog.at_level(logging.WARNING):
        out = json.loads(cct.handle_claude_code(
            {"action": "run", "prompt": "p"}, session_id="sess-123"))
    assert out["wake_wired"] is False
    assert out["warning"].startswith("ЗАВЕРШЕНИЕ ЭТОЙ ЗАДАЧИ НИКОМУ НЕ ПРИДЁТ")
    assert out["note"].startswith("⚠")
    assert any(cct.WAKE_LOG_PREFIX in r.getMessage() and
               "NOT wired" in r.getMessage() for r in caplog.records)
    # …and the card itself carries the mark, via the existing `comment` verb
    comment = next(c for c in _calls(env) if "comment" in c)
    assert "t_deadbeef" in comment and "БЕЗ ПРОБУЖДЕНИЯ" in " ".join(comment)


def test_run_follows_the_dispatcher_board_pin(env, monkeypatch, tmp_path):
    """`kanban_db_path` honours HERMES_KANBAN_DB BEFORE the board argument
    (kanban_db.py:1033) and the dispatcher pins it into every worker, so inside
    a worker `--board` does not choose the file. The tool's own writes must
    follow the same pin or they hit an empty database: on 13.08 three cards
    filed from a worker pinned to `hermes` while asking for `hermes-infra`
    ended up with a NULL session_id and no subscription."""
    pinned_dir = tmp_path / "kanban" / "boards" / "hermes"
    pinned_dir.mkdir(parents=True, exist_ok=True)
    pinned = pinned_dir / "kanban.db"
    from hermes_cli import kanban_db as _kb
    conn = _kb.connect(db_path=pinned)
    conn.execute("INSERT INTO tasks (id, title, status, created_at) "
                 "VALUES ('t_deadbeef', 't', 'ready', 0)")
    conn.commit()
    conn.close()
    monkeypatch.setenv("HERMES_KANBAN_DB", str(pinned))

    out = json.loads(cct.handle_claude_code(
        {"action": "run", "prompt": "p"}, session_id="sess-123"))
    assert out["session_wake"] is True
    assert out["board"] == "hermes" and out["board_requested"] == "hermes-infra"
    conn = sqlite3.connect(str(pinned))
    sid = conn.execute(
        "SELECT session_id FROM tasks WHERE id='t_deadbeef'").fetchone()[0]
    conn.close()
    assert sid == "sess-123"


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
