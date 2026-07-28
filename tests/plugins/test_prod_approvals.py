"""End-to-end tests for the prod-approvals plugin.

Covers the eight required scenario groups against real modules and a real
temp SQLite store (no mocks for the security-critical path). The gateway
Telegram/middleware wiring is exercised through the same public entrypoints the
plugin registers.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from pathlib import Path

import pytest

from plugins.prod_approvals import actions, fingerprint, gate, resolve
from plugins.prod_approvals import store as store_mod
from plugins.prod_approvals.store import (
    ApprovalStore,
    StoreUnavailable,
    REQUESTED,
    APPROVED,
    CLAIMED,
    SUCCEEDED,
    EXPIRED,
    INDETERMINATE,
)

SVC = "11111111-2222-3333-4444-555555555555"
SVC2 = "99999999-8888-7777-6666-555555555555"

CMD_SET = f"railway variables --set DATABASE_URL=postgres://secret --service {SVC}"
CMD_RESTART = f"railway redeploy --service {SVC} --yes"
CMD_VERIFY = f"railway variables --service {SVC}"


def CTX(**over):
    base = {
        "platform": "telegram",
        "chat_id": "chatA",
        "thread_id": "t1",
        "user_id": "userA",
        "session_id": "sessA",
        "profile": "prod",
        "task_id": "t_9b92dca5",
        "run_id": "7",
        "claim_lock": "host:123",
    }
    base.update(over)
    return base


class Clock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def __call__(self):
        return self.t

    def tick(self, dt):
        self.t += dt


@pytest.fixture
def base_dir(tmp_path, monkeypatch):
    d = tmp_path / "prod_approvals"
    monkeypatch.setattr(gate, "_base_dir", lambda: d)
    monkeypatch.setattr(resolve, "_base_dir", lambda: d)
    return d


def make_store(base_dir, clock=None):
    """Fresh store on the shared path with the fingerprint key attached (as the
    gate's own _open_store does)."""
    key = fingerprint.load_or_create_key(base_dir)
    st = ApprovalStore(base_dir, now_fn=clock or store_mod._now)
    st._fp_key = key
    return st


def store_factory(base_dir, clock=None):
    return lambda: make_store(base_dir, clock)


def sink():
    calls = []

    def next_call(args):
        calls.append(args)
        return json.dumps({"ok": True, "exit_code": 0, "stdout": "done"})

    next_call.calls = calls
    return next_call


def run_gate(base_dir, command, ctx, next_call, clock=None):
    return gate.evaluate(
        "terminal",
        {"command": command},
        next_call,
        open_store=store_factory(base_dir, clock),
        bind_context=lambda: ctx,
    )


# --- Group 1: exact command requested, nonce-approved, once, replay blocked ---

def test_group1_request_approve_once_replay_blocked(base_dir):
    nc = sink()
    ctx = CTX()

    # 1) First attempt: blocked, a pending request with a nonce is created.
    r1 = json.loads(run_gate(base_dir, CMD_SET, ctx, nc))
    assert r1["blocked"] is True
    assert r1["reason"] == "production write requires approval"
    nonce = r1["nonce"]
    assert nonce
    assert nc.calls == []  # nothing executed
    # No plaintext secret in the block payload.
    assert "postgres://secret" not in json.dumps(r1)
    assert any("<redacted:" in tok for tok in r1["argv"])

    # 2) Approve exactly this nonce from the bound channel.
    out = json.loads(resolve.handle(nonce, open_store=store_factory(base_dir),
                                    bind_context=lambda: ctx))
    assert out["ok"] is True and out["action"] == "approved"

    # 3) Re-issue: executes exactly once.
    r2 = run_gate(base_dir, CMD_SET, ctx, nc)
    assert json.loads(r2)["exit_code"] == 0
    assert len(nc.calls) == 1

    # grant is now consumed/succeeded
    st = make_store(base_dir)
    g = st.get(nonce)
    assert g.state == SUCCEEDED
    st.close()

    # 4) Replay the same command: blocked again (fresh request), NOT executed.
    r3 = json.loads(run_gate(base_dir, CMD_SET, ctx, nc))
    assert r3["blocked"] is True
    assert len(nc.calls) == 1  # still exactly one execution


# --- Group 2: changed action / context never inherits an approval ---

@pytest.mark.parametrize("mutate", [
    {"command": f"railway variables --set DATABASE_URL=postgres://OTHER --service {SVC}"},
    {"command": f"railway variables --set DATABASE_URL=postgres://secret --service {SVC2}"},
    {"ctx": {"user_id": "userB"}},
    {"ctx": {"chat_id": "chatB"}},
    {"ctx": {"thread_id": "t2"}},
    {"ctx": {"profile": "staging"}},
    {"ctx": {"session_id": "sessB"}},
    {"ctx": {"task_id": "t_other"}},
    {"ctx": {"run_id": "8"}},          # retry = new run id
    {"ctx": {"claim_lock": "host:999"}},  # reassign = new claim lock
])
def test_group2_changed_action_or_context_blocked(base_dir, mutate):
    nc = sink()
    ctx = CTX()

    # Approve the canonical set command in ctx A.
    r1 = json.loads(run_gate(base_dir, CMD_SET, ctx, nc))
    resolve.handle(r1["nonce"], open_store=store_factory(base_dir), bind_context=lambda: ctx)

    # Now attempt with a mutated action or context.
    cmd = mutate.get("command", CMD_SET)
    new_ctx = CTX(**mutate.get("ctx", {}))
    result = run_gate(base_dir, cmd, new_ctx, nc)
    data = json.loads(result)
    assert data.get("blocked") is True
    assert nc.calls == []  # the approved-A grant never executed under B


def test_group2_cwd_change_blocked(base_dir):
    nc = sink()
    ctx = CTX()
    # Approve with cwd X.
    r1 = json.loads(gate.evaluate("terminal", {"command": CMD_SET, "cwd": "/srv/x"}, nc,
                                  open_store=store_factory(base_dir), bind_context=lambda: ctx))
    resolve.handle(r1["nonce"], open_store=store_factory(base_dir), bind_context=lambda: ctx)
    # Execute with a different cwd → different action fingerprint → blocked.
    res = gate.evaluate("terminal", {"command": CMD_SET, "cwd": "/srv/y"}, nc,
                        open_store=store_factory(base_dir), bind_context=lambda: ctx)
    assert json.loads(res)["blocked"] is True
    assert nc.calls == []


# --- Group 3: 2 pending + nonce selects the exact request ---

def test_group3_two_pending_nonce_selects_exact(base_dir):
    nc = sink()
    ctx = CTX()
    r_set = json.loads(run_gate(base_dir, CMD_SET, ctx, nc))
    r_restart = json.loads(run_gate(base_dir, CMD_RESTART, ctx, nc))
    n_set, n_restart = r_set["nonce"], r_restart["nonce"]
    assert n_set != n_restart

    # Approve only the restart nonce.
    out = json.loads(resolve.handle(n_restart, open_store=store_factory(base_dir),
                                    bind_context=lambda: ctx))
    assert out["ok"] is True

    st = make_store(base_dir)
    assert st.get(n_restart).state == APPROVED
    assert st.get(n_set).state == REQUESTED  # untouched
    st.close()

    # The set command is still blocked; the restart command executes.
    assert json.loads(run_gate(base_dir, CMD_SET, ctx, nc))["blocked"] is True
    assert json.loads(run_gate(base_dir, CMD_RESTART, ctx, nc))["exit_code"] == 0
    assert len(nc.calls) == 1


def test_group3_approve_from_other_channel_rejected(base_dir):
    nc = sink()
    ctx = CTX()
    r = json.loads(run_gate(base_dir, CMD_SET, ctx, nc))
    # Someone in another chat/user tries to approve this nonce.
    other = CTX(chat_id="chatZ", user_id="userZ")
    out = json.loads(resolve.handle(r["nonce"], open_store=store_factory(base_dir),
                                    bind_context=lambda: other))
    assert out["ok"] is False
    assert "not authorised" in out["error"]
    # Still blocked when re-issued in the real context.
    assert json.loads(run_gate(base_dir, CMD_SET, ctx, nc))["blocked"] is True


# --- Group 4: races, crash/indeterminate, revoke/expiry, restart/multi-process ---

def test_group4_parallel_claim_exactly_one_wins(base_dir):
    st = make_store(base_dir)
    g = st.request(grant_fp="fp", action_fp="afp", action_class="railway_restart",
                   redacted_argv=("railway", "redeploy"), targets=(("service", SVC),),
                   ctx=CTX())
    assert st.approve(g.nonce)
    st.close()

    results = []

    def worker():
        s = make_store(base_dir)
        try:
            results.append(s.claim(g.nonce, 1))  # fence after approve == 1
        finally:
            s.close()

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results.count(True) == 1
    assert results.count(False) == 5


def test_group4_crash_marks_indeterminate_no_autoretry(base_dir):
    ctx = CTX()

    def approve_first():
        nc = sink()
        r = json.loads(run_gate(base_dir, CMD_RESTART, ctx, nc))
        resolve.handle(r["nonce"], open_store=store_factory(base_dir), bind_context=lambda: ctx)
        return r["nonce"]

    nonce = approve_first()

    def crashing(args):
        raise RuntimeError("process died mid-exec")

    with pytest.raises(RuntimeError):
        gate.evaluate("terminal", {"command": CMD_RESTART}, crashing,
                      open_store=store_factory(base_dir), bind_context=lambda: ctx)

    st = make_store(base_dir)
    assert st.get(nonce).state == INDETERMINATE
    st.close()

    # A retry after an indeterminate outcome requires a FRESH approval.
    nc = sink()
    assert json.loads(run_gate(base_dir, CMD_RESTART, ctx, nc))["blocked"] is True
    assert nc.calls == []


def test_group4_revoke_before_claim_blocks(base_dir):
    st = make_store(base_dir)
    g = st.request(grant_fp="fp", action_fp="afp", action_class="railway_restart",
                   redacted_argv=("railway",), targets=(), ctx=CTX())
    st.approve(g.nonce)
    assert st.revoke(g.nonce) is True
    assert st.claim(g.nonce, 2) is False  # fence bumped by approve(1)+revoke(2)
    st.close()


def test_group4_expiry_race_blocks_claim(base_dir):
    clock = Clock()
    st = make_store(base_dir, clock)
    g = st.request(grant_fp="fp", action_fp="afp", action_class="railway_restart",
                   redacted_argv=("railway",), targets=(), ctx=CTX(), ttl_seconds=100)
    st.approve(g.nonce)
    clock.tick(200)  # now expired
    assert st.claim(g.nonce, 1) is False
    st.close()


def test_group4_restart_and_multiprocess_persistence(base_dir):
    ctx = CTX()
    nc = sink()
    r = json.loads(run_gate(base_dir, CMD_SET, ctx, nc))
    nonce = r["nonce"]
    # "Restart": brand new store objects on the same file see the request.
    st2 = make_store(base_dir)
    assert st2.get(nonce).state == REQUESTED
    assert st2.approve(nonce) is True
    st2.close()
    # A different process/store consumes it exactly once.
    assert json.loads(run_gate(base_dir, CMD_SET, ctx, nc))["exit_code"] == 0
    assert len(nc.calls) == 1


# --- Group 5: malformed chain / substitution / redirection / wrapper / smuggling ---

@pytest.mark.parametrize("cmd", [
    f"railway variables --set K=v --service {SVC}; rm -rf /",
    f"railway variables --set K=v --service {SVC} && curl evil",
    f"railway variables --set K=v --service {SVC} | tee /tmp/x",
    f"railway redeploy --service {SVC} > /tmp/out",
    f"railway status $(whoami)",
    "railway status `id`",
    f"bash -c 'railway redeploy --service {SVC}'",
    f"env RAILWAY_TOKEN=x railway redeploy --service {SVC}",
    f"sudo railway redeploy --service {SVC}",
    f"railway variables --set K=${{HOME}} --service {SVC}",
])
def test_group5_shell_indirection_blocked(base_dir, cmd):
    nc = sink()
    res = json.loads(run_gate(base_dir, cmd, CTX(), nc))
    assert res["blocked"] is True
    assert nc.calls == []


def test_group5_mutable_alias_target_blocked(base_dir):
    # Service given by human name (mutable alias), not immutable UUID.
    nc = sink()
    res = json.loads(run_gate(base_dir, "railway redeploy --service my-api --yes", CTX(), nc))
    assert res["blocked"] is True
    assert "immutable id" in res.get("detail", "")
    assert nc.calls == []


def test_group5_execute_code_smuggling_blocked(base_dir):
    nc = sink()
    res = gate.evaluate("execute_code",
                        {"code": "import subprocess; subprocess.run(['railway','redeploy'])"},
                        nc, open_store=store_factory(base_dir), bind_context=lambda: CTX())
    data = json.loads(res)
    assert data["blocked"] is True
    assert data["class"] == "execute_code-smuggling"
    assert nc.calls == []


def test_group5_nonprod_command_passes_through(base_dir):
    nc = sink()
    res = gate.evaluate("terminal", {"command": "ls -la /tmp"}, nc,
                        open_store=store_factory(base_dir), bind_context=lambda: CTX())
    assert json.loads(res)["exit_code"] == 0
    assert len(nc.calls) == 1  # executed, no approval required


# --- Group 6: ordered bounded bundle happy path + failure modes ---

def _bundle_steps(key, ctx):
    def step(cmd):
        act = actions.classify(cmd)
        gfp = fingerprint.grant_fingerprint(act, ctx, key)
        return {
            "grant_fp": gfp,
            "action_fp": fingerprint.action_fingerprint(act, key),
            "action_class": act.action_class,
            "redacted_argv": fingerprint.redact_argv(act, key),
            "targets": act.targets,
        }
    return [step(CMD_SET), step(CMD_RESTART), step(CMD_VERIFY)]


def test_group6_bundle_happy_path_in_order(base_dir):
    ctx = CTX()
    key = fingerprint.load_or_create_key(base_dir)
    st = make_store(base_dir)
    steps = _bundle_steps(key, ctx)
    bundle_id, grants = st.create_bundle(steps=steps, ctx=ctx)
    assert st.approve_bundle(bundle_id) is True

    # Consume in order; each dependency must have succeeded first.
    for i, cmd in enumerate([CMD_SET, CMD_RESTART, CMD_VERIFY]):
        act = actions.classify(cmd)
        gfp = fingerprint.grant_fingerprint(act, ctx, key)
        g = st.find_bundle_step(gfp, ctx)
        assert g is not None, f"step {i} not consumable"
        assert st.claim(g.nonce, g.fence) is True
        assert st.finish(g.nonce, SUCCEEDED) is True
    st.close()


def test_group6_out_of_order_blocked(base_dir):
    ctx = CTX()
    key = fingerprint.load_or_create_key(base_dir)
    st = make_store(base_dir)
    steps = _bundle_steps(key, ctx)
    bundle_id, grants = st.create_bundle(steps=steps, ctx=ctx)
    st.approve_bundle(bundle_id)
    # Try to claim step 1 (restart) before step 0 (set) succeeded.
    act = actions.classify(CMD_RESTART)
    gfp = fingerprint.grant_fingerprint(act, ctx, key)
    assert st.find_bundle_step(gfp, ctx) is None  # deps unmet → not the next step
    # step 1 grant exists but deps unsatisfied → direct claim also fails
    step1 = st.get(grants[1].nonce)  # fresh post-approve fence
    assert st.claim(step1.nonce, step1.fence) is False
    st.close()


def test_group6_cardinality_bound(base_dir):
    ctx = CTX()
    key = fingerprint.load_or_create_key(base_dir)
    st = make_store(base_dir)
    many = _bundle_steps(key, ctx) * 4  # 12 steps > max 8
    with pytest.raises(ValueError):
        st.create_bundle(steps=many, ctx=ctx, max_steps=8)
    st.close()


def test_group6_dependency_and_double_consume(base_dir):
    ctx = CTX()
    key = fingerprint.load_or_create_key(base_dir)
    st = make_store(base_dir)
    steps = _bundle_steps(key, ctx)
    bundle_id, grants = st.create_bundle(steps=steps, ctx=ctx)
    st.approve_bundle(bundle_id)
    # Claim + succeed step 0 (re-read for the fresh post-approve fence).
    g0 = st.get(grants[0].nonce)
    assert st.claim(g0.nonce, g0.fence) is True
    # Cardinality: cannot claim step 0 again.
    assert st.claim(g0.nonce, g0.fence + 1) is False
    assert st.finish(g0.nonce, SUCCEEDED) is True
    # Now step 1 dep satisfied.
    g1 = st.get(grants[1].nonce)
    assert st.claim(g1.nonce, g1.fence) is True
    st.close()


def test_group6_bundle_expiry_blocks(base_dir):
    ctx = CTX()
    clock = Clock()
    key = fingerprint.load_or_create_key(base_dir)
    st = make_store(base_dir, clock)
    steps = _bundle_steps(key, ctx)
    bundle_id, grants = st.create_bundle(steps=steps, ctx=ctx, ttl_seconds=100)
    st.approve_bundle(bundle_id)
    clock.tick(200)
    g0 = grants[0]
    assert st.claim(g0.nonce, g0.fence) is False
    st.close()


def test_group6_rollback_is_separate_bounded_bundle(base_dir):
    ctx = CTX()
    key = fingerprint.load_or_create_key(base_dir)
    st = make_store(base_dir)
    fwd_id, _ = st.create_bundle(steps=_bundle_steps(key, ctx), ctx=ctx, kind="forward")
    rb_id, rb_grants = st.create_bundle(steps=_bundle_steps(key, ctx), ctx=ctx, kind="rollback")
    assert fwd_id != rb_id
    # Approving the forward bundle does not approve rollback steps.
    st.approve_bundle(fwd_id)
    assert st.get(rb_grants[0].nonce).state == REQUESTED
    st.close()


# --- Group 7: corrupt / unavailable / stale schema fail closed; migration ---

def test_group7_corrupt_store_fails_closed(base_dir):
    base_dir.mkdir(parents=True, exist_ok=True)
    (base_dir / "approvals.db").write_bytes(b"this is not a sqlite database at all")
    with pytest.raises(StoreUnavailable):
        ApprovalStore(base_dir)


def test_group7_newer_schema_fails_closed(base_dir):
    st = make_store(base_dir)
    st.close()
    conn = sqlite3.connect(str(base_dir / "approvals.db"))
    conn.execute("UPDATE schema_meta SET version=? WHERE id=1", (store_mod.SCHEMA_VERSION + 5,))
    conn.commit()
    conn.close()
    with pytest.raises(StoreUnavailable):
        ApprovalStore(base_dir)


def test_group7_gate_blocks_when_store_unavailable(base_dir):
    nc = sink()

    def broken_store():
        raise StoreUnavailable("disk gone")

    res = gate.evaluate("terminal", {"command": CMD_SET}, nc,
                        open_store=broken_store, bind_context=lambda: CTX())
    assert json.loads(res)["blocked"] is True
    assert nc.calls == []


def test_group7_migration_invalidates_legacy_pending(base_dir):
    # Seed a store with a requested + an approved grant, then downgrade the
    # recorded schema version to simulate a legacy store.
    st = make_store(base_dir)
    g_req = st.request(grant_fp="fp1", action_fp="a1", action_class="railway_restart",
                       redacted_argv=("railway",), targets=(), ctx=CTX())
    g_appr = st.request(grant_fp="fp2", action_fp="a2", action_class="railway_variable_set",
                        redacted_argv=("railway",), targets=(), ctx=CTX(chat_id="chatB"))
    st.approve(g_appr.nonce)
    st.close()

    conn = sqlite3.connect(str(base_dir / "approvals.db"))
    conn.execute("UPDATE schema_meta SET version=0 WHERE id=1")
    conn.commit()
    conn.close()

    # Reopen → migration runs → every non-terminal grant invalidated.
    st2 = ApprovalStore(base_dir)
    assert st2.get(g_req.nonce).state == EXPIRED
    assert st2.get(g_appr.nonce).state == EXPIRED
    # And a migrated legacy 'approved' grant is no longer consumable.
    assert st2.find_consumable("fp2", CTX(chat_id="chatB")) is None
    st2.close()


def test_group7_context_forgery_fails_closed(monkeypatch, base_dir):
    # A worker rewrites HERMES_KANBAN_* to impersonate another task; the DB
    # anchor disagrees → ContextError → gate blocks.
    from plugins.prod_approvals import context as ctxmod

    def forged_validator():
        raise ctxmod.ContextError("kanban env anchor does not match DB anchor")

    def bind():
        return ctxmod.bind_context(kanban_validator=forged_validator)

    nc = sink()
    res = gate.evaluate("terminal", {"command": CMD_SET}, nc,
                        open_store=store_factory(base_dir), bind_context=bind)
    assert json.loads(res)["blocked"] is True
    assert "context could not be bound" in json.loads(res)["reason"]
    assert nc.calls == []


# --- Group 8: no-regression / plugin wiring ---

def test_group8_plugin_register_wires_middleware_and_command(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    import plugins.prod_approvals as plugin
    from plugins.prod_approvals import heartbeat as _heartbeat

    registered = {"middleware": [], "commands": [], "gateway_actions": [], "tools": []}

    class FakeCtx:
        def register_middleware(self, kind, cb):
            registered["middleware"].append((kind, cb))

        def register_command(self, name, handler, description="", args_hint=""):
            registered["commands"].append(name)

        def register_gateway_action_handler(self, prefix, callback):
            registered["gateway_actions"].append((prefix, callback))

        def register_tool(self, name, toolset, schema, handler, **kw):
            registered["tools"].append(name)

    try:
        plugin.register(FakeCtx())
        assert registered["middleware"][0][0] == "tool_execution"
        assert "approve-prod" in registered["commands"]
        # Real one-tap surface wired (not just the slash fallback).
        assert any(p == "pa:" for p, _ in registered["gateway_actions"])
        # User-facing bundle creation path wired.
        assert "prod_bundle_request" in registered["tools"]
        # activation marker dropped by the heartbeat
        assert (Path(tmp_path) / "prod_approvals" / "plugin_active").exists()
    finally:
        _heartbeat.stop_singleton()  # no daemon-thread leak


def test_group8_middleware_signature_matches_host_contract(base_dir):
    # The registered middleware must accept the host kwargs and pass non-gated
    # tools straight through (mirrors run_tool_execution_middleware call shape).
    called = []

    def next_call(args):
        called.append(args)
        return "ok"

    out = gate.middleware(
        tool_name="read_file",
        args={"path": "README.md"},
        original_args={"path": "README.md"},
        next_call=next_call,
        task_id="t", session_id="s", tool_call_id="c", turn_id="u", api_request_id="a",
    )
    assert out == "ok"
    assert called == [{"path": "README.md"}]


def test_group8_no_secret_in_audit_or_fingerprint(base_dir):
    ctx = CTX()
    key = fingerprint.load_or_create_key(base_dir)
    act = actions.classify(CMD_SET)
    fp = fingerprint.grant_fingerprint(act, ctx, key)
    red = fingerprint.redact_argv(act, key)
    assert "postgres://secret" not in fp
    assert all("postgres://secret" not in tok for tok in red)
    # store audit rows carry no secret either
    st = make_store(base_dir)
    st.request(grant_fp=fp, action_fp=fingerprint.action_fingerprint(act, key),
               action_class=act.action_class, redacted_argv=red, targets=act.targets, ctx=ctx)
    rows = st._conn.execute("SELECT detail FROM audit").fetchall()
    assert all("postgres://secret" not in (r[0] or "") for r in rows)
    ctx_rows = st._conn.execute("SELECT ctx_json, redacted_argv FROM grants").fetchall()
    for cj, ra in ctx_rows:
        assert "postgres://secret" not in cj
        assert "postgres://secret" not in ra
    st.close()


# --- Group 9: read-only pass-through, conservative outcome, card delivery ---

def test_group9_readonly_status_passes_through(base_dir):
    nc = sink()
    res = run_gate(base_dir, "railway status", CTX(), nc)
    assert json.loads(res)["exit_code"] == 0
    assert len(nc.calls) == 1  # executed with NO approval
    st = make_store(base_dir)
    assert st.list_pending() == []  # and created no pending request
    st.close()


def test_group9_readonly_variables_list_passes_through(base_dir):
    nc = sink()
    res = run_gate(base_dir, CMD_VERIFY, CTX(), nc)  # `railway variables --service <uuid>`
    assert json.loads(res)["exit_code"] == 0
    assert len(nc.calls) == 1


def test_group9_write_still_gated_after_readonly_change(base_dir):
    # A read passing through must not weaken gating of a write.
    nc = sink()
    run_gate(base_dir, "railway status", CTX(), nc)
    res = run_gate(base_dir, CMD_SET, CTX(), nc)
    assert json.loads(res)["blocked"] is True
    assert len(nc.calls) == 1  # only the read executed


def test_group9_outcome_defaults_indeterminate_on_unproven_result(base_dir):
    ctx = CTX()
    r = json.loads(run_gate(base_dir, CMD_RESTART, ctx, sink()))
    resolve.handle(r["nonce"], open_store=store_factory(base_dir), bind_context=lambda: ctx)

    def plain(args):
        return "restart triggered"  # not JSON → success cannot be proven

    res = gate.evaluate("terminal", {"command": CMD_RESTART}, plain,
                        open_store=store_factory(base_dir), bind_context=lambda: ctx)
    assert res == "restart triggered"
    st = make_store(base_dir)
    assert st.get(r["nonce"]).state == INDETERMINATE
    st.close()


def test_group9_outcome_failed_on_nonzero_exit(base_dir):
    ctx = CTX()
    r = json.loads(run_gate(base_dir, CMD_RESTART, ctx, sink()))
    resolve.handle(r["nonce"], open_store=store_factory(base_dir), bind_context=lambda: ctx)

    def failing(args):
        return json.dumps({"exit_code": 3, "stderr": "boom"})

    gate.evaluate("terminal", {"command": CMD_RESTART}, failing,
                  open_store=store_factory(base_dir), bind_context=lambda: ctx)
    st = make_store(base_dir)
    assert st.get(r["nonce"]).state == "failed"
    st.close()


def test_group9_gate_delivers_card_with_nonce_buttons(base_dir):
    from plugins.prod_approvals import cards
    captured = []

    def deliver(card):
        captured.append(card)
        return True

    ctx = CTX()
    r = json.loads(gate.evaluate(
        "terminal", {"command": CMD_SET}, sink(),
        open_store=store_factory(base_dir), bind_context=lambda: ctx, deliver_card=deliver,
    ))
    assert r["blocked"] is True and r["card_delivered"] is True
    assert len(captured) == 1
    card = captured[0]
    datas = [d for row in card.buttons for (_label, d) in row]
    assert f"{cards.CB_APPROVE}{r['nonce']}" in datas
    assert f"{cards.CB_DENY}{r['nonce']}" in datas
    # The card carries the nonce in the button, never the secret nor a 32-hex to type.
    assert "postgres://secret" not in card.text
    assert card.platform == "telegram" and card.chat_id == "chatA" and card.thread_id == "t1"


def test_group9_execute_code_classification_failure_fails_closed(base_dir, monkeypatch):
    # If classification of execute_code raises, the gate must block (fail closed),
    # never fall through to run the code.
    from plugins.prod_approvals import actions

    def boom(_cmd):
        raise RuntimeError("classifier exploded")

    monkeypatch.setattr(actions, "is_prod_write_class", boom)
    nc = sink()
    res = gate.evaluate("execute_code", {"code": "print(1)"}, nc,
                        open_store=store_factory(base_dir), bind_context=lambda: CTX())
    assert json.loads(res)["blocked"] is True
    assert nc.calls == []


# --- Group 10: user-facing bundle creation → approve → consumed in order via gate ---

def test_group10_bundle_created_and_consumed_in_order(base_dir, monkeypatch):
    from plugins.prod_approvals import bundle as bundlemod
    from plugins.prod_approvals import context as ctxmod
    from plugins.prod_approvals import cards as cardsmod

    ctx = CTX()
    monkeypatch.setattr(ctxmod, "bind_context", lambda *a, **k: ctx)
    captured = []
    monkeypatch.setattr(cardsmod, "deliver", lambda card: (captured.append(card) or True))

    out = json.loads(bundlemod.request_bundle(
        {"commands": [CMD_SET, CMD_RESTART, CMD_VERIFY], "kind": "forward"}
    ))
    assert out["ok"] is True and out["card_delivered"] is True
    bundle_id = out["bundle_id"]
    assert len(captured) == 1
    # single card, bundle-bound button
    datas = [d for row in captured[0].buttons for (_l, d) in row]
    assert f"{cardsmod.CB_BUNDLE_APPROVE}{bundle_id}" in datas

    nc = sink()

    def g(cmd):
        return gate.evaluate("terminal", {"command": cmd}, nc,
                             open_store=store_factory(base_dir), bind_context=lambda: ctx)

    # Before approval, step 0 is blocked (no approved grant yet).
    assert json.loads(g(CMD_SET))["blocked"] is True
    assert nc.calls == []

    # Approve the whole bundle (bundle-bound).
    st = make_store(base_dir)
    assert st.approve_bundle(bundle_id) is True
    st.close()

    # Out of order: restart before set → blocked (dependency unmet).
    assert json.loads(g(CMD_RESTART))["blocked"] is True
    assert nc.calls == []

    # In order: set → restart → read-only verify, each exactly once.
    assert json.loads(g(CMD_SET))["exit_code"] == 0
    assert json.loads(g(CMD_RESTART))["exit_code"] == 0
    assert json.loads(g(CMD_VERIFY))["exit_code"] == 0
    assert len(nc.calls) == 3


def test_group10_bundle_rejects_unstructured_step(base_dir, monkeypatch):
    from plugins.prod_approvals import bundle as bundlemod
    from plugins.prod_approvals import context as ctxmod
    monkeypatch.setattr(ctxmod, "bind_context", lambda *a, **k: CTX())
    out = json.loads(bundlemod.request_bundle(
        {"commands": [CMD_SET, f"railway redeploy --service {SVC}; rm -rf /"]}
    ))
    assert out["ok"] is False and out["blocked"] is True


def test_group10_bundle_cardinality_enforced(base_dir, monkeypatch):
    from plugins.prod_approvals import bundle as bundlemod
    from plugins.prod_approvals import context as ctxmod
    monkeypatch.setattr(ctxmod, "bind_context", lambda *a, **k: CTX())
    out = json.loads(bundlemod.request_bundle({"commands": [CMD_VERIFY] * 9}))
    assert out["ok"] is False


# --- Group 11: liveness heartbeat (>6 minute staleness, refresh, no leak) ---

def test_group11_marker_stale_after_window_blocks(base_dir):
    from plugins.prod_approvals import heartbeat
    now = 1_000_000.0
    heartbeat.write_marker(base_dir, now=now)
    # Fresh right after write.
    assert heartbeat.is_fresh(base_dir, now=now + 30) is True
    # >6 minutes later with no refresh → stale (backstop would block).
    assert heartbeat.is_fresh(base_dir, now=now + 400) is False


def test_group11_refresher_keeps_marker_fresh_then_stops(base_dir):
    from plugins.prod_approvals import heartbeat
    hb = heartbeat.Heartbeat(base_dir, interval=0.02)
    hb.start()
    try:
        import time as _t
        ts1 = heartbeat.read_marker_ts(base_dir)
        assert ts1 is not None
        _t.sleep(0.12)  # several refresh intervals
        ts2 = heartbeat.read_marker_ts(base_dir)
        assert ts2 is not None and ts2 > ts1  # timestamp advanced while alive
    finally:
        hb.stop()
    assert hb.alive is False  # no daemon-thread leak


def test_group11_backstop_blocks_when_marker_stale(tmp_path):
    # Backstop hook with a >6-minute-old marker treats the plugin as down.
    import time
    marker_dir = tmp_path / "prod_approvals"
    marker_dir.mkdir(parents=True)
    (marker_dir / "plugin_active").write_text(f"{time.time() - 400:.3f} 4242")
    out = _run_hook(
        {"tool_name": "terminal", "tool_input": {"command": f"railway redeploy --service {SVC}"}},
        tmp_path,
    )
    assert json.loads(out)["decision"] == "block"


def test_group11_backstop_defers_when_marker_has_pid(tmp_path):
    # New "<ts> <pid>" marker format is parsed by the (updated) hook.
    import time
    marker_dir = tmp_path / "prod_approvals"
    marker_dir.mkdir(parents=True)
    (marker_dir / "plugin_active").write_text(f"{time.time():.3f} 4242")
    out = _run_hook(
        {"tool_name": "terminal", "tool_input": {"command": f"railway redeploy --service {SVC}"}},
        tmp_path,
    )
    assert out == ""  # fresh → defer to middleware gate


# --- Backstop shell hook (fail-closed floor / activation contract) ---

import subprocess

_HOOK = Path(__file__).resolve().parents[2] / "plugins" / "prod_approvals" / "backstop" / "gate-prod-writes.sh"


def _run_hook(payload, home):
    return subprocess.run(
        ["bash", str(_HOOK)],
        input=json.dumps(payload),
        env={**os.environ, "HERMES_HOME": str(home)},
        capture_output=True, text=True,
    ).stdout.strip()


def test_backstop_blocks_prod_write_when_plugin_absent(tmp_path):
    out = _run_hook(
        {"tool_name": "terminal", "tool_input": {"command": f"railway redeploy --service {SVC}"}},
        tmp_path,
    )
    assert json.loads(out)["decision"] == "block"


def test_backstop_passes_nonprod(tmp_path):
    out = _run_hook({"tool_name": "terminal", "tool_input": {"command": "ls -la"}}, tmp_path)
    assert out == ""


def test_backstop_defers_when_plugin_marker_fresh(tmp_path):
    import time
    marker_dir = tmp_path / "prod_approvals"
    marker_dir.mkdir(parents=True)
    (marker_dir / "plugin_active").write_text(str(time.time()))
    out = _run_hook(
        {"tool_name": "terminal", "tool_input": {"command": f"railway redeploy --service {SVC}"}},
        tmp_path,
    )
    assert out == ""  # defer to in-process middleware gate


def test_backstop_blocks_execute_code_smuggling(tmp_path):
    out = _run_hook(
        {"tool_name": "execute_code", "tool_input": {"code": "subprocess.run(['railway','up'])"}},
        tmp_path,
    )
    assert json.loads(out)["decision"] == "block"
