"""Tests for the novel-task-gate ``accesses`` credential fast path.

Covers ``profile-plugins/novel-task-gate/accesses_fast_path.py``:

  * **Minting** — the lease is minted only when the user's own message both
    names credential material *and* spells out exactly one repo-local
    ``<repo>/accesses/<name>.md`` path.
  * **Denials** — no credential marker, ambiguous (two) paths, ``..``
    traversal, non-``.md``, a destination outside the repo's ``accesses``
    directory, ``.env``/config/auth stores, a symlinked ``accesses`` directory
    or symlinked target, ``$HERMES_HOME``, and a path with no git repo above
    it.
  * **Authorisation** — only ``write_file`` to the exact leased path; never
    ``terminal`` / ``patch`` / ``delegate_task`` / a send tool, never a second
    path, never ``cross_profile``.
  * **Finalize** — 0700/0600 modes, the *exact* ``/accesses/`` ignore line,
    ignored-and-untracked verification, deterministic non-secret readback, and
    fail-closed cleanup (created files removed, pre-existing files kept).
  * **No mutating git** — ``_git`` refuses any subcommand outside the
    read-only set, so a commit/push path cannot grow here by accident.
"""

import importlib.util
import os
import stat
import subprocess
from pathlib import Path

import pytest


SECRET = "hunter2-super-secret-value"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_module():
    path = _repo_root() / "profile-plugins" / "novel-task-gate" / "accesses_fast_path.py"
    spec = importlib.util.spec_from_file_location("accesses_fast_path_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def afp():
    return _load_module()


@pytest.fixture(autouse=True)
def _isolate_hermes_home(tmp_path, monkeypatch):
    """$HERMES_HOME must never overlap the fixture repo."""
    home = tmp_path / "hermes-home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    return home


def _git(repo, *argv, check=True):
    proc = subprocess.run(
        ["git", "-C", str(repo), *argv], capture_output=True, text=True
    )
    if check and proc.returncode != 0:
        raise AssertionError(f"git {argv} failed: {proc.stderr}")
    return proc


@pytest.fixture
def repo(tmp_path):
    """A real git working tree at ``<tmp>/proj`` with one commit."""
    root = Path(os.path.realpath(tmp_path)) / "proj"
    root.mkdir()
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "t@example.com")
    _git(root, "config", "user.name", "T")
    (root / "README.md").write_text("hi\n")
    _git(root, "add", "README.md")
    _git(root, "-c", "commit.gpgsign=false", "commit", "-qm", "init")
    return root


# ---------------------------------------------------------------------------
# Minting — the positive path
# ---------------------------------------------------------------------------

def test_mint_from_explicit_credential_and_path(afp, repo):
    msg = f"вот логин и пароль от панели: admin / {SECRET}. Положи в accesses/panel.md"
    lease = afp.mint_lease(msg, cwd=str(repo))
    assert lease is not None
    assert lease.repo_root == str(repo)
    assert lease.target_path == str(repo / "accesses" / "panel.md")
    assert lease.rel_path == os.path.join("accesses", "panel.md")


@pytest.mark.parametrize(
    "token",
    ["accesses/panel.md", "./accesses/panel.md", "proj/accesses/panel.md"],
)
def test_mint_accepts_equivalent_spellings_of_one_path(afp, repo, token):
    cwd = str(repo.parent) if token.startswith("proj/") else str(repo)
    lease = afp.mint_lease(f"api key: {SECRET} -> {token}", cwd=cwd)
    assert lease is not None
    assert lease.target_path == str(repo / "accesses" / "panel.md")


def test_mint_accepts_absolute_path(afp, repo):
    target = repo / "accesses" / "panel.md"
    lease = afp.mint_lease(f"token {SECRET}, сохрани в {target}", cwd=str(repo))
    assert lease is not None and lease.target_path == str(target)


# ---------------------------------------------------------------------------
# Minting — denials (fail closed)
# ---------------------------------------------------------------------------

def test_denied_without_credential_marker(afp, repo):
    assert afp.mint_lease("запиши заметку в accesses/panel.md", cwd=str(repo)) is None


def test_denied_when_no_accesses_path_named(afp, repo):
    assert afp.mint_lease(f"вот пароль {SECRET}, сохрани куда-нибудь", cwd=str(repo)) is None


def test_denied_on_two_distinct_paths(afp, repo):
    msg = f"пароль {SECRET} -> accesses/a.md и accesses/b.md"
    assert afp.mint_lease(msg, cwd=str(repo)) is None


@pytest.mark.parametrize(
    "token",
    [
        "accesses/../.env",
        "accesses/../../secrets.md",
        "../accesses/panel.md",
        "accesses/sub/panel.md",   # `accesses` is not the immediate parent
        "vault/panel.md",          # not an accesses dir at all
        "accesses/panel.txt",      # not a .md
        "accesses/.env.md",        # dotfile basename
        "accesses/panel.md.env",   # not a .md
    ],
)
def test_denied_paths(afp, repo, token):
    assert afp.mint_lease(f"пароль {SECRET} -> {token}", cwd=str(repo)) is None


@pytest.mark.parametrize(
    "token", [".env", "config.yaml", "auth.json", "~/.ssh/id_rsa", ".hermes/auth.json"]
)
def test_denied_secret_stores(afp, repo, token):
    assert afp.mint_lease(f"пароль {SECRET} -> {token}", cwd=str(repo)) is None


def test_denied_nested_accesses_not_at_repo_root(afp, repo):
    nested = repo / "pkg" / "accesses"
    nested.mkdir(parents=True)
    msg = f"пароль {SECRET} -> {nested / 'panel.md'}"
    assert afp.mint_lease(msg, cwd=str(repo)) is None


def test_denied_when_accesses_dir_is_a_symlink(afp, repo, tmp_path):
    elsewhere = Path(os.path.realpath(tmp_path)) / "elsewhere"
    elsewhere.mkdir()
    (repo / "accesses").symlink_to(elsewhere, target_is_directory=True)
    assert afp.mint_lease(f"пароль {SECRET} -> accesses/panel.md", cwd=str(repo)) is None


def test_denied_when_target_is_a_symlink(afp, repo, tmp_path):
    (repo / "accesses").mkdir()
    outside = Path(os.path.realpath(tmp_path)) / "outside.md"
    outside.write_text("x\n")
    (repo / "accesses" / "panel.md").symlink_to(outside)
    assert afp.mint_lease(f"пароль {SECRET} -> accesses/panel.md", cwd=str(repo)) is None


def test_denied_outside_any_git_repo(afp, tmp_path):
    plain = Path(os.path.realpath(tmp_path)) / "plain"
    (plain / "accesses").mkdir(parents=True)
    assert afp.mint_lease(f"пароль {SECRET} -> accesses/panel.md", cwd=str(plain)) is None


def test_denied_inside_hermes_home(afp, tmp_path, monkeypatch):
    home = Path(os.path.realpath(tmp_path)) / "hermes-home"
    monkeypatch.setenv("HERMES_HOME", str(home))
    inner = home / "repo"
    inner.mkdir(parents=True)
    _git(inner, "init", "-q")
    assert afp.mint_lease(f"пароль {SECRET} -> accesses/panel.md", cwd=str(inner)) is None


# ---------------------------------------------------------------------------
# Authorisation
# ---------------------------------------------------------------------------

@pytest.fixture
def lease(afp, repo):
    msg = f"логин admin, пароль {SECRET} — положи в accesses/panel.md"
    lease = afp.mint_lease(msg, cwd=str(repo))
    assert lease is not None
    return lease


def test_authorizes_the_one_write(afp, repo, lease):
    args = {"path": "accesses/panel.md", "content": f"user: admin\npass: {SECRET}\n"}
    assert afp.authorizes(lease, "write_file", args, cwd=str(repo)) is True
    args_abs = {"path": lease.target_path, "content": "x"}
    assert afp.authorizes(lease, "write_file", args_abs, cwd=str(repo)) is True


@pytest.mark.parametrize(
    "tool", ["terminal", "patch", "delegate_task", "send_message", "memory_write", "process"]
)
def test_denies_every_other_tool(afp, repo, lease, tool):
    args = {"path": "accesses/panel.md", "content": "x", "command": "git commit -am x"}
    assert afp.authorizes(lease, tool, args, cwd=str(repo)) is False


@pytest.mark.parametrize(
    "args",
    [
        {"path": "accesses/other.md", "content": "x"},
        {"path": "notes/panel.md", "content": "x"},
        {"path": "../accesses/panel.md", "content": "x"},
        {"path": "accesses/panel.md", "content": ""},
        {"path": "accesses/panel.md"},
        {"path": "", "content": "x"},
        {"path": "accesses/panel.md", "content": "x", "cross_profile": True},
    ],
)
def test_denies_wrong_write_args(afp, repo, lease, args):
    assert afp.authorizes(lease, "write_file", args, cwd=str(repo)) is False


def test_lease_is_single_use(afp, repo, lease):
    args = {"path": "accesses/panel.md", "content": "x"}
    assert afp.authorizes(lease, "write_file", args, cwd=str(repo)) is True
    lease.finalized = True
    assert afp.authorizes(lease, "write_file", args, cwd=str(repo)) is False


def test_authorizes_none_lease(afp, repo):
    assert afp.authorizes(None, "write_file", {"path": "accesses/panel.md", "content": "x"},
                          cwd=str(repo)) is False


# ---------------------------------------------------------------------------
# Finalize — hardening + verification
# ---------------------------------------------------------------------------

def _write_drop(repo, body=f"user: admin\npass: {SECRET}\n"):
    d = repo / "accesses"
    d.mkdir(exist_ok=True)
    f = d / "panel.md"
    f.write_text(body)
    return f


def test_finalize_hardens_ignores_and_reads_back(afp, repo, lease):
    target = _write_drop(repo)
    assert afp.finalize(lease, preexisting=False) is True

    assert stat.S_IMODE(os.stat(repo / "accesses").st_mode) == 0o700
    assert stat.S_IMODE(os.stat(target).st_mode) == 0o600

    lines = (repo / ".gitignore").read_text().splitlines()
    assert "/accesses/" in [ln.strip() for ln in lines]
    assert _git(repo, "check-ignore", "-q", "--", str(target), check=False).returncode == 0
    assert _git(repo, "ls-files", "--error-unmatch", "--",
                str(target), check=False).returncode != 0
    assert _git(repo, "status", "--porcelain", "--", str(target)).stdout.strip() == ""

    note = afp.result_note(lease)
    assert "accesses fast-path: OK" in note
    assert "git ignored:    yes" in note
    assert "git tracked:    no" in note
    assert "0600" in note and "0700" in note


def test_readback_never_echoes_the_secret(afp, repo, lease):
    _write_drop(repo)
    assert afp.finalize(lease, preexisting=False) is True
    note = afp.result_note(lease)
    assert SECRET not in note
    assert "admin" not in note
    assert lease.rel_path in note


def test_readback_is_deterministic(afp, repo):
    msg = f"пароль {SECRET} -> accesses/panel.md"
    _write_drop(repo)
    first = afp.mint_lease(msg, cwd=str(repo))
    assert afp.finalize(first, preexisting=True) is True
    second = afp.mint_lease(msg, cwd=str(repo))
    assert afp.finalize(second, preexisting=True) is True
    assert afp.result_note(first) == afp.result_note(second)


def test_finalize_requires_the_exact_ignore_line(afp, repo, lease):
    """A near-miss pattern is not enough — the exact ``/accesses/`` must land."""
    (repo / ".gitignore").write_text("accesses/\n*.log\n")
    _write_drop(repo)
    assert afp.finalize(lease, preexisting=False) is True
    lines = [ln.strip() for ln in (repo / ".gitignore").read_text().splitlines()]
    assert "/accesses/" in lines
    assert "accesses/" in lines  # existing content preserved


def test_finalize_fails_closed_and_removes_created_file(afp, repo, lease):
    """No usable .gitignore -> the drop is deleted, not left unprotected."""
    target = _write_drop(repo)
    (repo / ".gitignore").mkdir()  # cannot hold the ignore line
    assert afp.finalize(lease, preexisting=False) is False
    assert not target.exists()
    note = afp.result_note(lease)
    assert "FAILED" in note and "file removed:   yes" in note
    assert SECRET not in note


def test_finalize_keeps_a_preexisting_file_on_failure(afp, repo, lease):
    target = _write_drop(repo)
    (repo / ".gitignore").mkdir()
    assert afp.finalize(lease, preexisting=True) is False
    assert target.exists()
    assert "left untouched" in afp.result_note(lease)


def test_finalize_fails_closed_when_file_is_tracked(afp, repo):
    d = repo / "accesses"
    d.mkdir()
    target = d / "panel.md"
    target.write_text("placeholder\n")
    _git(repo, "add", "-f", str(target))
    _git(repo, "-c", "commit.gpgsign=false", "commit", "-qm", "tracked")
    lease = afp.mint_lease(f"пароль {SECRET} -> accesses/panel.md", cwd=str(repo))
    assert lease is not None
    assert afp.finalize(lease, preexisting=True) is False
    assert target.exists()
    assert "FAILED" in afp.result_note(lease)


def test_finalize_fails_when_target_missing(afp, repo, lease):
    (repo / "accesses").mkdir()
    assert afp.finalize(lease, preexisting=False) is False
    assert "FAILED" in afp.result_note(lease)


def test_finalize_fails_when_target_became_a_symlink(afp, repo, lease, tmp_path):
    outside = Path(os.path.realpath(tmp_path)) / "outside.md"
    outside.write_text("x\n")
    d = repo / "accesses"
    d.mkdir()
    (d / "panel.md").symlink_to(outside)
    assert afp.finalize(lease, preexisting=False) is False
    assert outside.exists(), "fail-closed must not delete through a symlink"


# ---------------------------------------------------------------------------
# The module can never grow a commit/push path
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("sub", ["commit", "push", "add", "config", "remote"])
def test_git_helper_refuses_mutating_subcommands(afp, repo, sub):
    with pytest.raises(AssertionError):
        afp._git(str(repo), sub, "-m", "x")


def test_result_note_empty_before_finalize(afp, lease):
    assert afp.result_note(lease) == ""
