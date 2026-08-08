"""A test may never git-checkout or reset a REAL hermes clone.

2026-08-08, twice in one day. At 13:18 a full ``tests/hermes_cli`` run whose cwd
was inside ``~/.hermes/hermes-agent`` executed the update-machinery tests, which
performed a real ``checkout main`` + ``reset --hard origin/main`` there: 23 local
patches gone from disk, the gateway restarted onto upstream code 27 seconds
later, and the Telegram Business secretary dead for six hours before a human
noticed. At 23:46 the identical accident happened again, from the same command,
run deliberately to attribute red tests against a clean base.

Twice in one day by two different intentions is a missing guard, not
carelessness. These tests pin both sides of it: the refusal, and — just as
important — that ordinary git use and throwaway repos stay untouched, because a
guard that blocks the legitimate case gets disabled within a week.
"""
import os

import pytest

from tests.conftest import (
    _hermes_git_mutation_target,
    _hermes_repo_mutation_refusal,
)


ROOT = "/Users/tony/.hermes/hermes-agent"


@pytest.fixture
def roots(tmp_path):
    """A protected root that actually exists, so realpath comparisons are real."""
    repo = tmp_path / "protected"
    (repo / ".git").mkdir(parents=True)
    return [os.path.realpath(str(repo))]


# --- what must be refused ---------------------------------------------------

@pytest.mark.parametrize("verb", ["checkout", "switch", "reset", "clean", "restore", "stash"])
def test_every_destructive_verb_is_refused(roots, verb):
    msg = _hermes_repo_mutation_refusal(["git", verb, "main"], roots[0], roots)
    assert msg and "repo-mutation guard" in msg


def test_the_exact_incident_command_is_refused(roots):
    assert _hermes_repo_mutation_refusal(
        ["git", "reset", "--hard", "origin/main"], roots[0], roots
    )


def test_dash_C_overrides_a_harmless_cwd(roots, tmp_path):
    """`git -C <protected> reset` from a safe cwd is still an attack on the repo."""
    assert _hermes_repo_mutation_refusal(
        ["git", "-C", roots[0], "reset", "--hard"], str(tmp_path), roots
    )


def test_a_subdirectory_of_a_protected_repo_is_protected(roots):
    inner = os.path.join(roots[0], "hermes_cli")
    assert _hermes_repo_mutation_refusal(["git", "checkout", "main"], inner, roots)


def test_a_shell_string_command_is_inspected(roots):
    assert _hermes_repo_mutation_refusal("git checkout main", roots[0], roots)


def test_env_and_sudo_wrappers_do_not_hide_it(roots):
    assert _hermes_repo_mutation_refusal(["env", "git", "checkout", "main"], roots[0], roots)
    assert _hermes_repo_mutation_refusal(["sudo", "git", "reset", "--hard"], roots[0], roots)
    assert _hermes_repo_mutation_refusal(
        ["env", "GIT_DIR=x", "git", "checkout", "main"], roots[0], roots
    )


def test_global_flags_before_the_verb_do_not_hide_it(roots):
    assert _hermes_repo_mutation_refusal(
        ["git", "-c", "core.hooksPath=/dev/null", "checkout", "main"], roots[0], roots
    )


def test_the_message_says_what_happened_not_just_that_it_is_forbidden(roots):
    msg = _hermes_repo_mutation_refusal(["git", "reset", "--hard"], roots[0], roots)
    assert "2026-08-08" in msg
    assert "scratch copy" in msg
    assert "HERMES_TESTS_ALLOW_REPO_MUTATION" in msg


# --- what must stay allowed -------------------------------------------------

def test_a_throwaway_repo_is_untouched(roots, tmp_path):
    """The updater's own tests build a repo under tmp_path — they must still run."""
    assert _hermes_repo_mutation_refusal(
        ["git", "reset", "--hard"], str(tmp_path), roots
    ) is None


def test_read_only_git_is_allowed_inside_a_protected_repo(roots):
    for cmd in (
        ["git", "status", "--porcelain"],
        ["git", "log", "--oneline", "-1"],
        ["git", "rev-parse", "--show-toplevel"],
        ["git", "diff"],
        ["git", "worktree", "list"],
    ):
        assert _hermes_repo_mutation_refusal(cmd, roots[0], roots) is None, cmd


def test_non_git_commands_are_allowed(roots):
    assert _hermes_repo_mutation_refusal(["rm", "-rf", "checkout"], roots[0], roots) is None
    assert _hermes_repo_mutation_refusal(["python", "-m", "pytest"], roots[0], roots) is None


def test_a_program_merely_named_like_git_is_not_git(roots):
    assert _hermes_repo_mutation_refusal(["gitk", "checkout"], roots[0], roots) is None


def test_no_protected_roots_means_no_refusal(tmp_path):
    assert _hermes_repo_mutation_refusal(["git", "reset", "--hard"], str(tmp_path), []) is None


def test_empty_and_odd_commands_do_not_crash(roots):
    assert _hermes_repo_mutation_refusal([], roots[0], roots) is None
    assert _hermes_repo_mutation_refusal(None, roots[0], roots) is None
    assert _hermes_repo_mutation_refusal(42, roots[0], roots) is None
    assert _hermes_repo_mutation_refusal("git checkout 'unclosed", roots[0], roots) is None


def test_target_resolution_prefers_dash_C_over_cwd(tmp_path):
    target = _hermes_git_mutation_target(
        ["git", "-C", "/somewhere/else", "checkout", "main"], str(tmp_path)
    )
    assert target == "/somewhere/else"


def test_target_resolution_falls_back_to_cwd(tmp_path):
    assert _hermes_git_mutation_target(["git", "checkout", "main"], str(tmp_path)) == str(tmp_path)


def test_a_read_only_verb_has_no_target(tmp_path):
    assert _hermes_git_mutation_target(["git", "status"], str(tmp_path)) is None
