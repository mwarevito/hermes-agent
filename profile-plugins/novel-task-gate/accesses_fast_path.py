"""accesses_fast_path — narrowest mechanical fast path for credential drops.

The problem (P0, observed on the default/Tony profile): a request of the shape

    "вот логин/пароль от X, положи в <repo>/accesses/x.md"

is a *one-write* task, yet the ``novel-task-gate`` plugin routes it through the
full ``classify -> load skill -> discovery -> register_plan -> nonce-bound
critic -> resolve -> act`` ceremony, because ``write_file`` is a mutation and
mutations are never in the ``trivial_low_risk`` allow-list. That is five extra
API requests and a delegated critic for a file write the user spelled out
verbatim — and the ceremony itself keeps the plaintext credential alive in
context for much longer than necessary.

This module is the **narrowest** fix: a capability-bound lease, minted from the
user's own message, that authorises **exactly one** tool call — ``write_file``
to the **one** ``<repo>/accesses/<name>.md`` path the user literally named —
and then mechanically hardens and verifies the result. Everything else stays
gated.

Design mirrors ``decide_path.py``: intentionally standalone (no gate state, no
plugin imports) so it is reviewable and unit-testable in isolation, then wired
into the gate's existing hooks (``pre_llm_call`` / ``pre_tool_call`` /
``post_tool_call`` / ``transform_tool_result``) — see ``patches/``.

Security contract (all enforced here, all fail-closed):

* **Never echo the secret.** Nothing in this module reads, hashes, logs or
  returns file content. The readback is structural facts only.
* **Only the exact user-explicit repo-local accesses path.** The path must
  appear verbatim in the user's message, must resolve to
  ``<git-toplevel>/accesses/<name>.md`` with ``accesses`` as the *immediate*
  parent and directly at the repo root, and the message must resolve to
  **exactly one** such path (ambiguity = no lease).
* **Modes.** Directory ``0700``, file ``0600``, both re-``stat``-verified.
* **Exact git-ignore.** The repo's root ``.gitignore`` must contain the exact
  line ``/accesses/``; it is added if missing, then re-read, then confirmed
  through ``git check-ignore`` **and** confirmed untracked via
  ``git ls-files --error-unmatch``.
* **Deterministic non-secret readback.** Path, byte count, line counts, modes,
  ignore/track state. No timestamps, no content, no hashes of content.
* **Fail closed** for: arbitrary secret destinations, ``.env`` / config / auth
  stores, ``$HERMES_HOME``, symlinked directories or targets, ``..``
  traversal, any tool other than ``write_file``, any path other than the
  leased one, external sends, and commits/pushes (this module never invokes a
  mutating git command — only ``rev-parse``, ``check-ignore``, ``ls-files``,
  ``status``).

What stays a model judgment (mirrors the gate's own mechanical/judgment split):
whether the text the user pasted really *is* a credential. The mechanical proxy
is an explicit credential keyword; the authority it unlocks is a single write to
a file the user named by hand, so a false positive grants nothing the user did
not literally ask for.
"""

from __future__ import annotations

import logging
import os
import re
import stat as _stat
import subprocess
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# --- the one shape this fast path knows about ------------------------------
ACCESSES_DIRNAME = "accesses"
IGNORE_LINE = "/accesses/"
DIR_MODE = 0o700
FILE_MODE = 0o600
WRITE_TOOL = "write_file"

# Hard caps so a pathological message can never make the scan expensive.
MAX_MESSAGE_CHARS = 262_144
MAX_CANDIDATE_TOKENS = 4_096
GIT_TIMEOUT = 10

# Explicit "the user handed me credentials" markers. Deliberately a closed,
# bilingual list of *credential* words — not a heuristic entropy check, which
# would fire on hashes, ids and base64 blobs that are not secrets at all.
_CREDENTIAL_MARKERS = (
    r"passwo?rds?", r"passwd", r"pass-?phrase", r"login", r"log-?in",
    r"username", r"user-?name", r"api[\s_-]?keys?", r"access[\s_-]?keys?",
    r"secret[\s_-]?keys?", r"secrets?", r"tokens?", r"credentials?",
    r"private[\s_-]?keys?", r"seed[\s_-]?phrase", r"otp", r"2fa",
    r"парол[а-яё]*", r"логин[а-яё]*", r"учет[а-яё]*\s+запис[а-яё]*",
    r"учётн[а-яё]*\s+запис[а-яё]*", r"доступ[а-яё]*", r"ключ[а-яё]*",
    r"токен[а-яё]*", r"секрет[а-яё]*",
)
_CREDENTIAL_RE = re.compile(
    r"(?<![0-9A-Za-zА-Яа-яЁё_])(?:" + "|".join(_CREDENTIAL_MARKERS) + r")",
    re.IGNORECASE,
)

# Path candidates are pulled out as whitespace/punctuation-delimited tokens.
# Backticks, quotes and markdown brackets are separators so `accesses/x.md`
# and "accesses/x.md" both yield the bare token.
_TOKEN_SPLIT_RE = re.compile(r"[\s,;:'\"`()\[\]{}<>*|]+")
# Trailing sentence punctuation that can never be part of a `.md` filename.
_TRAILING_PUNCT = ".,;:!?)»…"
# A filename component we are willing to create: no dotfiles, no spaces.
_FILENAME_RE = re.compile(r"^[0-9A-Za-z_][0-9A-Za-z_.\-]*\.md$")
# Directory components allowed in a candidate path token.
_DIRPART_RE = re.compile(r"^[0-9A-Za-z_.\- ]+$")


class AccessesLease:
    """One authorised write, bound to one absolute path in one repo."""

    __slots__ = (
        "repo_root", "accesses_dir", "target_path", "rel_path",
        "preexisting", "finalized", "ok", "readback", "error", "announced",
    )

    def __init__(self, repo_root: str, accesses_dir: str, target_path: str, rel_path: str):
        self.repo_root = repo_root
        self.accesses_dir = accesses_dir
        self.target_path = target_path
        self.rel_path = rel_path
        # Filled in by the gate at admit time / by finalize().
        self.preexisting: Optional[bool] = None
        self.finalized: bool = False
        self.ok: bool = False
        self.readback: str = ""
        self.error: str = ""
        # Set by the gate once the readback has ridden back on a tool result.
        self.announced: bool = False

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"<AccessesLease {self.rel_path} repo={self.repo_root}>"


# ---------------------------------------------------------------------------
# git helpers — read-only commands only. This module NEVER runs add/commit/push.
# ---------------------------------------------------------------------------

_READ_ONLY_GIT = frozenset({"rev-parse", "check-ignore", "ls-files", "status"})


def _git(repo: str, *argv: str) -> Tuple[int, str]:
    """Run a read-only git command in *repo*. Returns ``(returncode, stdout)``.

    Any non-read-only subcommand is a programming error and raises — the
    module must never be able to grow a commit/push path by accident.
    """
    if not argv or argv[0] not in _READ_ONLY_GIT:
        raise AssertionError(f"accesses_fast_path: refusing non-read-only git: {argv!r}")
    env = dict(os.environ)
    env["GIT_OPTIONAL_LOCKS"] = "0"
    env["GIT_TERMINAL_PROMPT"] = "0"
    try:
        proc = subprocess.run(
            ["git", "-C", repo, *argv],
            capture_output=True, text=True, timeout=GIT_TIMEOUT, env=env,
        )
    except Exception as exc:
        logger.debug("accesses_fast_path: git %s failed: %s", argv[0], exc)
        return 128, ""
    return proc.returncode, (proc.stdout or "")


# ---------------------------------------------------------------------------
# Minting
# ---------------------------------------------------------------------------

def has_credential_marker(message: str) -> bool:
    """True iff the message explicitly names credential material."""
    if not isinstance(message, str) or not message.strip():
        return False
    return bool(_CREDENTIAL_RE.search(message[:MAX_MESSAGE_CHARS]))


def extract_accesses_paths(message: str) -> List[str]:
    """Return the raw ``…/accesses/<name>.md`` tokens the message spells out.

    Structural only — no filesystem access. Tokens containing ``..``, a
    backslash, shell metacharacters or a non-``accesses`` immediate parent are
    dropped here, before anything is resolved.
    """
    if not isinstance(message, str) or not message.strip():
        return []
    out: List[str] = []
    seen = set()
    for raw in _TOKEN_SPLIT_RE.split(message[:MAX_MESSAGE_CHARS])[:MAX_CANDIDATE_TOKENS]:
        tok = raw.strip().rstrip(_TRAILING_PUNCT)
        if not tok or "accesses/" not in tok or not tok.endswith(".md"):
            continue
        if ".." in tok or "\\" in tok or "$" in tok or "%" in tok:
            continue
        # Strip a leading markdown/quoting artefact that survived the split.
        while tok[:1] in "-+#":
            tok = tok[1:]
        if not tok:
            continue
        parts = tok.split("/")
        if len(parts) < 2 or parts[-2] != ACCESSES_DIRNAME:
            continue
        if not _FILENAME_RE.match(parts[-1]):
            continue
        # Every intermediate component must look like a plain directory name.
        prefix = parts[:-2]
        if prefix and prefix[0] in ("", "~", "."):
            prefix = prefix[1:]
        if any(not _DIRPART_RE.match(p) for p in prefix):
            continue
        if tok not in seen:
            seen.add(tok)
            out.append(tok)
    return out


def _resolve_candidate(token: str, cwd: str) -> Optional[str]:
    """Resolve a raw token to a normalised absolute path, or None."""
    try:
        if token.startswith("~/") or token == "~":
            path = os.path.expanduser(token)
        elif os.path.isabs(token):
            path = token
        else:
            path = os.path.join(cwd, token)
        path = os.path.normpath(path)
    except Exception:
        return None
    if not os.path.isabs(path) or ".." in path.split(os.sep):
        return None
    return path


def _hermes_home() -> str:
    raw = os.environ.get("HERMES_HOME") or os.path.join(os.path.expanduser("~"), ".hermes")
    return os.path.realpath(os.path.expanduser(raw))


def _is_within(path: str, root: str) -> bool:
    root = root.rstrip(os.sep)
    return path == root or path.startswith(root + os.sep)


def _validate_target(target: str, cwd: str) -> Optional[AccessesLease]:
    """Structural + filesystem validation of one resolved absolute path.

    Returns a lease, or None if anything at all is off (fail closed).
    """
    parent = os.path.dirname(target)
    name = os.path.basename(target)
    if os.path.basename(parent) != ACCESSES_DIRNAME or not _FILENAME_RE.match(name):
        return None

    # The path must live inside a real git working tree, and `accesses` must sit
    # directly at that tree's root — `<repo>/accesses/*.md`, nothing deeper.
    probe = parent if os.path.isdir(parent) else os.path.dirname(parent)
    if not os.path.isdir(probe):
        probe = cwd
    rc, out = _git(probe, "rev-parse", "--show-toplevel")
    if rc != 0 or not out.strip():
        return None
    repo_root = os.path.realpath(out.strip())
    if not os.path.isdir(repo_root):
        return None

    # A git repo whose root IS the home directory (or $HERMES_HOME, or the
    # filesystem root) would make far too much "repo-local". Refuse.
    home = os.path.realpath(os.path.expanduser("~"))
    hermes_home = _hermes_home()
    if repo_root in (os.sep, home, hermes_home) or _is_within(repo_root, hermes_home):
        return None

    accesses_dir = os.path.join(repo_root, ACCESSES_DIRNAME)
    # Symlinked `accesses` (or a symlinked ancestor) would let the drop land
    # outside the repo while still *looking* repo-local.
    if os.path.lexists(accesses_dir):
        if os.path.islink(accesses_dir) or not os.path.isdir(accesses_dir):
            return None
        if os.path.realpath(accesses_dir) != accesses_dir:
            return None
    if os.path.realpath(parent) != accesses_dir:
        # `parent` may not exist yet; normpath equality is then the check.
        if os.path.lexists(parent) or os.path.normpath(parent) != accesses_dir:
            return None

    final = os.path.join(accesses_dir, name)
    if os.path.lexists(final):
        if os.path.islink(final) or not os.path.isfile(final):
            return None
        if os.path.realpath(final) != final:
            return None
    if _is_within(final, hermes_home):
        return None

    return AccessesLease(
        repo_root=repo_root,
        accesses_dir=accesses_dir,
        target_path=final,
        rel_path=os.path.join(ACCESSES_DIRNAME, name),
    )


def mint_lease(user_message: str, *, cwd: Optional[str] = None) -> Optional[AccessesLease]:
    """Mint a lease from the user's own message, or return None.

    Both conditions must hold, and the path must be unambiguous:

    1. the message explicitly names credential material, and
    2. it spells out **exactly one** ``<repo>/accesses/<name>.md`` path.
    """
    try:
        if not has_credential_marker(user_message):
            return None
        tokens = extract_accesses_paths(user_message)
        if not tokens:
            return None
        cwd = os.path.realpath(cwd or os.getcwd())
        resolved: List[str] = []
        for tok in tokens:
            path = _resolve_candidate(tok, cwd)
            if path and path not in resolved:
                resolved.append(path)
        # Ambiguity is not a fast path. Exactly one destination or nothing.
        if len(resolved) != 1:
            return None
        return _validate_target(resolved[0], cwd)
    except Exception as exc:  # fail closed, never break the turn
        logger.warning("accesses_fast_path: mint failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Authorisation
# ---------------------------------------------------------------------------

def authorizes(lease: Optional[AccessesLease], tool_name: str, args: Any,
               *, cwd: Optional[str] = None) -> bool:
    """True iff this exact tool call is the one write the lease authorises.

    Anything else — ``patch``, ``terminal``, ``delegate_task``, a send tool, a
    git commit, a second file, a ``cross_profile`` opt-out — is False, and the
    normal gate applies.
    """
    try:
        if lease is None or lease.finalized:
            return False
        if tool_name != WRITE_TOOL or not isinstance(args, dict):
            return False
        if args.get("cross_profile"):
            return False
        path = args.get("path")
        content = args.get("content")
        if not isinstance(path, str) or not path.strip():
            return False
        if not isinstance(content, str) or not content:
            return False
        resolved = _resolve_candidate(path.strip(), os.path.realpath(cwd or os.getcwd()))
        if resolved is None or resolved != lease.target_path:
            return False
        # Defence in depth: re-check the shape of the resolved path itself, so a
        # lease can never authorise something structurally wrong even if the
        # filesystem changed under us between mint and call.
        parent = os.path.dirname(resolved)
        if os.path.basename(parent) != ACCESSES_DIRNAME:
            return False
        if not _FILENAME_RE.match(os.path.basename(resolved)):
            return False
        if os.path.islink(parent) or os.path.islink(resolved):
            return False
        return True
    except Exception as exc:
        logger.warning("accesses_fast_path: authorize failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Hardening + verification
# ---------------------------------------------------------------------------

def _ensure_ignore_line(repo_root: str) -> bool:
    """Guarantee the repo's root ``.gitignore`` holds the exact ignore line."""
    path = os.path.join(repo_root, ".gitignore")
    try:
        existing = ""
        if os.path.lexists(path):
            if os.path.islink(path) or not os.path.isfile(path):
                return False
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                existing = fh.read()
        if IGNORE_LINE not in [ln.strip() for ln in existing.splitlines()]:
            sep = "" if (not existing or existing.endswith("\n")) else "\n"
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(f"{sep}{IGNORE_LINE}\n")
        # Re-read: the line must be there now, exactly.
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            return IGNORE_LINE in [ln.strip() for ln in fh.read().splitlines()]
    except Exception as exc:
        logger.warning("accesses_fast_path: .gitignore update failed: %s", exc)
        return False


def _readback(lease: AccessesLease, size: int, lines: int, nonempty: int) -> str:
    return (
        "\n\n[novel-task-gate] accesses fast-path: OK (no classify/plan/critic needed)\n"
        f"  repo:           {lease.repo_root}\n"
        f"  path:           {lease.rel_path}\n"
        f"  bytes:          {size}\n"
        f"  lines:          {lines} ({nonempty} non-empty)\n"
        f"  file mode:      0{FILE_MODE:o}\n"
        f"  dir mode:       0{DIR_MODE:o}\n"
        f"  .gitignore:     {IGNORE_LINE} (present)\n"
        "  git ignored:    yes\n"
        "  git tracked:    no\n"
        "  secret echoed:  no — content was not read back, hashed or logged\n"
        "  not done here:  no commit, no push, no send. Report the path only."
    )


def _failure(lease: AccessesLease, reason: str, removed: bool) -> str:
    return (
        "\n\n[novel-task-gate] accesses fast-path: FAILED — " + reason + "\n"
        f"  path:           {lease.rel_path}\n"
        f"  file removed:   {'yes' if removed else 'no (it pre-existed — left untouched)'}\n"
        "  Nothing was committed, pushed or sent. Do NOT paste the credential "
        "into chat; fix the repo state and retry, or fall back to the normal "
        "classify -> plan -> critic path."
    )


def finalize(lease: AccessesLease, *, preexisting: Optional[bool] = None) -> bool:
    """Harden and verify the just-written drop. Fail closed.

    On success: dir ``0700``, file ``0600``, exact ignore line present, path
    confirmed ignored *and* untracked; ``lease.readback`` holds the deterministic
    non-secret summary.

    On any failure: the file is removed **iff** this fast path created it (a
    pre-existing file is never destroyed), and ``lease.error`` holds the reason.
    """
    if preexisting is None:
        preexisting = bool(lease.preexisting)
    lease.finalized = True
    target = lease.target_path
    reason = ""
    try:
        while True:
            if os.path.islink(target) or not os.path.isfile(target):
                reason = "target is missing or not a regular file"
                break
            if os.path.realpath(target) != target:
                reason = "target resolves through a symlink"
                break
            if not os.path.isdir(lease.accesses_dir) or os.path.islink(lease.accesses_dir):
                reason = "accesses directory is missing or a symlink"
                break

            os.chmod(lease.accesses_dir, DIR_MODE)
            os.chmod(target, FILE_MODE)

            if not _ensure_ignore_line(lease.repo_root):
                reason = f"could not guarantee the exact '{IGNORE_LINE}' line in .gitignore"
                break

            rc, _ = _git(lease.repo_root, "check-ignore", "-q", "--", target)
            if rc != 0:
                reason = "git does not consider the file ignored"
                break
            rc, _ = _git(lease.repo_root, "ls-files", "--error-unmatch", "--", target)
            if rc == 0:
                reason = "the file is TRACKED by git — a credential must never be tracked"
                break
            rc, out = _git(lease.repo_root, "status", "--porcelain", "--", target)
            if rc != 0 or out.strip():
                reason = "git status still reports the path (not cleanly ignored)"
                break

            dmode = _stat.S_IMODE(os.stat(lease.accesses_dir).st_mode)
            fmode = _stat.S_IMODE(os.stat(target).st_mode)
            if dmode != DIR_MODE or fmode != FILE_MODE:
                reason = f"mode verification failed (dir 0{dmode:o}, file 0{fmode:o})"
                break

            size = os.path.getsize(target)
            with open(target, "r", encoding="utf-8", errors="replace") as fh:
                raw = fh.read()
            lines = len(raw.splitlines())
            nonempty = sum(1 for ln in raw.splitlines() if ln.strip())
            del raw  # never retained, never logged, never returned
            lease.ok = True
            lease.readback = _readback(lease, size, lines, nonempty)
            return True
    except Exception as exc:
        reason = f"internal error: {type(exc).__name__}"
        logger.warning("accesses_fast_path: finalize error: %s", exc)

    removed = False
    if not preexisting:
        try:
            if os.path.isfile(target) and not os.path.islink(target):
                os.remove(target)
                removed = True
        except Exception as exc:  # pragma: no cover - best effort
            logger.warning("accesses_fast_path: cleanup failed: %s", exc)
    lease.ok = False
    lease.error = _failure(lease, reason or "unknown", removed)
    return False


def result_note(lease: Optional[AccessesLease]) -> str:
    """The block to append to the ``write_file`` result, once, after finalize."""
    if lease is None or not lease.finalized:
        return ""
    return lease.readback if lease.ok else lease.error
