"""Typed production-write action classifier.

The gate never approves a *shell command string*; it approves a *typed action*
whose executable, argv, cwd and immutable target ids are structurally proven.
This module turns a terminal ``command`` string into either a
:class:`ProdAction` (a flat, metacharacter-free argv mapped to a known action
class) or an :class:`UnsafeCommand` rejection.

The grammar is deliberately tiny: a single simple command, no shell
indirection of any kind. Anything that a POSIX shell would treat as more than
one literal executable + literal arguments is rejected. This is what lets us
prove that the string the model re-issues after approval is byte-equivalent to
the argv that was approved: with every metacharacter banned, ``shlex.split`` is
a total, deterministic function of the string.

Supported action classes (minimum required set):

* ``railway_variable_set``  — ``railway variables --set KEY=VALUE ...``
* ``railway_restart``       — ``railway redeploy|restart --service <id> ...``
* ``railway_readonly_verify`` — ``railway variables`` / ``railway status`` (reads)
* ``vercel_prod_deploy``    — ``vercel deploy --prod --yes --scope <team id>``
  where ``--scope`` must equal the immutable team id of the linked project
  metadata (``.vercel/project.json``) under the *canonical, existing* cwd
  (no symlinked cwd or metadata components); the cwd and both immutable ids
  (project + team) are bound as targets.
* ``vercel_readonly_verify`` — narrowly-specified reads: ``vercel whoami``,
  ``vercel project ls``, ``vercel ls``, ``vercel inspect <deployment>``
  (each with at most an optional immutable ``--scope <team id>``).
* ``npm_install_hosting_cli`` — ``npm install --global
  --registry=https://registry.npmjs.org/ vercel@X.Y.Z``: the one canonical
  argv — exact tokens in exact order, ``--global`` spelled out, official
  registry with trailing slash pinned on the command line, exact numeric
  semver (no ``-g``, reordered forms, dist-tags, ranges, extra flags, or
  custom registries); a write action so the one-time
  CLI install is approvable without weakening the prod-token smuggling scan
  for every other ``npm`` form.

Everything else that *looks like* a production-write CLI (``railway``,
``vercel``, ``supabase``, ``flyctl`` …) but does not match a structured spec is
rejected as an unstructured production write — never silently passed through.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import stat
from dataclasses import dataclass, field
from typing import Optional, Tuple

# --- classification results ------------------------------------------------


@dataclass(frozen=True)
class ProdAction:
    """A structurally-validated production-write (or read-verify) action."""

    action_class: str
    executable: str
    argv: Tuple[str, ...]
    cwd: str
    targets: Tuple[Tuple[str, str], ...]  # sorted (kind, id) pairs, immutable
    # Indices into ``argv`` whose *value half* is secret (redacted everywhere).
    secret_positions: Tuple[int, ...] = ()
    read_only: bool = False

    @property
    def is_action(self) -> bool:
        return True


@dataclass(frozen=True)
class UnsafeCommand:
    """A command that touches a production-write class but cannot be proven safe."""

    reason: str
    detail: str = ""

    @property
    def is_action(self) -> bool:
        return False


# ``None`` = not a production-write class at all (gate passes through).
Classification = Optional[object]  # ProdAction | UnsafeCommand | None


# --- shell-safety scanner --------------------------------------------------

# Any of these characters, appearing outside a single-quoted region, means the
# string is more than one literal command + literal args. We reject the whole
# command rather than try to reason about it. ``$`` and backtick are banned even
# inside double quotes because a shell still expands them there.
_FORBIDDEN_OUTSIDE_SQUOTE = set(";|&<>`$()[]{}*?~!#\n\r\t\\")


class ShellIndirection(Exception):
    """Raised when the command is not a single flat literal argv."""


def scan_shell_safe(command: str) -> None:
    """Reject any shell indirection. Raises :class:`ShellIndirection` on failure.

    Walks the string tracking single/double-quote state. A metacharacter is
    only ever allowed inside a single-quoted region (where the shell treats it
    literally). Double quotes still expand ``$`` / backtick, so those stay
    banned even there.
    """
    if not command or not command.strip():
        raise ShellIndirection("empty command")
    in_single = False
    in_double = False
    for ch in command:
        if in_single:
            if ch == "'":
                in_single = False
            continue
        if in_double:
            # Inside double quotes the shell still performs $ and ` expansion.
            if ch in "$`\\":
                raise ShellIndirection(f"expansion character {ch!r} inside double quotes")
            if ch == '"':
                in_double = False
            continue
        if ch == "'":
            in_single = True
            continue
        if ch == '"':
            in_double = True
            continue
        if ch in _FORBIDDEN_OUTSIDE_SQUOTE:
            raise ShellIndirection(f"shell metacharacter {ch!r}")
    if in_single or in_double:
        raise ShellIndirection("unterminated quote")


def tokenize(command: str) -> Tuple[str, ...]:
    """Shell-safe scan + POSIX tokenize into a flat argv.

    Because ``scan_shell_safe`` has already banned every operator/expansion,
    ``shlex.split`` is a total deterministic function here: the same string
    always yields the same argv, and that argv contains no smuggled shell
    behaviour.
    """
    scan_shell_safe(command)
    try:
        argv = shlex.split(command, posix=True)
    except ValueError as exc:  # unbalanced quotes shlex catches that we don't
        raise ShellIndirection(f"tokenization failed: {exc}") from exc
    if not argv:
        raise ShellIndirection("no tokens")
    # Control characters (incl. quoted newlines/tabs, which the metacharacter
    # scan cannot see inside single quotes) never belong in a provable argv.
    for tok in argv:
        if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in tok):
            raise ShellIndirection("control character in token")
    # Reject a leading VAR=value environment-assignment prefix (a shell feature,
    # not an argument to the executable).
    if "=" in argv[0]:
        raise ShellIndirection("environment-assignment prefix")
    # Reject wrapper executables that would re-enter a shell / run arbitrary code.
    if argv[0] in _WRAPPER_EXECUTABLES:
        raise ShellIndirection(f"wrapper executable {argv[0]!r}")
    return tuple(argv)


_WRAPPER_EXECUTABLES = {
    "sh", "bash", "zsh", "dash", "fish", "ksh", "csh", "tcsh",
    "env", "eval", "exec", "sudo", "doas", "nohup", "time", "timeout",
    "xargs", "nice", "setsid", "watch", "script", "stdbuf", "command",
    "python", "python3", "node", "ruby", "perl", "ssh", "docker",
}


# --- production-write class detection --------------------------------------

# Executables that mutate production infrastructure. Presence of one of these
# as argv[0] means the gate MUST resolve to a structured action or block; it is
# never passed through.
_PROD_CLIS = {"railway", "vercel", "supabase", "flyctl", "fly"}

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def _is_immutable_id(value: str) -> bool:
    """Immutable target ids are UUIDs. Human-facing names are mutable aliases
    (a service can be renamed / re-pointed) and are rejected as targets."""
    return bool(_UUID_RE.match(value))


def _token_references_prod_cli(tok: str) -> bool:
    if tok.rsplit("/", 1)[-1] in _PROD_CLIS:
        return True
    for cli in _PROD_CLIS:
        if re.search(rf"(?<![\w/-]){re.escape(cli)}(?![\w-])", tok):
            return True
    return False


def is_prod_write_class(command: str) -> bool:
    """Cheap pre-check: does this command reference a production-write CLI?

    Two complementary conservative passes. The raw quote-insensitive scan
    catches prod tokens smuggled behind metacharacters (strings shlex cannot
    or should not be trusted to tokenize). The tokenization-aware pass catches
    what the raw scan cannot see: POSIX quote concatenation (``ver'cel'``)
    and quoted executable paths (``'/usr/bin/vercel'``) reassemble into a prod
    token only after unquoting. Either pass matching admits the command into
    full classification (which then rejects every unsafe form) — the passes
    only ever add detections, never subtract.
    """
    try:
        head = command.strip().split()[0]
    except IndexError:
        return False
    base = head.rsplit("/", 1)[-1]
    if base in _PROD_CLIS:
        return True
    # Raw scan: a prod CLI token anywhere in the un-tokenized string
    # (word-boundary matched to avoid substring false hits).
    for cli in _PROD_CLIS:
        if re.search(rf"(?<![\w/-]){re.escape(cli)}(?![\w-])", command):
            return True
    # Tokenization-aware scan on the unquoted argv.
    try:
        tokens = shlex.split(command, posix=True)
    except ValueError:
        return False  # unbalanced quotes; raw scan above already had its say
    return any(_token_references_prod_cli(tok) for tok in tokens)


# --- railway argv parsers --------------------------------------------------


def _parse_flags(argv: Tuple[str, ...], names: set) -> Tuple[dict, list, list]:
    """Split argv[2:] into (single-valued flags, --set pairs, positionals).

    Only ``--flag value`` (space-separated) form is accepted. ``--flag=value``
    is rejected upstream by the ``=`` scan for non ``--set`` flags; for ``--set``
    we require ``--set KEY=VALUE``.
    """
    flags: dict = {}
    sets: list = []
    positionals: list = []
    i = 0
    body = list(argv)
    while i < len(body):
        tok = body[i]
        if tok == "--set":
            if i + 1 >= len(body):
                raise ShellIndirection("--set without KEY=VALUE")
            pair = body[i + 1]
            if "=" not in pair or pair.startswith("="):
                raise ShellIndirection("--set requires KEY=VALUE")
            key, _, _val = pair.partition("=")
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
                raise ShellIndirection(f"invalid variable name {key!r}")
            sets.append((key, i + 1))  # remember argv index of the KEY=VALUE token
            i += 2
            continue
        if tok in names:
            if i + 1 >= len(body):
                raise ShellIndirection(f"{tok} without value")
            flags[tok] = (body[i + 1], i + 1)
            i += 2
            continue
        if tok in _RAILWAY_BOOL_FLAGS:
            flags[tok] = (True, i)
            i += 1
            continue
        if tok.startswith("-"):
            raise ShellIndirection(f"unrecognized flag {tok!r}")
        positionals.append((tok, i))
        i += 1
    return flags, sets, positionals


_RAILWAY_VALUE_FLAGS = {"--service", "--project", "--environment", "-s", "-p", "-e"}
_RAILWAY_BOOL_FLAGS = {"--yes", "-y", "--kv", "--json"}
_FLAG_CANON = {"-s": "--service", "-p": "--project", "-e": "--environment"}


def _extract_targets(flags: dict) -> Tuple[Tuple[Tuple[str, str], ...], Tuple[int, ...]]:
    targets = {}
    positions = []
    for raw, canon in (("--service", "service"), ("-s", "service"),
                       ("--project", "project"), ("-p", "project"),
                       ("--environment", "environment"), ("-e", "environment")):
        if raw in flags:
            value, idx = flags[raw]
            if not _is_immutable_id(value):
                raise ShellIndirection(
                    f"target {canon}={value!r} is not an immutable id (UUID required)"
                )
            if canon in targets and targets[canon] != value:
                raise ShellIndirection(f"conflicting {canon} targets")
            targets[canon] = value
            positions.append(idx)
    return tuple(sorted(targets.items())), tuple(sorted(positions))


def _classify_railway(argv: Tuple[str, ...], cwd: str) -> object:
    if len(argv) < 2:
        return UnsafeCommand("unstructured-prod-write", "railway needs a subcommand")
    sub = argv[1]

    if sub == "variables":
        try:
            flags, sets, positionals = _parse_flags(argv[2:], _RAILWAY_VALUE_FLAGS)
            targets, _ = _extract_targets(flags)
        except ShellIndirection as exc:
            return UnsafeCommand("unstructured-prod-write", str(exc))
        # Positional args to `variables` (legacy `set K V`) are not supported —
        # only the explicit `--set KEY=VALUE` form, which is unambiguous.
        if positionals:
            return UnsafeCommand("unstructured-prod-write",
                                 "positional args to `railway variables` not allowed")
        if sets:
            # variable-set (write) — requires an immutable service target.
            if not any(k == "service" for k, _ in targets):
                return UnsafeCommand("ambiguous-target",
                                     "railway variable-set requires --service <uuid>")
            # secret positions: the argv index of each KEY=VALUE token (value redacted).
            secret_positions = tuple(sorted(idx for _k, idx in
                                            ((k, pos + 2) for k, pos in sets)))
            return ProdAction(
                action_class="railway_variable_set",
                executable="railway",
                argv=argv,
                cwd=cwd,
                targets=targets,
                secret_positions=secret_positions,
                read_only=False,
            )
        # No --set → read-only variable listing.
        return ProdAction(
            action_class="railway_readonly_verify",
            executable="railway",
            argv=argv,
            cwd=cwd,
            targets=targets,
            read_only=True,
        )

    if sub in ("redeploy", "restart"):
        try:
            flags, sets, positionals = _parse_flags(argv[2:], _RAILWAY_VALUE_FLAGS)
            targets, _ = _extract_targets(flags)
        except ShellIndirection as exc:
            return UnsafeCommand("unstructured-prod-write", str(exc))
        if sets or positionals:
            return UnsafeCommand("unstructured-prod-write",
                                 "restart takes no positional args")
        if not any(k == "service" for k, _ in targets):
            return UnsafeCommand("ambiguous-target",
                                 "railway restart requires --service <uuid>")
        return ProdAction(
            action_class="railway_restart",
            executable="railway",
            argv=argv,
            cwd=cwd,
            targets=targets,
            read_only=False,
        )

    if sub == "status":
        return ProdAction(
            action_class="railway_readonly_verify",
            executable="railway",
            argv=argv,
            cwd=cwd,
            targets=(),
            read_only=True,
        )

    # `railway run`, `railway up`, `railway down`, `railway delete`, `railway link`
    # etc. — arbitrary-execution wrappers or unstructured mutations. Never
    # passed through, never approvable as a typed action.
    return UnsafeCommand("unstructured-prod-write", f"railway {sub} is not a structured action")


# --- vercel (hosting CLI) argv parsers --------------------------------------

# Immutable Vercel ids. Human-facing project/team *names* are mutable aliases
# and are never accepted as targets or scopes.
_VERCEL_PROJECT_ID_RE = re.compile(r"^prj_[A-Za-z0-9]{8,64}$")
_VERCEL_TEAM_ID_RE = re.compile(r"^team_[A-Za-z0-9]{8,64}$")
# A deployment reference for `inspect`: immutable dpl_ id or a concrete
# *.vercel.app deployment URL (optionally https://-prefixed).
_VERCEL_DEPLOYMENT_RE = re.compile(
    r"^(?:https://)?(?:dpl_[A-Za-z0-9]{8,64}|[a-z0-9][a-z0-9.-]{0,250}\.vercel\.app)$"
)


def _read_linked_project(cwd: str) -> Tuple[str, str]:
    """Read the linked-project metadata under ``cwd`` (generic — any project).

    Requires a canonical (symlink-free, ``realpath``-identical), existing,
    absolute cwd whose ``.vercel/project.json`` is a regular file reached
    through no symlinked component (opened ``O_NOFOLLOW`` where supported),
    holding immutable ``projectId`` (prj_…) and ``orgId`` (team_…). Anything
    missing, unreadable, symlinked, or mutable-looking raises
    :class:`ShellIndirection` so the caller rejects the write.

    Re-read on every classification: metadata drift between approval and the
    gate's execution-time re-classification changes the targets — and thus the
    grant fingerprint — so the stale approval no longer matches. That binding
    holds at the local-host trust boundary only: a concurrent local process
    with write access could still swap the file between this read and the
    hosting CLI's own read, which no userspace check here can prevent.
    """
    if not cwd or not os.path.isabs(cwd):
        raise ShellIndirection(f"cwd must be an absolute path, got {cwd!r}")
    if os.path.realpath(cwd) != cwd:
        raise ShellIndirection(
            f"cwd must be a canonical real path (no symlinked components): {cwd!r}"
        )
    if not os.path.isdir(cwd):
        raise ShellIndirection(f"cwd does not exist: {cwd!r}")
    meta_path = os.path.join(cwd, ".vercel", "project.json")
    # cwd is canonical, so any symlink in the .vercel/project.json chain makes
    # realpath diverge; O_NOFOLLOW additionally refuses a symlinked final
    # component at open time on platforms that support it.
    if os.path.realpath(meta_path) != meta_path:
        raise ShellIndirection(
            "linked-project metadata path contains a symlinked component"
        )
    try:
        fd = os.open(meta_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except FileNotFoundError:
        raise ShellIndirection(
            "cwd is not a linked project (.vercel/project.json missing)"
        ) from None
    except OSError as exc:
        raise ShellIndirection(f"linked-project metadata unreadable: {exc}") from exc
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ShellIndirection("linked-project metadata is not a regular file")
        fh = os.fdopen(fd, "r", encoding="utf-8")
    except BaseException:
        os.close(fd)
        raise
    try:
        with fh:
            meta = json.load(fh)
    except (OSError, ValueError) as exc:
        raise ShellIndirection(f"linked-project metadata unreadable: {exc}") from exc
    if not isinstance(meta, dict):
        raise ShellIndirection("linked-project metadata is not an object")
    project_id = meta.get("projectId")
    org_id = meta.get("orgId")
    if not isinstance(project_id, str) or not _VERCEL_PROJECT_ID_RE.fullmatch(project_id):
        raise ShellIndirection("linked projectId is not an immutable prj_ id")
    if not isinstance(org_id, str) or not _VERCEL_TEAM_ID_RE.fullmatch(org_id):
        raise ShellIndirection("linked orgId is not an immutable team_ id")
    return project_id, org_id


def _parse_optional_scope(rest: Tuple[str, ...]) -> Tuple[Tuple[str, str], ...]:
    """``rest`` must be empty or exactly ``--scope <team id>``. Anything else
    (extra flags, positionals, mutable scope names) raises."""
    if not rest:
        return ()
    if len(rest) == 2 and rest[0] == "--scope" and _VERCEL_TEAM_ID_RE.fullmatch(rest[1]):
        return (("team", rest[1]),)
    raise ShellIndirection(
        "only an optional `--scope <team id>` is allowed on this read-only form"
    )


def _vercel_readonly(argv: Tuple[str, ...], cwd: str,
                     targets: Tuple[Tuple[str, str], ...]) -> ProdAction:
    return ProdAction(
        action_class="vercel_readonly_verify",
        executable="vercel",
        argv=argv,
        cwd=cwd,
        targets=targets,
        read_only=True,
    )


def _classify_vercel(argv: Tuple[str, ...], cwd: str) -> object:
    if len(argv) < 2:
        return UnsafeCommand("unstructured-prod-write", "vercel needs a subcommand")
    sub = argv[1]

    if sub == "deploy":
        # Exact production deploy grammar (order-insensitive, nothing extra):
        #   vercel deploy --prod --yes --scope <team id>
        body = argv[2:]
        seen: dict = {}
        scope = None
        i = 0
        while i < len(body):
            tok = body[i]
            if tok in ("--prod", "--yes"):
                if tok in seen:
                    return UnsafeCommand("unstructured-prod-write", f"duplicate {tok}")
                seen[tok] = True
                i += 1
                continue
            if tok == "--scope":
                if scope is not None:
                    return UnsafeCommand("unstructured-prod-write", "duplicate --scope")
                if i + 1 >= len(body):
                    return UnsafeCommand("unstructured-prod-write", "--scope without value")
                scope = body[i + 1]
                i += 2
                continue
            # Any other flag (--token, --env, --prebuilt, --force, …) or any
            # positional is outside the production grammar.
            return UnsafeCommand(
                "unstructured-prod-write",
                f"{tok!r} is not part of the production deploy grammar "
                "(vercel deploy --prod --yes --scope <team id>)",
            )
        if "--prod" not in seen:
            return UnsafeCommand("unstructured-prod-write",
                                 "preview deploys are not allowed; --prod is required")
        if "--yes" not in seen:
            return UnsafeCommand("unstructured-prod-write",
                                 "production deploy grammar requires --yes")
        if scope is None:
            return UnsafeCommand("ambiguous-target",
                                 "production deploy requires --scope <team id>")
        if not _VERCEL_TEAM_ID_RE.fullmatch(scope):
            return UnsafeCommand(
                "unstructured-prod-write",
                f"--scope {scope!r} is not an immutable team id (team_… required)",
            )
        try:
            project_id, org_id = _read_linked_project(cwd)
        except ShellIndirection as exc:
            return UnsafeCommand("unstructured-prod-write", str(exc))
        if scope != org_id:
            return UnsafeCommand(
                "ambiguous-target",
                "--scope does not match the linked project's immutable team id",
            )
        return ProdAction(
            action_class="vercel_prod_deploy",
            executable="vercel",
            argv=argv,
            cwd=cwd,
            targets=tuple(sorted((("cwd", cwd),
                                  ("project", project_id),
                                  ("team", org_id)))),
            read_only=False,
        )

    # Narrowly-specified read-only forms. Each accepts at most an optional
    # immutable `--scope <team id>`; anything else is rejected.
    try:
        if sub == "whoami":
            if len(argv) != 2:
                raise ShellIndirection("`vercel whoami` takes no arguments")
            return _vercel_readonly(argv, cwd, ())
        if sub == "project":
            if len(argv) < 3 or argv[2] != "ls":
                raise ShellIndirection("only `vercel project ls` is a read-only form")
            return _vercel_readonly(argv, cwd, _parse_optional_scope(argv[3:]))
        if sub == "ls":
            return _vercel_readonly(argv, cwd, _parse_optional_scope(argv[2:]))
        if sub == "inspect":
            if len(argv) < 3:
                raise ShellIndirection("`vercel inspect` needs a deployment id/URL")
            dep = argv[2]
            if not _VERCEL_DEPLOYMENT_RE.fullmatch(dep):
                raise ShellIndirection(
                    f"{dep!r} is not a dpl_ id or *.vercel.app deployment URL"
                )
            return _vercel_readonly(argv, cwd, _parse_optional_scope(argv[3:]))
    except ShellIndirection as exc:
        return UnsafeCommand("unstructured-prod-write", str(exc))

    # `vercel env`, `alias`, `rollback`, `promote`, `link`, `pull`, `rm`, bare
    # `vercel --prod`, etc. — unstructured mutations or alias/rollback surfaces.
    # Never passed through, never approvable as a typed action.
    return UnsafeCommand("unstructured-prod-write",
                         f"vercel {sub} is not a structured action")


# --- npm one-time hosting-CLI install ----------------------------------------

_SEMVER_EXACT_RE = re.compile(r"^\d+\.\d+\.\d+$")
_HOSTING_CLI_PACKAGE = "vercel"
# The registry is pinned on the command line (highest-precedence npm config),
# so a user/project .npmrc pointing at a custom registry cannot redirect the
# package. Exactly this single-token official-registry form (trailing slash
# included) — no no-slash or space-separated variants.
_NPM_OFFICIAL_REGISTRY_FLAG = "--registry=https://registry.npmjs.org/"
# The one canonical argv prefix: exact tokens in exact order, followed only by
# the exact-semver package spec.
_NPM_CANONICAL_PREFIX = ("npm", "install", "--global", _NPM_OFFICIAL_REGISTRY_FLAG)


def _classify_npm(argv: Tuple[str, ...], cwd: str) -> object:
    """Only the one canonical global-install argv of the official hosting CLI —
    ``npm install --global --registry=https://registry.npmjs.org/
    vercel@X.Y.Z`` — exact tokens, exact order — is a typed (approvable) write
    action; every other npm invocation that carries a prod CLI token stays a
    smuggling rejection, so the token scan is not weakened."""
    if len(argv) == 5 and argv[:4] == _NPM_CANONICAL_PREFIX:
        spec = argv[4]
        name, sep, version = spec.partition("@")
        if name == _HOSTING_CLI_PACKAGE and sep:
            if _SEMVER_EXACT_RE.fullmatch(version):
                return ProdAction(
                    action_class="npm_install_hosting_cli",
                    executable="npm",
                    argv=argv,
                    cwd=cwd,
                    targets=(("package", spec),),
                    read_only=False,
                )
            return UnsafeCommand(
                "unstructured-prod-write",
                "hosting CLI install requires an exact numeric semver "
                f"({_HOSTING_CLI_PACKAGE}@X.Y.Z) — no dist-tags or ranges",
            )
    return UnsafeCommand(
        "smuggling",
        "npm with a production CLI token is only approvable as the canonical "
        f"`npm install --global {_NPM_OFFICIAL_REGISTRY_FLAG} "
        f"{_HOSTING_CLI_PACKAGE}@X.Y.Z` (exact tokens, exact order)",
    )


def classify(command: str, cwd: str = "") -> Classification:
    """Classify a terminal command.

    Returns ``None`` if the command is not a production-write class (the gate
    passes it through untouched), a :class:`ProdAction` if it is a structurally
    proven action, or an :class:`UnsafeCommand` if it touches a production-write
    class but cannot be proven safe (the gate blocks it).
    """
    if not is_prod_write_class(command):
        return None
    # From here the command references a production-write CLI: it must resolve
    # to a structured action or be rejected. Never pass through.
    try:
        argv = tokenize(command)
    except ShellIndirection as exc:
        return UnsafeCommand("shell-indirection", str(exc))

    exe = argv[0].rsplit("/", 1)[-1]
    if exe == "railway":
        return _classify_railway(argv, cwd)
    if exe == "vercel":
        # Exact bare executable identity: an arbitrary path whose basename
        # happens to be "vercel" (/tmp/evil/vercel) is not the hosting CLI.
        if argv[0] != "vercel":
            return UnsafeCommand(
                "unstructured-prod-write",
                f"hosting CLI must be invoked as bare 'vercel', not {argv[0]!r}",
            )
        return _classify_vercel(argv, cwd)
    if exe == "npm":
        # Reached only when a prod CLI token appears in the command (the
        # is_prod_write_class pre-check); plain npm commands pass through above.
        if argv[0] != "npm":
            return UnsafeCommand(
                "unstructured-prod-write",
                f"npm must be invoked as bare 'npm', not {argv[0]!r}",
            )
        return _classify_npm(argv, cwd)
    if exe in _PROD_CLIS:
        # Recognised prod CLI without a structured spec yet: block, don't pass.
        return UnsafeCommand("unstructured-prod-write",
                             f"{exe} has no structured action spec")
    # argv[0] is not a prod CLI, but a prod token was smuggled somewhere in the
    # command (e.g. as an argument to a wrapper). Block.
    return UnsafeCommand("smuggling", f"production CLI token smuggled into {exe!r} argv")
