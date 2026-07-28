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

Everything else that *looks like* a production-write CLI (``railway``,
``vercel``, ``supabase``, ``flyctl`` …) but does not match a structured spec is
rejected as an unstructured production write — never silently passed through.
"""

from __future__ import annotations

import re
import shlex
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


def is_prod_write_class(command: str) -> bool:
    """Cheap pre-check: does this command reference a production-write CLI?

    Deliberately conservative and quote-insensitive so smuggling attempts that
    embed ``railway`` behind metacharacters still get routed into full
    classification (which then rejects them). Used by the gate to decide
    whether to engage at all.
    """
    try:
        head = command.strip().split()[0]
    except IndexError:
        return False
    base = head.rsplit("/", 1)[-1]
    if base in _PROD_CLIS:
        return True
    # Token-level scan for a prod CLI appearing anywhere (smuggled via a wrapper
    # or metacharacters). Word-boundary matched to avoid substring false hits.
    for cli in _PROD_CLIS:
        if re.search(rf"(?<![\w/-]){re.escape(cli)}(?![\w-])", command):
            return True
    return False


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
    if exe in _PROD_CLIS:
        # Recognised prod CLI without a structured spec yet: block, don't pass.
        return UnsafeCommand("unstructured-prod-write",
                             f"{exe} has no structured action spec")
    # argv[0] is not a prod CLI, but a prod token was smuggled somewhere in the
    # command (e.g. as an argument to a wrapper). Block.
    return UnsafeCommand("smuggling", f"production CLI token smuggled into {exe!r} argv")
