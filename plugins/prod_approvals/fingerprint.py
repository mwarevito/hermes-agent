"""Keyed fingerprints and secret redaction.

Two hard rules:

* No secret value (a Railway variable VALUE, a token) ever appears in a
  fingerprint, an audit row, or any string returned to the model / user. Secret
  halves of ``KEY=VALUE`` tokens are replaced by a keyed HMAC tag so identity is
  preserved (same value → same tag) without disclosure.
* The fingerprint binds the *action* to the *authoritative context*. A grant
  approved for one (action, task, run, claim, chat, thread, user, profile) can
  never satisfy a request with any of those changed.

The HMAC key lives in a 0600 file under ``HERMES_HOME/prod_approvals`` and is
generated once. Rotating (deleting) it invalidates every outstanding grant —
old fingerprints stop matching — which is exactly the desired fail-closed
behaviour on key loss.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Tuple

if TYPE_CHECKING:  # avoid import cycle at runtime
    from plugins.prod_approvals.actions import ProdAction


def _tag(key: bytes, *, domain: str, value: str) -> str:
    mac = hmac.new(key, f"{domain}\x00{value}".encode("utf-8"), hashlib.sha256)
    return mac.hexdigest()[:32]


def redact_argv(action: "ProdAction", key: bytes) -> Tuple[str, ...]:
    """Return argv with each secret VALUE half replaced by ``KEY=<sha:tag>``.

    Safe to store and to show. The tag is stable per (key, value) so replay of
    the identical secret is detectable, but the plaintext is unrecoverable.
    """
    redacted = list(action.argv)
    for pos in action.secret_positions:
        if 0 <= pos < len(redacted):
            tok = redacted[pos]
            name, sep, val = tok.partition("=")
            if sep:
                redacted[pos] = f"{name}=<redacted:{_tag(key, domain='secret', value=val)}>"
    return tuple(redacted)


def action_fingerprint(action: "ProdAction", key: bytes) -> str:
    """Fingerprint of the action alone (executable + redacted argv + cwd +
    immutable targets + secret tags). Context is bound separately by
    :func:`grant_fingerprint` so we can dedupe pending requests per action while
    still binding approval to the full context."""
    payload = {
        "class": action.action_class,
        "executable": action.executable,
        "argv": list(redact_argv(action, key)),
        "cwd": action.cwd,
        "targets": [list(t) for t in action.targets],
        "read_only": action.read_only,
    }
    canon = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hmac.new(key, canon.encode("utf-8"), hashlib.sha256).hexdigest()


def context_fingerprint(ctx: dict, key: bytes) -> str:
    """Fingerprint of the authoritative binding context."""
    # Only the authoritative, non-forgeable-vs-DB fields participate.
    fields = (
        "platform", "chat_id", "thread_id", "user_id",
        "profile", "session_id",
        "task_id", "run_id", "claim_lock",
    )
    payload = {k: str(ctx.get(k, "")) for k in fields}
    canon = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hmac.new(key, canon.encode("utf-8"), hashlib.sha256).hexdigest()


def grant_fingerprint(action: "ProdAction", ctx: dict, key: bytes) -> str:
    """The identity a request/approval is keyed on: action ⊗ context."""
    af = action_fingerprint(action, key)
    cf = context_fingerprint(ctx, key)
    return hmac.new(key, f"{af}\x00{cf}".encode("utf-8"), hashlib.sha256).hexdigest()


def load_or_create_key(base_dir: Path) -> bytes:
    """Load the 32-byte HMAC key, creating it 0600 on first use.

    The key file is separate from the SQLite store: a corrupt DB never leaks the
    key, and deleting the key deterministically invalidates all grants.
    """
    base_dir.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(base_dir, 0o700)
    except OSError:
        pass
    key_path = base_dir / "fingerprint.key"
    if key_path.exists():
        data = key_path.read_bytes()
        if len(data) >= 32:
            return data[:32]
        # Corrupt/short key → refuse rather than silently weaken.
        raise RuntimeError("prod-approvals fingerprint key is corrupt")
    key = os.urandom(32)
    # Write with tight perms atomically.
    fd = os.open(str(key_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, key)
    finally:
        os.close(fd)
    try:
        os.chmod(key_path, 0o600)
    except OSError:
        pass
    return key
