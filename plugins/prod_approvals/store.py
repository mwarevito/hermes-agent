"""Durable, fenced approval store (SQLite WAL, 0600, transactional CAS).

Design contract:

* One SQLite database under ``HERMES_HOME/prod_approvals/approvals.db``, WAL
  mode, file mode 0600. Survives gateway restarts; safe across multiple gateway
  processes (SQLite serialises writers; every state transition is a
  compare-and-swap on ``(nonce, state, fence)``).
* Schema is versioned. Opening a store whose version is *newer* than this code
  understands, or whose file is corrupt/unreadable, raises
  :class:`StoreUnavailable` — the gate treats that as *block* (fail closed). A
  store at an *older* version is migrated, and migration **invalidates every
  non-terminal (legacy pending) grant** so a stale ``requested``/``approved``
  row can never be silently honoured across a schema change.
* Lifecycle: ``requested → {approved|denied|revoked|expired}``;
  ``approved → claimed`` (single CAS consume = exactly-once);
  ``claimed → {succeeded|failed|indeterminate}``. ``indeterminate`` and every
  unknown external outcome are terminal and never auto-retried.

No secret values are ever stored: only redacted argv, immutable target ids, the
authoritative context, and keyed fingerprints.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

SCHEMA_VERSION = 1

# States
REQUESTED = "requested"
APPROVED = "approved"
DENIED = "denied"
REVOKED = "revoked"
EXPIRED = "expired"
CLAIMED = "claimed"
SUCCEEDED = "succeeded"
FAILED = "failed"
INDETERMINATE = "indeterminate"

_NON_TERMINAL = (REQUESTED, APPROVED)
_TERMINAL = (DENIED, REVOKED, EXPIRED, SUCCEEDED, FAILED, INDETERMINATE)

DEFAULT_TTL_SECONDS = 15 * 60


class StoreUnavailable(Exception):
    """The store is corrupt, unreachable, or at an unknown schema version.

    The gate MUST fail closed (block) when it sees this."""


@dataclass(frozen=True)
class Grant:
    nonce: str
    grant_fp: str
    action_fp: str
    action_class: str
    redacted_argv: Tuple[str, ...]
    targets: Tuple[Tuple[str, str], ...]
    ctx: Dict[str, str]
    state: str
    fence: int
    bundle_id: Optional[str]
    step_index: Optional[int]
    deps: Tuple[int, ...]
    created_at: float
    expires_at: float


def _now() -> float:
    return time.time()


class ApprovalStore:
    def __init__(self, base_dir: Path, *, now_fn=_now, ttl_seconds: int = DEFAULT_TTL_SECONDS):
        self._base = Path(base_dir)
        self._now = now_fn
        self._ttl = ttl_seconds
        self._db_path = self._base / "approvals.db"
        self._conn = self._open()

    # -- connection / schema -------------------------------------------------

    def _open(self) -> sqlite3.Connection:
        try:
            self._base.mkdir(parents=True, exist_ok=True)
            try:
                os.chmod(self._base, 0o700)
            except OSError:
                pass
            fresh = not self._db_path.exists()
            if fresh:
                # Create 0600 up front so the file is never briefly world-readable.
                fd = os.open(str(self._db_path), os.O_CREAT | os.O_WRONLY, 0o600)
                os.close(fd)
            conn = sqlite3.connect(
                str(self._db_path), timeout=10.0, isolation_level=None
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=10000")
            # Corruption check — fail closed if the file is damaged.
            row = conn.execute("PRAGMA quick_check").fetchone()
            if not row or row[0] != "ok":
                raise StoreUnavailable(f"integrity check failed: {row and row[0]}")
        except StoreUnavailable:
            raise
        except sqlite3.DatabaseError as exc:
            raise StoreUnavailable(f"cannot open store: {exc}") from exc
        try:
            self._ensure_schema(conn)
        except StoreUnavailable:
            raise
        except sqlite3.DatabaseError as exc:
            raise StoreUnavailable(f"schema error: {exc}") from exc
        try:
            os.chmod(self._db_path, 0o600)
        except OSError:
            pass
        return conn

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        has_meta = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_meta'"
        ).fetchone()
        if not has_meta:
            self._create_schema(conn)
            return
        row = conn.execute("SELECT version FROM schema_meta WHERE id=1").fetchone()
        version = int(row["version"]) if row else 0
        if version == SCHEMA_VERSION:
            return
        if version > SCHEMA_VERSION:
            # Newer than we understand — rollback safety: treat as unapproved,
            # do not touch it, do not honour any grant.
            raise StoreUnavailable(
                f"store schema v{version} is newer than supported v{SCHEMA_VERSION}"
            )
        # Older schema → migrate. Migration invalidates every legacy pending grant.
        self._migrate(conn, version)

    def _create_schema(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            BEGIN;
            CREATE TABLE schema_meta (
                id INTEGER PRIMARY KEY CHECK (id=1),
                version INTEGER NOT NULL,
                created_at REAL NOT NULL
            );
            CREATE TABLE grants (
                nonce TEXT PRIMARY KEY,
                grant_fp TEXT NOT NULL,
                action_fp TEXT NOT NULL,
                action_class TEXT NOT NULL,
                redacted_argv TEXT NOT NULL,
                targets TEXT NOT NULL,
                ctx_json TEXT NOT NULL,
                state TEXT NOT NULL,
                fence INTEGER NOT NULL DEFAULT 0,
                bundle_id TEXT,
                step_index INTEGER,
                deps TEXT NOT NULL DEFAULT '[]',
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                approved_at REAL,
                claimed_at REAL,
                resolved_at REAL,
                approver TEXT,
                reason TEXT
            );
            CREATE INDEX idx_grants_fp_state ON grants(grant_fp, state);
            CREATE INDEX idx_grants_bundle ON grants(bundle_id, step_index);
            CREATE TABLE bundles (
                bundle_id TEXT PRIMARY KEY,
                spec_json TEXT NOT NULL,
                state TEXT NOT NULL,
                kind TEXT NOT NULL DEFAULT 'forward',
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL
            );
            CREATE TABLE audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                nonce TEXT,
                event TEXT NOT NULL,
                detail TEXT
            );
            COMMIT;
            """
        )
        conn.execute(
            "INSERT INTO schema_meta(id, version, created_at) VALUES (1, ?, ?)",
            (SCHEMA_VERSION, self._now()),
        )

    def _migrate(self, conn: sqlite3.Connection, from_version: int) -> None:
        # v1 is the first real schema. Any store presenting an older version is
        # legacy: we bring the schema forward (idempotent create-if-missing) and
        # invalidate every non-terminal grant so nothing pending survives the
        # migration as still-approved.
        try:
            conn.execute("BEGIN IMMEDIATE")
            # Ensure tables exist (older stores may predate some of them).
            self._create_missing_tables(conn)
            conn.execute(
                "UPDATE grants SET state=?, resolved_at=?, reason=? "
                "WHERE state IN (?, ?)",
                (EXPIRED, self._now(), f"invalidated by migration v{from_version}->v{SCHEMA_VERSION}",
                 REQUESTED, APPROVED),
            )
            conn.execute("UPDATE bundles SET state='expired' WHERE state NOT IN ('succeeded','failed','expired','revoked')")
            conn.execute(
                "UPDATE schema_meta SET version=? WHERE id=1", (SCHEMA_VERSION,)
            )
            conn.execute("COMMIT")
        except sqlite3.DatabaseError as exc:
            conn.execute("ROLLBACK")
            raise StoreUnavailable(f"migration failed: {exc}") from exc
        self._audit(conn, None, "migration",
                    f"v{from_version}->v{SCHEMA_VERSION}, legacy pending invalidated")

    def _create_missing_tables(self, conn: sqlite3.Connection) -> None:
        existing = {
            r["name"]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "schema_meta" not in existing:
            conn.execute(
                "CREATE TABLE schema_meta (id INTEGER PRIMARY KEY CHECK (id=1), "
                "version INTEGER NOT NULL, created_at REAL NOT NULL)"
            )
            conn.execute(
                "INSERT OR IGNORE INTO schema_meta(id,version,created_at) VALUES (1,?,?)",
                (0, self._now()),
            )

    # -- audit ---------------------------------------------------------------

    def _audit(self, conn: sqlite3.Connection, nonce: Optional[str], event: str, detail: str) -> None:
        try:
            conn.execute(
                "INSERT INTO audit(ts, nonce, event, detail) VALUES (?,?,?,?)",
                (self._now(), nonce, event, detail),
            )
        except sqlite3.DatabaseError:
            pass

    # -- row -> Grant --------------------------------------------------------

    @staticmethod
    def _row_to_grant(row: sqlite3.Row) -> Grant:
        return Grant(
            nonce=row["nonce"],
            grant_fp=row["grant_fp"],
            action_fp=row["action_fp"],
            action_class=row["action_class"],
            redacted_argv=tuple(json.loads(row["redacted_argv"])),
            targets=tuple(tuple(t) for t in json.loads(row["targets"])),
            ctx=json.loads(row["ctx_json"]),
            state=row["state"],
            fence=int(row["fence"]),
            bundle_id=row["bundle_id"],
            step_index=row["step_index"],
            deps=tuple(json.loads(row["deps"] or "[]")),
            created_at=row["created_at"],
            expires_at=row["expires_at"],
        )

    # -- expiry sweep --------------------------------------------------------

    def _sweep_expired(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            "UPDATE grants SET state=?, resolved_at=?, reason=COALESCE(reason,'expired') "
            "WHERE state IN (?, ?) AND expires_at <= ?",
            (EXPIRED, self._now(), REQUESTED, APPROVED, self._now()),
        )

    # -- public API ----------------------------------------------------------

    def request(
        self,
        *,
        grant_fp: str,
        action_fp: str,
        action_class: str,
        redacted_argv: Tuple[str, ...],
        targets: Tuple[Tuple[str, str], ...],
        ctx: Dict[str, str],
        ttl_seconds: Optional[int] = None,
        bundle_id: Optional[str] = None,
        step_index: Optional[int] = None,
        deps: Optional[Tuple[int, ...]] = None,
    ) -> Grant:
        """Create (or return the existing pending) request for a grant fingerprint.

        Dedupe: if a non-expired ``requested``/``approved`` grant with the same
        ``grant_fp`` already exists, it is returned unchanged so a model
        re-issuing the identical tool call before approval doesn't spawn
        duplicate prompts."""
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            self._sweep_expired(conn)
            existing = conn.execute(
                "SELECT * FROM grants WHERE grant_fp=? AND state IN (?, ?) "
                "AND expires_at > ? ORDER BY created_at LIMIT 1",
                (grant_fp, REQUESTED, APPROVED, self._now()),
            ).fetchone()
            if existing is not None and bundle_id is None:
                conn.execute("COMMIT")
                return self._row_to_grant(existing)
            nonce = uuid.uuid4().hex
            now = self._now()
            ttl = ttl_seconds if ttl_seconds is not None else self._ttl
            expires_at = now + ttl
            conn.execute(
                "INSERT INTO grants(nonce, grant_fp, action_fp, action_class, "
                "redacted_argv, targets, ctx_json, state, fence, bundle_id, "
                "step_index, deps, created_at, expires_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    nonce, grant_fp, action_fp, action_class,
                    json.dumps(list(redacted_argv)),
                    json.dumps([list(t) for t in targets]),
                    json.dumps(ctx),
                    REQUESTED, 0, bundle_id, step_index,
                    json.dumps(list(deps or ())),
                    now, expires_at,
                ),
            )
            self._audit(conn, nonce, "requested", action_class)
            row = conn.execute("SELECT * FROM grants WHERE nonce=?", (nonce,)).fetchone()
            conn.execute("COMMIT")
            return self._row_to_grant(row)
        except sqlite3.DatabaseError as exc:
            conn.execute("ROLLBACK")
            raise StoreUnavailable(f"request failed: {exc}") from exc

    def get(self, nonce: str) -> Optional[Grant]:
        row = self._conn.execute("SELECT * FROM grants WHERE nonce=?", (nonce,)).fetchone()
        return self._row_to_grant(row) if row else None

    def list_pending(self, ctx: Optional[Dict[str, str]] = None) -> List[Grant]:
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            self._sweep_expired(conn)
            conn.execute("COMMIT")
        except sqlite3.DatabaseError:
            conn.execute("ROLLBACK")
        rows = conn.execute(
            "SELECT * FROM grants WHERE state IN (?, ?) ORDER BY created_at",
            (REQUESTED, APPROVED),
        ).fetchall()
        grants = [self._row_to_grant(r) for r in rows]
        if ctx is not None:
            grants = [g for g in grants if _ctx_matches(g.ctx, ctx)]
        return grants

    def _transition(
        self, nonce: str, from_state: str, to_state: str,
        *, expected_fence: Optional[int] = None, extra_sql: str = "", extra_params: tuple = (),
        require_unexpired: bool = False,
    ) -> bool:
        """Fenced CAS state transition. Returns True iff exactly one row moved."""
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            where = "nonce=? AND state=?"
            sql = f"UPDATE grants SET state=?, fence=fence+1{extra_sql} WHERE {where}"
            args = [to_state] + list(extra_params) + [nonce, from_state]
            if expected_fence is not None:
                sql += " AND fence=?"
                args.append(expected_fence)
            if require_unexpired:
                sql += " AND expires_at > ?"
                args.append(self._now())
            cur = conn.execute(sql, args)
            moved = cur.rowcount == 1
            if moved:
                self._audit(conn, nonce, to_state, from_state)
            conn.execute("COMMIT")
            return moved
        except sqlite3.DatabaseError as exc:
            conn.execute("ROLLBACK")
            raise StoreUnavailable(f"transition failed: {exc}") from exc

    def approve(self, nonce: str, approver: str = "") -> bool:
        return self._transition(
            nonce, REQUESTED, APPROVED,
            extra_sql=", approved_at=?, approver=?",
            extra_params=(self._now(), approver),
            require_unexpired=True,
        )

    def deny(self, nonce: str, reason: str = "") -> bool:
        return self._transition(
            nonce, REQUESTED, DENIED,
            extra_sql=", resolved_at=?, reason=?",
            extra_params=(self._now(), reason or "denied"),
        )

    def revoke(self, nonce: str, reason: str = "") -> bool:
        # A grant can be revoked while still requested OR approved (before claim).
        if self._transition(
            nonce, APPROVED, REVOKED,
            extra_sql=", resolved_at=?, reason=?",
            extra_params=(self._now(), reason or "revoked"),
        ):
            return True
        return self._transition(
            nonce, REQUESTED, REVOKED,
            extra_sql=", resolved_at=?, reason=?",
            extra_params=(self._now(), reason or "revoked"),
        )

    def find_consumable(self, grant_fp: str, ctx: Dict[str, str]) -> Optional[Grant]:
        """Find an approved, unexpired grant whose fingerprint AND bound context
        both match. Context is re-checked here as defence in depth even though it
        is baked into ``grant_fp``."""
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            self._sweep_expired(conn)
            conn.execute("COMMIT")
        except sqlite3.DatabaseError:
            conn.execute("ROLLBACK")
        rows = conn.execute(
            "SELECT * FROM grants WHERE grant_fp=? AND state=? AND expires_at > ? "
            "ORDER BY approved_at LIMIT 1",
            (grant_fp, APPROVED, self._now()),
        ).fetchall()
        for r in rows:
            g = self._row_to_grant(r)
            if _ctx_matches(g.ctx, ctx):
                return g
        return None

    def claim(self, nonce: str, expected_fence: int) -> bool:
        """Exactly-once consume: CAS approved→claimed. For a bundle step, also
        enforce that all dependency steps have succeeded and it's not expired."""
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute("SELECT * FROM grants WHERE nonce=?", (nonce,)).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return False
            g = self._row_to_grant(row)
            if g.bundle_id is not None:
                if not self._deps_satisfied(conn, g):
                    conn.execute("COMMIT")
                    return False
            cur = conn.execute(
                "UPDATE grants SET state=?, fence=fence+1, claimed_at=? "
                "WHERE nonce=? AND state=? AND fence=? AND expires_at > ?",
                (CLAIMED, self._now(), nonce, APPROVED, expected_fence, self._now()),
            )
            moved = cur.rowcount == 1
            if moved:
                self._audit(conn, nonce, CLAIMED, APPROVED)
            conn.execute("COMMIT")
            return moved
        except sqlite3.DatabaseError as exc:
            conn.execute("ROLLBACK")
            raise StoreUnavailable(f"claim failed: {exc}") from exc

    def _deps_satisfied(self, conn: sqlite3.Connection, g: Grant) -> bool:
        if not g.deps:
            return True
        rows = conn.execute(
            "SELECT step_index, state FROM grants WHERE bundle_id=?",
            (g.bundle_id,),
        ).fetchall()
        state_by_step = {r["step_index"]: r["state"] for r in rows}
        return all(state_by_step.get(dep) == SUCCEEDED for dep in g.deps)

    def finish(self, nonce: str, outcome: str) -> bool:
        assert outcome in (SUCCEEDED, FAILED, INDETERMINATE)
        return self._transition(
            nonce, CLAIMED, outcome,
            extra_sql=", resolved_at=?",
            extra_params=(self._now(),),
        )

    # -- bundles -------------------------------------------------------------

    def create_bundle(
        self,
        *,
        steps: List[Dict[str, Any]],
        ctx: Dict[str, str],
        ttl_seconds: Optional[int] = None,
        kind: str = "forward",
        max_steps: int = 8,
    ) -> Tuple[str, List[Grant]]:
        """Create an ordered bounded bundle. Each ``step`` dict carries
        ``grant_fp``, ``action_fp``, ``action_class``, ``redacted_argv``,
        ``targets`` and optional ``deps`` (defaults to linear: depends on the
        previous step). Returns ``(bundle_id, [step grants])``."""
        if not steps:
            raise ValueError("bundle needs at least one step")
        if len(steps) > max_steps:
            raise ValueError(f"bundle exceeds max cardinality {max_steps}")
        conn = self._conn
        bundle_id = uuid.uuid4().hex
        now = self._now()
        ttl = ttl_seconds if ttl_seconds is not None else self._ttl
        expires_at = now + ttl
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "INSERT INTO bundles(bundle_id, spec_json, state, kind, created_at, expires_at) "
                "VALUES (?,?,?,?,?,?)",
                (bundle_id, json.dumps([_step_spec(s) for s in steps]),
                 REQUESTED, kind, now, expires_at),
            )
            grants: List[Grant] = []
            for idx, step in enumerate(steps):
                deps = tuple(step.get("deps", (idx - 1,) if idx > 0 else ()))
                nonce = uuid.uuid4().hex
                conn.execute(
                    "INSERT INTO grants(nonce, grant_fp, action_fp, action_class, "
                    "redacted_argv, targets, ctx_json, state, fence, bundle_id, "
                    "step_index, deps, created_at, expires_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        nonce, step["grant_fp"], step["action_fp"], step["action_class"],
                        json.dumps(list(step["redacted_argv"])),
                        json.dumps([list(t) for t in step.get("targets", ())]),
                        json.dumps(ctx),
                        REQUESTED, 0, bundle_id, idx,
                        json.dumps([d for d in deps if d >= 0]),
                        now, expires_at,
                    ),
                )
                grants.append(self._row_to_grant(
                    conn.execute("SELECT * FROM grants WHERE nonce=?", (nonce,)).fetchone()
                ))
            self._audit(conn, None, "bundle_requested", f"{bundle_id} steps={len(steps)} kind={kind}")
            conn.execute("COMMIT")
            return bundle_id, grants
        except sqlite3.DatabaseError as exc:
            conn.execute("ROLLBACK")
            raise StoreUnavailable(f"create_bundle failed: {exc}") from exc

    def approve_bundle(self, bundle_id: str, approver: str = "") -> bool:
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            self._sweep_expired(conn)
            b = conn.execute("SELECT * FROM bundles WHERE bundle_id=?", (bundle_id,)).fetchone()
            if b is None or b["state"] != REQUESTED or b["expires_at"] <= self._now():
                conn.execute("COMMIT")
                return False
            conn.execute(
                "UPDATE grants SET state=?, fence=fence+1, approved_at=?, approver=? "
                "WHERE bundle_id=? AND state=? AND expires_at > ?",
                (APPROVED, self._now(), approver, bundle_id, REQUESTED, self._now()),
            )
            conn.execute("UPDATE bundles SET state=? WHERE bundle_id=?", (APPROVED, bundle_id))
            self._audit(conn, None, "bundle_approved", bundle_id)
            conn.execute("COMMIT")
            return True
        except sqlite3.DatabaseError as exc:
            conn.execute("ROLLBACK")
            raise StoreUnavailable(f"approve_bundle failed: {exc}") from exc

    def revoke_bundle(self, bundle_id: str, reason: str = "revoked") -> int:
        """Revoke a whole bundle before it is consumed. Returns #steps revoked.

        Every non-terminal step (requested/approved) moves to ``revoked`` and
        the bundle itself to ``revoked``. A step already ``claimed`` or terminal
        is left alone (it is past the point of no return / already resolved)."""
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                "UPDATE grants SET state=?, fence=fence+1, resolved_at=?, reason=? "
                "WHERE bundle_id=? AND state IN (?, ?)",
                (REVOKED, self._now(), reason, bundle_id, REQUESTED, APPROVED),
            )
            n = cur.rowcount
            conn.execute(
                "UPDATE bundles SET state=? WHERE bundle_id=? AND state NOT IN "
                "('succeeded','failed')",
                (REVOKED, bundle_id),
            )
            self._audit(conn, None, "bundle_revoked", f"{bundle_id} steps={n}")
            conn.execute("COMMIT")
            return int(n)
        except sqlite3.DatabaseError as exc:
            conn.execute("ROLLBACK")
            raise StoreUnavailable(f"revoke_bundle failed: {exc}") from exc

    def find_bundle_step(self, grant_fp: str, ctx: Dict[str, str]) -> Optional[Grant]:
        """Find an approved bundle-step grant matching fingerprint+context whose
        dependencies are all satisfied (so it is the next legal step)."""
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            self._sweep_expired(conn)
            conn.execute("COMMIT")
        except sqlite3.DatabaseError:
            conn.execute("ROLLBACK")
        rows = conn.execute(
            "SELECT * FROM grants WHERE grant_fp=? AND state=? AND bundle_id IS NOT NULL "
            "AND expires_at > ? ORDER BY step_index",
            (grant_fp, APPROVED, self._now()),
        ).fetchall()
        for r in rows:
            g = self._row_to_grant(r)
            if _ctx_matches(g.ctx, ctx) and self._deps_satisfied(self._conn, g):
                return g
        return None

    # -- lifecycle -----------------------------------------------------------

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass


def _step_spec(step: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "action_class": step["action_class"],
        "action_fp": step["action_fp"],
        "deps": list(step.get("deps", [])),
    }


def _ctx_matches(bound: Dict[str, str], current: Dict[str, str]) -> bool:
    """Every authoritative field bound at request time must equal the current
    context. Missing-vs-present is a mismatch (no ambient inheritance)."""
    fields = (
        "platform", "chat_id", "thread_id", "user_id",
        "profile", "session_id",
        "task_id", "run_id", "claim_lock",
    )
    for f in fields:
        if str(bound.get(f, "")) != str(current.get(f, "")):
            return False
    return True
