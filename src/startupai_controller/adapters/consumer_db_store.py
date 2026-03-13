"""SQLite access layer for the board consumer daemon.

Manages lease and session state for codex execution cycles. Separated from
board_consumer.py for independent testability.

DB path: ~/.local/share/startupai/consumer.db (WAL mode, mkdir -p on first use).
DI: connection_factory callable for test isolation.
"""

from __future__ import annotations

import sqlite3
import uuid
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from startupai_controller.domain.models import ReviewQueueEntry, SessionInfo

DEFAULT_DB_PATH = Path.home() / ".local" / "share" / "startupai" / "consumer.db"

# Valid session statuses (state machine: pending -> running -> success|failed|timeout|aborted)
# "aborted" = lease conflict or pre-execution cancellation; never counts as a retry.
VALID_SESSION_STATUSES = {
    "pending",
    "running",
    "success",
    "failed",
    "timeout",
    "aborted",
}
VALID_SESSION_KINDS = {"new_work", "repair"}


@dataclass(frozen=True)
class RecoveredLease:

    issue_ref: str
    session_id: str
    session_status: str | None
    pr_url: str | None
    branch_name: str | None
    provenance_id: str | None
    session_kind: str
    repair_pr_url: str | None


@dataclass(frozen=True)
class DeferredAction:

    id: int
    scope_key: str
    action_type: str
    payload_json: str
    created_at: str

    @property
    def payload(self) -> dict[str, Any]:
        return json.loads(self.payload_json)


@dataclass(frozen=True)
class IssueContextCacheEntry:

    issue_ref: str
    owner: str
    repo: str
    number: int
    title: str
    body: str
    labels_json: str
    issue_updated_at: str
    fetched_at: str
    expires_at: str

    @property
    def labels(self) -> list[str]:
        try:
            payload = json.loads(self.labels_json)
        except json.JSONDecodeError:
            return []
        if not isinstance(payload, list):
            return []
        return [str(item) for item in payload if str(item)]


@dataclass(frozen=True)
class MetricEvent:

    id: int
    event_type: str
    issue_ref: str | None
    payload_json: str | None
    created_at: str

    @property
    def payload(self) -> dict[str, Any]:
        if not self.payload_json:
            return {}
        try:
            payload = json.loads(self.payload_json)
        except json.JSONDecodeError:
            return {}
        if not isinstance(payload, dict):
            return {}
        return payload


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS leases (
    issue_ref TEXT PRIMARY KEY,
    session_id TEXT NOT NULL UNIQUE,
    slot_id INTEGER,
    acquired_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    issue_ref TEXT NOT NULL,
    repo_prefix TEXT,
    worktree_path TEXT,
    branch_name TEXT,
    executor TEXT NOT NULL DEFAULT 'codex',
    slot_id INTEGER,
    status TEXT NOT NULL DEFAULT 'pending',
    phase TEXT,
    started_at TEXT,
    completed_at TEXT,
    outcome_json TEXT,
    failure_reason TEXT,
    retry_count INTEGER DEFAULT 0,
    pr_url TEXT,
    provenance_id TEXT,
    session_kind TEXT NOT NULL DEFAULT 'new_work',
    repair_pr_url TEXT,
    branch_reconcile_state TEXT,
    branch_reconcile_error TEXT,
    resolution_kind TEXT,
    verification_class TEXT,
    resolution_evidence_json TEXT,
    resolution_action TEXT,
    done_reason TEXT
);

CREATE TABLE IF NOT EXISTS deferred_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scope_key TEXT NOT NULL,
    action_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS control_state (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS issue_context_cache (
    issue_ref TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    repo TEXT NOT NULL,
    number INTEGER NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    labels_json TEXT NOT NULL,
    issue_updated_at TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS consumer_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    issue_ref TEXT,
    payload_json TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS review_queue (
    issue_ref TEXT PRIMARY KEY,
    pr_url TEXT NOT NULL,
    pr_repo TEXT NOT NULL,
    pr_number INTEGER NOT NULL,
    source_session_id TEXT,
    enqueued_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    next_attempt_at TEXT NOT NULL,
    last_attempt_at TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_result TEXT,
    last_reason TEXT,
    last_state_digest TEXT
);

CREATE INDEX IF NOT EXISTS idx_consumer_metrics_type_created
ON consumer_metrics(event_type, created_at);

CREATE INDEX IF NOT EXISTS idx_consumer_metrics_created
ON consumer_metrics(created_at);

CREATE INDEX IF NOT EXISTS idx_review_queue_next_attempt
ON review_queue(next_attempt_at, enqueued_at);
"""


# ---------------------------------------------------------------------------
# ConsumerDB
# ---------------------------------------------------------------------------


class ConsumerDB:
    """SQLite access layer for consumer lease and session state.

    Args:
        db_path: Path to SQLite database file. Parent dirs created on init.
        connection_factory: Optional callable returning a sqlite3.Connection.
            Used for test isolation (e.g. in-memory DB via tmp_path).
    """

    def __init__(
        self,
        db_path: Path = DEFAULT_DB_PATH,
        *,
        connection_factory: Callable[[], sqlite3.Connection] | None = None,
    ) -> None:
        self._db_path = db_path
        self._connection_factory = connection_factory
        self._conn: sqlite3.Connection | None = None

    def _get_connection(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn

        if self._connection_factory is not None:
            self._conn = self._connection_factory()
        else:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(str(self._db_path))
            self._conn.execute("PRAGMA journal_mode=WAL")

        self._conn.row_factory = sqlite3.Row
        self._init_schema(self._conn)
        return self._conn

    @staticmethod
    def _init_schema(conn: sqlite3.Connection) -> None:
        conn.executescript(_SCHEMA_SQL)
        ConsumerDB._ensure_column(conn, "leases", "slot_id", "INTEGER")
        ConsumerDB._ensure_column(conn, "sessions", "repo_prefix", "TEXT")
        ConsumerDB._ensure_column(conn, "sessions", "slot_id", "INTEGER")
        ConsumerDB._ensure_column(conn, "sessions", "phase", "TEXT")
        ConsumerDB._ensure_column(conn, "sessions", "provenance_id", "TEXT")
        ConsumerDB._ensure_column(conn, "sessions", "failure_reason", "TEXT")
        ConsumerDB._ensure_column(
            conn, "sessions", "session_kind", "TEXT NOT NULL DEFAULT 'new_work'"
        )
        ConsumerDB._ensure_column(conn, "sessions", "repair_pr_url", "TEXT")
        ConsumerDB._ensure_column(conn, "sessions", "branch_reconcile_state", "TEXT")
        ConsumerDB._ensure_column(conn, "sessions", "branch_reconcile_error", "TEXT")
        ConsumerDB._ensure_column(conn, "sessions", "resolution_kind", "TEXT")
        ConsumerDB._ensure_column(conn, "sessions", "verification_class", "TEXT")
        ConsumerDB._ensure_column(conn, "sessions", "resolution_evidence_json", "TEXT")
        ConsumerDB._ensure_column(conn, "sessions", "resolution_action", "TEXT")
        ConsumerDB._ensure_column(conn, "sessions", "done_reason", "TEXT")
        ConsumerDB._ensure_column(conn, "review_queue", "last_state_digest", "TEXT")
        ConsumerDB._ensure_column(
            conn, "review_queue", "blocked_streak", "INTEGER NOT NULL DEFAULT 0"
        )
        ConsumerDB._ensure_column(conn, "review_queue", "blocked_class", "TEXT")

    @staticmethod
    def _ensure_column(
        conn: sqlite3.Connection,
        table: str,
        column: str,
        column_sql: str,
    ) -> None:
        columns = {
            row["name"]
            for row in conn.execute(
                f"PRAGMA table_info({table})"
            ).fetchall()  # noqa: S608
        }
        if column in columns:
            return
        try:
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN {column} {column_sql}"  # noqa: S608
            )
        except sqlite3.OperationalError as error:
            if "duplicate column name" not in str(error).lower():
                raise

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # -- Lease operations ---------------------------------------------------

    def acquire_lease(
        self,
        issue_ref: str,
        session_id: str,
        slot_id: int | None = None,
        now: datetime | None = None,
    ) -> bool:
        """Attempt to acquire a lease for issue_ref.

        Returns True on success, False if a lease already exists (PK conflict).
        """
        ts = (now or datetime.now(timezone.utc)).isoformat()
        conn = self._get_connection()
        try:
            conn.execute(
                "INSERT INTO leases "
                "(issue_ref, session_id, slot_id, acquired_at, heartbeat_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (issue_ref, session_id, slot_id, ts, ts),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def release_lease(self, issue_ref: str) -> None:
        conn = self._get_connection()
        conn.execute("DELETE FROM leases WHERE issue_ref = ?", (issue_ref,))
        conn.commit()

    def active_lease_count(self) -> int:
        conn = self._get_connection()
        row = conn.execute("SELECT COUNT(*) FROM leases").fetchone()
        return row[0]

    def active_lease_issue_refs(self) -> list[str]:
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT issue_ref FROM leases ORDER BY acquired_at ASC"
        ).fetchall()
        return [str(row["issue_ref"]) for row in rows]

    def active_slot_ids(self) -> set[int]:
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT slot_id FROM leases WHERE slot_id IS NOT NULL"
        ).fetchall()
        return {int(row["slot_id"]) for row in rows}

    def active_workers(self) -> list[SessionInfo]:
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT s.* FROM leases l "
            "JOIN sessions s ON s.id = l.session_id "
            "ORDER BY COALESCE(l.slot_id, 0) ASC, l.acquired_at ASC"
        ).fetchall()
        return [self._row_to_session(row) for row in rows]

    def update_heartbeat(self, issue_ref: str, now: datetime | None = None) -> None:
        ts = (now or datetime.now(timezone.utc)).isoformat()
        conn = self._get_connection()
        conn.execute(
            "UPDATE leases SET heartbeat_at = ? WHERE issue_ref = ?",
            (ts, issue_ref),
        )
        conn.commit()

    def expire_stale_leases(
        self,
        max_age_seconds: int,
        now: datetime | None = None,
    ) -> list[str]:
        """Delete leases whose heartbeat is older than max_age_seconds.

        Also marks corresponding sessions as 'timeout'.
        Returns list of expired issue_refs.
        """
        current = now or datetime.now(timezone.utc)
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT issue_ref, session_id, heartbeat_at FROM leases"
        ).fetchall()

        expired: list[str] = []
        for row in rows:
            heartbeat = datetime.fromisoformat(row["heartbeat_at"])
            if heartbeat.tzinfo is None:
                heartbeat = heartbeat.replace(tzinfo=timezone.utc)
            age = (current - heartbeat).total_seconds()
            if age > max_age_seconds:
                expired.append(row["issue_ref"])
                # Mark session as timeout
                conn.execute(
                    "UPDATE sessions SET status = 'timeout', completed_at = ? "
                    "WHERE id = ? AND status IN ('pending', 'running')",
                    (current.isoformat(), row["session_id"]),
                )
                conn.execute(
                    "DELETE FROM leases WHERE issue_ref = ?",
                    (row["issue_ref"],),
                )

        if expired:
            conn.commit()
        return expired

    def recover_interrupted_leases(
        self,
        now: datetime | None = None,
    ) -> list[RecoveredLease]:
        """Clear leftover leases from a previously interrupted daemon process.

        Any `pending` or `running` session tied to an active lease is marked
        `aborted`, then the lease is removed so the next daemon can resume work.
        Completed sessions keep their existing terminal status.
        """
        current = (now or datetime.now(timezone.utc)).isoformat()
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT l.issue_ref, l.session_id, s.status, s.pr_url, s.branch_name, "
            "s.provenance_id, s.session_kind, s.repair_pr_url "
            "FROM leases l "
            "LEFT JOIN sessions s ON s.id = l.session_id"
        ).fetchall()

        recovered: list[RecoveredLease] = []
        for row in rows:
            recovered.append(
                RecoveredLease(
                    issue_ref=row["issue_ref"],
                    session_id=row["session_id"],
                    session_status=row["status"],
                    pr_url=row["pr_url"],
                    branch_name=row["branch_name"],
                    provenance_id=row["provenance_id"],
                    session_kind=row["session_kind"] or "new_work",
                    repair_pr_url=row["repair_pr_url"],
                )
            )
            conn.execute(
                "UPDATE sessions "
                "SET status = 'aborted', "
                "failure_reason = COALESCE(failure_reason, 'interrupted_restart'), "
                "completed_at = COALESCE(completed_at, ?) "
                "WHERE id = ? AND status IN ('pending', 'running')",
                (current, row["session_id"]),
            )
            conn.execute(
                "DELETE FROM leases WHERE issue_ref = ?",
                (row["issue_ref"],),
            )

        if recovered:
            conn.commit()
        return recovered

    # -- Session operations -------------------------------------------------

    def create_session(
        self,
        issue_ref: str,
        repo_prefix: str | None = None,
        executor: str = "codex",
        slot_id: int | None = None,
        phase: str = "claimed",
        provenance_id: str | None = None,
        session_kind: str = "new_work",
        repair_pr_url: str | None = None,
    ) -> str:
        if session_kind not in VALID_SESSION_KINDS:
            raise ValueError(
                f"Invalid session kind: {session_kind}. Must be one of {VALID_SESSION_KINDS}"
            )
        session_id = uuid.uuid4().hex[:12]
        conn = self._get_connection()
        conn.execute(
            "INSERT INTO sessions "
            "("
            "id, issue_ref, repo_prefix, executor, slot_id, status, phase, "
            "provenance_id, session_kind, repair_pr_url"
            ") "
            "VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)",
            (
                session_id,
                issue_ref,
                repo_prefix,
                executor,
                slot_id,
                phase,
                provenance_id,
                session_kind,
                repair_pr_url,
            ),
        )
        conn.commit()
        return session_id

    def update_session(self, session_id: str, **fields: Any) -> None:
        """Update session fields by session_id.

        Accepted fields: worktree_path, branch_name, status, started_at,
        completed_at, outcome_json, failure_reason, retry_count, pr_url.
        """
        allowed = {
            "repo_prefix",
            "worktree_path",
            "branch_name",
            "status",
            "slot_id",
            "phase",
            "started_at",
            "completed_at",
            "outcome_json",
            "failure_reason",
            "retry_count",
            "pr_url",
            "provenance_id",
            "session_kind",
            "repair_pr_url",
            "branch_reconcile_state",
            "branch_reconcile_error",
            "resolution_kind",
            "verification_class",
            "resolution_evidence_json",
            "resolution_action",
            "done_reason",
        }
        invalid = set(fields) - allowed
        if invalid:
            raise ValueError(f"Invalid session fields: {invalid}")
        if "status" in fields and fields["status"] not in VALID_SESSION_STATUSES:
            raise ValueError(
                f"Invalid session status: {fields['status']}. "
                f"Must be one of {VALID_SESSION_STATUSES}"
            )
        if (
            "session_kind" in fields
            and fields["session_kind"] not in VALID_SESSION_KINDS
        ):
            raise ValueError(
                f"Invalid session kind: {fields['session_kind']}. "
                f"Must be one of {VALID_SESSION_KINDS}"
            )
        if not fields:
            return

        set_clause = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values()) + [session_id]
        conn = self._get_connection()
        conn.execute(
            f"UPDATE sessions SET {set_clause} WHERE id = ?",  # noqa: S608
            values,
        )
        conn.commit()

    def get_session(self, session_id: str) -> SessionInfo | None:
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()
        if row is None:
            return None
        return self._row_to_session(row)

    def count_retries(self, issue_ref: str) -> int:
        """Count execution retries for issue_ref.

        Pre-execution failures do not burn retry budget. A failed session only
        counts once actual execution started (`started_at` recorded). Timeouts
        always count because they imply an in-flight session expired.
        """
        conn = self._get_connection()
        row = conn.execute(
            "SELECT COUNT(*) FROM sessions "
            "WHERE issue_ref = ? "
            "AND (status = 'timeout' OR (status = 'failed' AND started_at IS NOT NULL))",
            (issue_ref,),
        ).fetchone()
        return row[0]

    def latest_session_for_issue(
        self,
        issue_ref: str,
        *,
        exclude_session_id: str | None = None,
    ) -> SessionInfo | None:
        return self._latest_session_for_issue(
            issue_ref,
            exclude_session_id=exclude_session_id,
        )

    def _latest_session_for_issue(
        self,
        issue_ref: str,
        *,
        exclude_session_id: str | None = None,
    ) -> SessionInfo | None:
        conn = self._get_connection()
        if exclude_session_id is None:
            row = conn.execute(
                "SELECT * FROM sessions WHERE issue_ref = ? ORDER BY rowid DESC LIMIT 1",
                (issue_ref,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM sessions WHERE issue_ref = ? AND id != ? "
                "ORDER BY rowid DESC LIMIT 1",
                (issue_ref, exclude_session_id),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_session(row)

    def latest_session_for_worktree(self, worktree_path: str) -> SessionInfo | None:
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM sessions WHERE worktree_path = ? "
            "ORDER BY rowid DESC LIMIT 1",
            (worktree_path,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_session(row)

    def recent_sessions(self, limit: int = 20) -> list[SessionInfo]:
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT * FROM sessions ORDER BY rowid DESC LIMIT ?", (limit,)
        ).fetchall()
        return [self._row_to_session(r) for r in rows]

    def latest_review_issue_refs(self) -> list[str]:
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT s.issue_ref "
            "FROM sessions s "
            "JOIN ("
            "  SELECT issue_ref, MAX(rowid) AS latest_rowid "
            "  FROM sessions "
            "  GROUP BY issue_ref"
            ") latest "
            "ON latest.issue_ref = s.issue_ref AND latest.latest_rowid = s.rowid "
            "WHERE s.phase = 'review' "
            "ORDER BY s.issue_ref ASC"
        ).fetchall()
        return [str(row["issue_ref"]) for row in rows]

    # -- Deferred action operations -----------------------------------------

    def queue_deferred_action(
        self,
        scope_key: str,
        action_type: str,
        payload: dict[str, Any],
        *,
        now: datetime | None = None,
    ) -> int:
        """Queue a deferred action for later replay.

        Actions replay FIFO within the same scope. Newer `set_status` actions
        supersede older queued `set_status` actions for that same scope.
        Exact duplicates for non-status actions are ignored.
        """
        ts = (now or datetime.now(timezone.utc)).isoformat()
        payload_json = json.dumps(payload, sort_keys=True)
        conn = self._get_connection()

        existing_rows = conn.execute(
            "SELECT id, action_type, payload_json FROM deferred_actions "
            "WHERE scope_key = ? ORDER BY id ASC",
            (scope_key,),
        ).fetchall()
        for row in existing_rows:
            existing_type = str(row["action_type"])
            existing_payload = str(row["payload_json"])
            if action_type == "set_status" and existing_type == "set_status":
                conn.execute(
                    "DELETE FROM deferred_actions WHERE id = ?",
                    (row["id"],),
                )
                continue
            if existing_type == action_type and existing_payload == payload_json:
                conn.commit()
                return int(row["id"])

        cursor = conn.execute(
            "INSERT INTO deferred_actions (scope_key, action_type, payload_json, created_at) "
            "VALUES (?, ?, ?, ?)",
            (scope_key, action_type, payload_json, ts),
        )
        conn.commit()
        row_id = cursor.lastrowid
        if row_id is None:
            raise RuntimeError("deferred action insert did not return a row id")
        return row_id

    def list_deferred_actions(self) -> list[DeferredAction]:
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT id, scope_key, action_type, payload_json, created_at "
            "FROM deferred_actions ORDER BY created_at ASC, id ASC"
        ).fetchall()
        return [
            DeferredAction(
                id=int(row["id"]),
                scope_key=str(row["scope_key"]),
                action_type=str(row["action_type"]),
                payload_json=str(row["payload_json"]),
                created_at=str(row["created_at"]),
            )
            for row in rows
        ]

    def delete_deferred_action(self, action_id: int) -> None:
        conn = self._get_connection()
        conn.execute("DELETE FROM deferred_actions WHERE id = ?", (action_id,))
        conn.commit()

    def deferred_action_count(self) -> int:
        conn = self._get_connection()
        row = conn.execute("SELECT COUNT(*) FROM deferred_actions").fetchone()
        return int(row[0])

    def oldest_deferred_action_age_seconds(
        self,
        *,
        now: datetime | None = None,
    ) -> float | None:
        conn = self._get_connection()
        row = conn.execute(
            "SELECT created_at FROM deferred_actions ORDER BY created_at ASC, id ASC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        created_at = datetime.fromisoformat(str(row["created_at"]))
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        current = now or datetime.now(timezone.utc)
        return max(0.0, (current - created_at).total_seconds())

    # -- Control state operations -------------------------------------------

    def set_control_value(self, key: str, value: str | None) -> None:
        conn = self._get_connection()
        if value is None:
            conn.execute("DELETE FROM control_state WHERE key = ?", (key,))
        else:
            conn.execute(
                "INSERT INTO control_state (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
        conn.commit()

    def get_control_value(self, key: str) -> str | None:
        conn = self._get_connection()
        row = conn.execute(
            "SELECT value FROM control_state WHERE key = ?",
            (key,),
        ).fetchone()
        if row is None:
            return None
        return str(row["value"])

    def control_state_snapshot(self) -> dict[str, str]:
        conn = self._get_connection()
        rows = conn.execute("SELECT key, value FROM control_state").fetchall()
        return {str(row["key"]): str(row["value"]) for row in rows}

    # -- Requeue cycle tracking --------------------------------------------

    def get_requeue_state(self, issue_ref: str) -> tuple[int, str | None]:
        raw = self.get_control_value(f"requeue:{issue_ref}")
        if raw is None:
            return 0, None
        try:
            data = json.loads(raw)
            return int(data.get("count", 0)), data.get("pr_url")
        except (json.JSONDecodeError, TypeError, ValueError):
            return 0, None

    def increment_requeue_count(self, issue_ref: str, pr_url: str) -> int:
        count, _ = self.get_requeue_state(issue_ref)
        new_count = count + 1
        self.set_control_value(
            f"requeue:{issue_ref}",
            json.dumps({"count": new_count, "pr_url": pr_url}),
        )
        return new_count

    def reset_requeue_count(self, issue_ref: str) -> None:
        self.set_control_value(
            f"requeue:{issue_ref}",
            json.dumps({"count": 0, "pr_url": None}),
        )

    # -- Issue context cache ------------------------------------------------

    def get_issue_context(self, issue_ref: str) -> IssueContextCacheEntry | None:
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM issue_context_cache WHERE issue_ref = ?",
            (issue_ref,),
        ).fetchone()
        if row is None:
            return None
        return IssueContextCacheEntry(
            issue_ref=str(row["issue_ref"]),
            owner=str(row["owner"]),
            repo=str(row["repo"]),
            number=int(row["number"]),
            title=str(row["title"]),
            body=str(row["body"]),
            labels_json=str(row["labels_json"]),
            issue_updated_at=str(row["issue_updated_at"]),
            fetched_at=str(row["fetched_at"]),
            expires_at=str(row["expires_at"]),
        )

    def set_issue_context(
        self,
        issue_ref: str,
        *,
        owner: str,
        repo: str,
        number: int,
        title: str,
        body: str,
        labels: list[str],
        issue_updated_at: str,
        fetched_at: str,
        expires_at: str,
    ) -> None:
        conn = self._get_connection()
        conn.execute(
            "INSERT INTO issue_context_cache ("
            "issue_ref, owner, repo, number, title, body, labels_json, "
            "issue_updated_at, fetched_at, expires_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(issue_ref) DO UPDATE SET "
            "owner = excluded.owner, "
            "repo = excluded.repo, "
            "number = excluded.number, "
            "title = excluded.title, "
            "body = excluded.body, "
            "labels_json = excluded.labels_json, "
            "issue_updated_at = excluded.issue_updated_at, "
            "fetched_at = excluded.fetched_at, "
            "expires_at = excluded.expires_at",
            (
                issue_ref,
                owner,
                repo,
                number,
                title,
                body,
                json.dumps(labels, sort_keys=True),
                issue_updated_at,
                fetched_at,
                expires_at,
            ),
        )
        conn.commit()

    def delete_issue_context(self, issue_ref: str) -> None:
        conn = self._get_connection()
        conn.execute(
            "DELETE FROM issue_context_cache WHERE issue_ref = ?",
            (issue_ref,),
        )
        conn.commit()

    # -- Review queue -------------------------------------------------------

    def enqueue_review_item(
        self,
        issue_ref: str,
        *,
        pr_url: str,
        pr_repo: str,
        pr_number: int,
        source_session_id: str | None = None,
        next_attempt_at: str | None = None,
        now: datetime | None = None,
    ) -> None:
        current = now or datetime.now(timezone.utc)
        ts = current.isoformat()
        next_attempt = next_attempt_at or ts
        conn = self._get_connection()
        existing = conn.execute(
            "SELECT pr_url, source_session_id FROM review_queue WHERE issue_ref = ?",
            (issue_ref,),
        ).fetchone()
        reset_attempts = (
            existing is None
            or str(existing["pr_url"]) != pr_url
            or str(existing["source_session_id"] or "") != str(source_session_id or "")
        )
        if existing is None or reset_attempts:
            conn.execute(
                "INSERT INTO review_queue ("
                "issue_ref, pr_url, pr_repo, pr_number, source_session_id, "
                "enqueued_at, updated_at, next_attempt_at, last_attempt_at, "
                "attempt_count, last_result, last_reason, last_state_digest, "
                "blocked_streak, blocked_class"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, NULL, NULL, NULL, 0, NULL) "
                "ON CONFLICT(issue_ref) DO UPDATE SET "
                "pr_url = excluded.pr_url, "
                "pr_repo = excluded.pr_repo, "
                "pr_number = excluded.pr_number, "
                "source_session_id = excluded.source_session_id, "
                "enqueued_at = excluded.enqueued_at, "
                "updated_at = excluded.updated_at, "
                "next_attempt_at = excluded.next_attempt_at, "
                "last_attempt_at = NULL, "
                "attempt_count = 0, "
                "last_result = NULL, "
                "last_reason = NULL, "
                "last_state_digest = NULL, "
                "blocked_streak = 0, "
                "blocked_class = NULL",
                (
                    issue_ref,
                    pr_url,
                    pr_repo,
                    pr_number,
                    source_session_id,
                    ts,
                    ts,
                    next_attempt,
                ),
            )
        else:
            conn.execute(
                "UPDATE review_queue SET "
                "pr_url = ?, pr_repo = ?, pr_number = ?, source_session_id = ?, "
                "updated_at = ?, next_attempt_at = ? "
                "WHERE issue_ref = ?",
                (
                    pr_url,
                    pr_repo,
                    pr_number,
                    source_session_id,
                    ts,
                    next_attempt,
                    issue_ref,
                ),
            )
        conn.commit()

    def get_review_queue_item(self, issue_ref: str) -> ReviewQueueEntry | None:
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM review_queue WHERE issue_ref = ?",
            (issue_ref,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_review_queue_entry(row)

    def list_review_queue_items(self) -> list[ReviewQueueEntry]:
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT * FROM review_queue "
            "ORDER BY next_attempt_at ASC, enqueued_at ASC, issue_ref ASC"
        ).fetchall()
        return [self._row_to_review_queue_entry(row) for row in rows]

    def list_due_review_queue_items(
        self,
        *,
        now: datetime | None = None,
        limit: int | None = None,
    ) -> list[ReviewQueueEntry]:
        current = (now or datetime.now(timezone.utc)).isoformat()
        conn = self._get_connection()
        sql = (
            "SELECT * FROM review_queue "
            "WHERE next_attempt_at <= ? "
            "ORDER BY next_attempt_at ASC, enqueued_at ASC, issue_ref ASC"
        )
        params: list[Any] = [current]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        rows = conn.execute(sql, params).fetchall()
        return [self._row_to_review_queue_entry(row) for row in rows]

    def review_queue_count(self) -> int:
        conn = self._get_connection()
        row = conn.execute("SELECT COUNT(*) FROM review_queue").fetchone()
        return int(row[0])

    def due_review_queue_count(
        self,
        *,
        now: datetime | None = None,
    ) -> int:
        current = (now or datetime.now(timezone.utc)).isoformat()
        conn = self._get_connection()
        row = conn.execute(
            "SELECT COUNT(*) FROM review_queue WHERE next_attempt_at <= ?",
            (current,),
        ).fetchone()
        return int(row[0])

    def update_review_queue_item(
        self,
        issue_ref: str,
        *,
        next_attempt_at: str,
        last_result: str,
        last_reason: str | None = None,
        last_state_digest: str | None = None,
        blocked_streak: int | None = None,
        blocked_class: str | None = None,
        now: datetime | None = None,
    ) -> None:
        current = (now or datetime.now(timezone.utc)).isoformat()
        conn = self._get_connection()
        parts = [
            "updated_at = ?",
            "next_attempt_at = ?",
            "last_attempt_at = ?",
            "attempt_count = attempt_count + 1",
            "last_result = ?",
            "last_reason = ?",
            "last_state_digest = ?",
        ]
        params: list[Any] = [
            current,
            next_attempt_at,
            current,
            last_result,
            last_reason,
            last_state_digest,
        ]
        if blocked_streak is not None:
            parts.append("blocked_streak = ?")
            params.append(blocked_streak)
        if blocked_class is not None or blocked_streak is not None:
            # When streak is explicitly set, always set class too (may be None to clear)
            parts.append("blocked_class = ?")
            params.append(blocked_class)
        params.append(issue_ref)
        conn.execute(
            f"UPDATE review_queue SET {', '.join(parts)} WHERE issue_ref = ?",
            tuple(params),
        )
        conn.commit()

    def reschedule_review_queue_item(
        self,
        issue_ref: str,
        *,
        next_attempt_at: str,
        now: datetime | None = None,
    ) -> None:
        current = (now or datetime.now(timezone.utc)).isoformat()
        conn = self._get_connection()
        conn.execute(
            "UPDATE review_queue "
            "SET updated_at = ?, next_attempt_at = ? "
            "WHERE issue_ref = ?",
            (
                current,
                next_attempt_at,
                issue_ref,
            ),
        )
        conn.commit()

    def delete_review_queue_item(self, issue_ref: str) -> None:
        conn = self._get_connection()
        conn.execute(
            "DELETE FROM review_queue WHERE issue_ref = ?",
            (issue_ref,),
        )
        conn.commit()

    # -- Metrics ------------------------------------------------------------

    def record_metric_event(
        self,
        event_type: str,
        *,
        issue_ref: str | None = None,
        payload: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> int:
        ts = (now or datetime.now(timezone.utc)).isoformat()
        payload_json = None
        if payload is not None:
            payload_json = json.dumps(payload, sort_keys=True)
        conn = self._get_connection()
        cursor = conn.execute(
            "INSERT INTO consumer_metrics (event_type, issue_ref, payload_json, created_at) "
            "VALUES (?, ?, ?, ?)",
            (event_type, issue_ref, payload_json, ts),
        )
        conn.commit()
        row_id = cursor.lastrowid
        if row_id is None:
            raise RuntimeError("consumer metric insert did not return a row id")
        return row_id

    def count_metric_events_since(
        self,
        since: datetime,
        *,
        event_types: tuple[str, ...] | None = None,
    ) -> dict[str, int]:
        conn = self._get_connection()
        params: list[Any] = [since.isoformat()]
        sql = (
            "SELECT event_type, COUNT(*) AS count "
            "FROM consumer_metrics "
            "WHERE created_at >= ?"
        )
        if event_types:
            placeholders = ", ".join("?" for _ in event_types)
            sql += f" AND event_type IN ({placeholders})"  # noqa: S608
            params.extend(event_types)
        sql += " GROUP BY event_type"
        rows = conn.execute(sql, params).fetchall()
        return {str(row["event_type"]): int(row["count"]) for row in rows}

    def metric_events_since(
        self,
        since: datetime,
        *,
        event_types: tuple[str, ...] | None = None,
    ) -> list[MetricEvent]:
        conn = self._get_connection()
        params: list[Any] = [since.isoformat()]
        sql = (
            "SELECT id, event_type, issue_ref, payload_json, created_at "
            "FROM consumer_metrics "
            "WHERE created_at >= ?"
        )
        if event_types:
            placeholders = ", ".join("?" for _ in event_types)
            sql += f" AND event_type IN ({placeholders})"  # noqa: S608
            params.extend(event_types)
        sql += " ORDER BY created_at ASC, id ASC"
        rows = conn.execute(sql, params).fetchall()
        return [
            MetricEvent(
                id=int(row["id"]),
                event_type=str(row["event_type"]),
                issue_ref=(
                    str(row["issue_ref"]) if row["issue_ref"] is not None else None
                ),
                payload_json=(
                    str(row["payload_json"])
                    if row["payload_json"] is not None
                    else None
                ),
                created_at=str(row["created_at"]),
            )
            for row in rows
        ]

    def recent_metric_events(self, limit: int = 50) -> list[MetricEvent]:
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT id, event_type, issue_ref, payload_json, created_at "
            "FROM consumer_metrics ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [
            MetricEvent(
                id=int(row["id"]),
                event_type=str(row["event_type"]),
                issue_ref=(
                    str(row["issue_ref"]) if row["issue_ref"] is not None else None
                ),
                payload_json=(
                    str(row["payload_json"])
                    if row["payload_json"] is not None
                    else None
                ),
                created_at=str(row["created_at"]),
            )
            for row in rows
        ]

    def occupied_slot_seconds_since(
        self,
        since: datetime,
        *,
        now: datetime | None = None,
    ) -> float:
        current = now or datetime.now(timezone.utc)
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT started_at, completed_at FROM sessions "
            "WHERE started_at IS NOT NULL"
        ).fetchall()
        total = 0.0
        for row in rows:
            started_at = datetime.fromisoformat(str(row["started_at"]))
            if started_at.tzinfo is None:
                started_at = started_at.replace(tzinfo=timezone.utc)
            completed_raw = row["completed_at"]
            completed_at = (
                datetime.fromisoformat(str(completed_raw))
                if completed_raw is not None
                else current
            )
            if completed_at.tzinfo is None:
                completed_at = completed_at.replace(tzinfo=timezone.utc)
            interval_start = max(started_at, since)
            interval_end = min(completed_at, current)
            if interval_end > interval_start:
                total += (interval_end - interval_start).total_seconds()
        return total

    @staticmethod
    def _row_to_session(row: sqlite3.Row) -> SessionInfo:
        return SessionInfo(
            id=row["id"],
            issue_ref=row["issue_ref"],
            repo_prefix=row["repo_prefix"],
            worktree_path=row["worktree_path"],
            branch_name=row["branch_name"],
            executor=row["executor"],
            slot_id=row["slot_id"],
            status=row["status"],
            phase=row["phase"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            outcome_json=row["outcome_json"],
            failure_reason=row["failure_reason"],
            retry_count=row["retry_count"],
            pr_url=row["pr_url"],
            provenance_id=row["provenance_id"],
            session_kind=row["session_kind"] or "new_work",
            repair_pr_url=row["repair_pr_url"],
            branch_reconcile_state=row["branch_reconcile_state"],
            branch_reconcile_error=row["branch_reconcile_error"],
            resolution_kind=row["resolution_kind"],
            verification_class=row["verification_class"],
            resolution_evidence_json=row["resolution_evidence_json"],
            resolution_action=row["resolution_action"],
            done_reason=row["done_reason"],
        )

    @staticmethod
    def _row_to_review_queue_entry(row: sqlite3.Row) -> ReviewQueueEntry:
        return ReviewQueueEntry(
            issue_ref=str(row["issue_ref"]),
            pr_url=str(row["pr_url"]),
            pr_repo=str(row["pr_repo"]),
            pr_number=int(row["pr_number"]),
            source_session_id=(
                str(row["source_session_id"])
                if row["source_session_id"] is not None
                else None
            ),
            enqueued_at=str(row["enqueued_at"]),
            updated_at=str(row["updated_at"]),
            next_attempt_at=str(row["next_attempt_at"]),
            last_attempt_at=(
                str(row["last_attempt_at"])
                if row["last_attempt_at"] is not None
                else None
            ),
            attempt_count=int(row["attempt_count"]),
            last_result=(
                str(row["last_result"]) if row["last_result"] is not None else None
            ),
            last_reason=(
                str(row["last_reason"]) if row["last_reason"] is not None else None
            ),
            last_state_digest=(
                str(row["last_state_digest"])
                if row["last_state_digest"] is not None
                else None
            ),
            blocked_streak=(
                int(row["blocked_streak"]) if row["blocked_streak"] is not None else 0
            ),
            blocked_class=(
                str(row["blocked_class"]) if row["blocked_class"] is not None else None
            ),
        )
