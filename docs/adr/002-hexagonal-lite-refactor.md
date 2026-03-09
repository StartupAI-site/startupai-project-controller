# ADR-002: Hexagonal-Lite Refactor

**Status**: Accepted
**Date**: 2026-03-09
**Relates to**: ADR-007 (hexagonal-domain in startupai-crew)

## Context

The controller works but core modules are too large and entangled:

| Module | Lines | Policy % | Adapter % |
|--------|-------|----------|-----------|
| `board_consumer.py` | 6716 | 10.4% | 37.2% |
| `board_automation.py` | 5359 | 15% | 11% |
| `board_io.py` | 3051 | 3% | 46% |

Five functions violate the dependency rule by mixing pure policy decisions
with GitHub transport, SQLite persistence, or subprocess execution. This
makes the system hard to reason about, test in isolation, and change
without regression.

## Decision

Restructure toward ADR-007 hexagonal-domain architecture with three layers:

```
domain/     Pure policy — stdlib only, zero outer-layer imports
ports/      Protocol classes — domain/ + stdlib only
adapters/   Implementations wrapping board_io, consumer_db, github_http
```

### Domain modules

| Module | Responsibility |
|--------|---------------|
| `models.py` | 25 domain types (policy outcomes, state, port contracts) |
| `review_queue_policy.py` | Blocker classification, escalation ceilings, retry backoff, requeue-vs-escalate decisions |
| `repair_policy.py` | Branch patterns, provenance markers, acceptance criteria |
| `scheduling_policy.py` | WIP limits, priority ranking, admission watermarks |
| `verdict_policy.py` | Verdict marker format, backfill eligibility |
| `launch_policy.py` | PR candidate classification, session kind determination, reconciliation truth table |
| `rescue_policy.py` | Rescue decision table (13 branches) |
| `automerge_policy.py` | Auto-merge gate evaluation |
| `resolution_policy.py` | Resolution normalization, autoclose decisions |

### Dependency rule

`domain/` imports ONLY stdlib + other `domain/` modules. Prohibited:
`board_io`, `consumer_db`, `github_http`, `subprocess`, `sqlite3`,
`logging`, `os.environ`.

### Port protocols

Six typed protocols formalize contracts between orchestration and
external mechanisms: ReviewStatePort, PullRequestPort, BoardMutationPort,
SessionStorePort, WorktreePort, GhRunnerPort.

### Backward compatibility

Source modules retain thin wrappers delegating to domain functions.
Re-export shims maintain import compatibility for tests and external
callers. Config-object parameters are destructured to primitives at the
orchestration boundary before calling domain functions.

## Migration Status

### Completed (fully migrated)

- **Types**: Orchestrators import `SessionInfo`, `ReviewQueueEntry`,
  `PrGateStatus`, `CheckObservation`, `OpenPullRequest`, `ReviewSnapshot`,
  `ClaimReadyResult`, and all policy outcome types from `domain/models.py`.
- **Policy functions**: Orchestrators import `_priority_rank`, `MARKER_PREFIX`,
  `VALID_EXECUTORS`, `parse_resolution_comment`, `admission_watermarks`,
  `has_structured_acceptance_criteria`, and all review queue/verdict/launch/
  rescue/automerge policy functions from `domain/` modules.
- **Domain decision wiring**: `review_rescue()` delegates to `rescue_decision()`,
  `automerge_review()` delegates to `automerge_gate_decision()`,
  `_reconcile_board_truth()` delegates to `reconcile_in_progress_decision()`,
  `_prepare_launch_candidate()` delegates to `launch_session_kind()`,
  `_drain_review_queue()` delegates to `requeue_or_escalate()` and
  `blocked_streak_needs_escalation()`.

### Transitional (not yet migrated)

Orchestrators (`board_consumer.py`, `board_automation.py`, `board_graph.py`)
still import mechanism functions directly from `board_io.py` for GitHub CLI
operations not yet covered by port methods. These include:

- Board query functions (`_list_project_items_by_status`, `_snapshot_to_issue_ref`, etc.)
- Board mutation functions (`_set_status_if_changed`, `_post_comment`, `close_issue`, etc.)
- PR query functions (`query_open_pull_requests`, `_query_pr_gate_status`, etc.)
- Snapshot/memo types (`CycleBoardSnapshot`, `CycleGitHubMemo`, `_ProjectItemSnapshot`)
- Adapter-internal types (`CodexReviewVerdict`, `PullRequestViewPayload`,
  `MetricEvent`, `RecoveredLease`) remain defined in their source modules

These are marked with `# transitional: mechanism access (see ADR-002)` comments
in the import statements. Migration to port-based access is tracked as follow-up
work requiring port protocol extension and composition-root DI wiring.

### Ports and adapters — current wiring

**SessionStorePort** (`ports/session_store.py`): Extended with 13 methods
covering review queue CRUD, session queries, requeue counts, and active worker
listing. `SqliteSessionStore` (`adapters/sqlite_store.py`) implements all
methods by delegation to `ConsumerDB`.

**Composition root**: `_prepare_cycle()` constructs `session_store = SqliteSessionStore(db)`
and passes it to `_reconcile_board_truth()` and `_drain_review_queue()`. The
`_cmd_reconcile()` CLI entrypoint also constructs its own store instance.

**Wired functions** (read/write via port, not direct `db.` calls):
- `_reconcile_board_truth()` — `active_workers()`, `latest_session_for_issue()`, `update_session()`
- `_drain_review_queue()` — `list_review_queue_items()`, `get_requeue_state()`,
  `delete_review_queue_item()`, `increment_requeue_count()`

**Not yet wired** (require new ports — M10 scope):
- `_apply_resolution_action()` — uses metrics, deferred actions, board mutations
- `_hydrate_issue_context()` — uses context cache (`get_issue_context`, `set_issue_context`)
- `_prepare_launch_candidate()` → `_setup_launch_worktree()` — uses worktree/subprocess/metrics

These functions use `ConsumerDB` methods outside the SessionStorePort scope
(metrics recording, deferred action queuing, context caching). Port protocols
for these operations (MetricsPort, DeferredActionPort, ContextCachePort,
WorktreePort) are deferred to M10.

### God-function decomposition

**`_drain_review_queue()`** (was 314 lines): Split into coordinator + 3 focused sub-functions:
- `_prune_stale_review_entries()` — remove queue rows for issues no longer in Review
- `_seed_new_review_entries()` — seed queue rows for new Review issues
- `_reconcile_review_queue_identity()` — reconcile queue rows against current PR identity

**`_prepare_launch_candidate()`** (was 208 lines): Split into coordinator + 2 sub-functions:
- `_setup_launch_worktree()` — worktree creation and repair branch reconciliation
- `_resolve_launch_runtime()` — workflow loading and effective config computation

### Import migration — `_parse_pr_url`

`board_consumer.py` now imports `parse_pr_url` from `domain/repair_policy`
(canonical path) instead of `_parse_pr_url` from `board_io`. The `board_io`
function is retained as a thin delegation shim for external callers.

### Remaining board_io surface

Board query/mutation functions (`_list_project_items_by_status`,
`_set_status_if_changed`, `_post_comment`, `close_issue`,
`query_open_pull_requests`, etc.) and snapshot types (`CycleBoardSnapshot`,
`_ProjectItemSnapshot`) are still imported directly from `board_io` by
orchestrators. These require ReviewStatePort, PullRequestPort, and
BoardMutationPort extensions — tracked for M8/M9.

## Consequences

- Domain functions are independently testable with no mocks
- Policy changes cannot accidentally introduce side effects
- Characterization tests lock down behavior before each extraction
- SessionStorePort is a real boundary — `_reconcile_board_truth` and
  `_drain_review_queue` access persistence only through the port
- God-functions are decomposed into focused sub-functions with single responsibilities
- Re-export shims add temporary maintenance burden (removal tracked separately)
- Board query/mutation mechanism access remains transitional — orchestrators still
  call board_io directly for GitHub operations not yet covered by ports

## Scope

Strict behavior preservation — no semantic changes to queue behavior,
retry policy, board state machine, or auto-merge logic. No production
deploy or restart.
