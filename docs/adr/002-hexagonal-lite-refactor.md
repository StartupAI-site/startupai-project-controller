# ADR-002: Hexagonal-Lite Refactor

**Status**: Accepted
**Date**: 2026-03-09
**Relates to**: ADR-007 (hexagonal-domain in startupai-crew)

## Context

The controller works but core modules are too large and entangled:

| Module | Lines | Policy % | Adapter % |
|--------|-------|----------|-----------|
| `board_consumer.py` | 8030 | transitional application shell | low direct shim use |
| `board_automation.py` | 5595 | transitional application shell | medium adapter coupling |
| `board_io.py` | 1471 | compatibility/mechanism remainder | shrinking |

Five functions violate the dependency rule by mixing pure policy decisions
with GitHub transport, SQLite persistence, or subprocess execution. This
makes the system hard to reason about, test in isolation, and change
without regression.

## Decision

Restructure toward ADR-007 hexagonal-domain architecture with three layers:

```
domain/     Pure policy — stdlib only, zero outer-layer imports
ports/      Protocol classes — domain/ + stdlib only
adapters/   Capability-specific mechanism implementations over transport/store code
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
orchestration boundary before calling domain functions. Runtime wiring is
assembled per command / per daemon cycle through `runtime/wiring.py`.

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
- **Composition root**: runtime adapter selection now happens in
  `runtime/wiring.py`, which builds per-command / per-cycle bundles for
  GitHub-backed ports, session store access, and worktree/process access.
- **Canonical runtime boundary**: runtime orchestrators no longer import
  `board_io.py`, `consumer_db.py`, `github_http.py`, or adapter helper modules
  directly. Their runtime boundary is now `domain/`, `ports/`, and
  `runtime/wiring.py`.
- **Graph input cleanup**: `board_graph.py` consumes typed inputs and no longer
  imports adapters or shim modules directly.
- **Capability adapters**: GitHub runtime access is now split across adapter
  modules by responsibility (`pull_requests`, `review_state`, `board_mutation`,
  transport/types), instead of routing orchestration through one giant shim.

### Transitional (not yet migrated)

The remaining transitional surfaces are now mostly **adapter concentration** or
**compatibility-shell** issues, not orchestration-boundary issues:

- `GitHubCliAdapter` remains as a compatibility façade while runtime ownership
  is split across capability adapters. It is smaller than before, but not yet
  deleted.
- `board_io.py` is now a compatibility shell and legacy wrapper surface. It is
  no longer imported by runtime orchestrators, but compatibility callers/tests
  still rely on it.
- `consumer_db.py` remains the underlying persistence/mechanism host behind
  `SqliteSessionStore`; the persistence boundary is real, but the store logic
  is not yet fully decomposed by capability.
- Adapter-internal types such as `CodexReviewVerdict`, `PullRequestViewPayload`,
  `MetricEvent`, and `RecoveredLease` remain owned by adapter/mechanism modules,
  not the domain.

### Ports and adapters — current wiring

**SessionStorePort** (`ports/session_store.py`): Extended with 13 methods
covering review queue CRUD, session queries, requeue counts, and active worker
listing. `SqliteSessionStore` (`adapters/sqlite_store.py`) implements all
methods by delegation to `ConsumerDB`.

**Composition root**: `runtime/wiring.py` builds:
- a per-command / per-cycle GitHub port bundle
- a session store adapter
- a worktree/process adapter

Coordinators consume those bundles instead of choosing concrete adapters inline.

**Wired functions** (read/write via port, not direct `db.` calls):
- `_reconcile_board_truth()` — `active_workers()`, `latest_session_for_issue()`, `update_session()`
- `_drain_review_queue()` — `list_review_queue_items()`, `get_requeue_state()`,
  `delete_review_queue_item()`, `increment_requeue_count()`
- `board_control_plane._tick()` — review queue drain and admission flow through
  runtime wiring
- `board_graph.py` — typed input only, no adapter/shim reads

**Still transitional**:
- `_apply_resolution_action()` and some status/metrics paths still rely on
  coordinator-local compatibility wrappers over the store/mutation boundary.
- `_hydrate_issue_context()` and worktree preparation still use the worktree /
  issue-context boundary more directly than the cleaner port-only ideal.
- Compatibility shell paths remain for external callers and tests, even though
  runtime orchestration is now on canonical boundaries.

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

`board_io.py` is now primarily a compatibility shell. Runtime orchestrators do
not import it directly, and capability adapters own the active GitHub
board/PR/review paths. The remaining `board_io.py` surface exists to preserve
legacy caller/test compatibility and to provide a narrow fallback layer while
older entry points are retired.

## Consequences

- Domain functions are independently testable with no mocks
- Policy changes cannot accidentally introduce side effects
- Characterization tests lock down behavior before each extraction
- SessionStorePort is a real boundary — `_reconcile_board_truth` and
  `_drain_review_queue` access persistence only through the port
- God-functions are decomposed into focused sub-functions with single responsibilities
- Re-export shims add temporary maintenance burden (removal tracked separately)
- Board query/mutation mechanism access is substantially improved — runtime
  orchestrators now use canonical domain/ports/runtime wiring boundaries, while
  the remaining transitional work is concentrated in compatibility shells and
  adapter consolidation rather than boundary violations

## Scope

Strict behavior preservation — no semantic changes to queue behavior,
retry policy, board state machine, or auto-merge logic. No production
deploy or restart.
