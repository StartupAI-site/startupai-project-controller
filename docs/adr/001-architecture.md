# ADR-001: Project Board Controller Architecture

**Status**: Accepted
**Date**: 2026-03-10 (consolidates decisions from 2026-03-07 through 2026-03-09)
**Decision Makers**: Chris Walker, Claude AI (Opus 4.6), Codex
**Predecessors**: startupai-crew ADR-018 (local board consumer), ADR-019 (extraction record)

## Context

The GitHub Project board is the execution control plane for StartupAI across
three repos (crew, app, site). The board consumer daemon and board automation
scripts were originally co-located in `startupai-crew` but had zero coupling to
the CrewAI/Modal application code. Over time the control-plane grew to ~19k
lines of Python across 13 modules, warranting extraction into a standalone
repository with independent CI, lighter dependency footprint, and explicit
boundaries.

This ADR consolidates four prior documents:

| Prior Document | Disposition |
|----------------|-------------|
| crew ADR-018 (local board consumer, 460 lines) | Superseded -- tombstone in crew |
| crew ADR-019 (extraction record, 68 lines) | Superseded -- tombstone in crew |
| controller ADR-001 (extraction mirror of 019) | Replaced by this document |
| controller ADR-002 (hexagonal-lite refactor) | Folded into Section 3 below |

## Decision

### 1. Origin and Rationale

Extract all control-plane code into
[`StartupAI-site/startupai-project-controller`](https://github.com/StartupAI-site/startupai-project-controller).

Reasons:

- **Clean separation** -- board automation is operationally independent of AI
  validation; no CrewAI, Modal, or Supabase dependencies.
- **Independent CI** -- controller tests run in ~9s with stdlib + PyYAML only.
- **Lighter startupai-crew** -- focused on its actual purpose.
- **Explicit dependency boundaries** -- instead of implicit co-location.

### 2. What This System Does

**Board consumer daemon** (`board_consumer.py`): Polls the GitHub Project board,
claims `Ready` issues, dispatches Codex coding sessions, and manages the full
issue lifecycle on a single execution-authority machine.

**Board automation** (`board_automation.py`): 16 subcommands invoked by GitHub
Actions workflows (still triggered from the crew repo) for workflow-driven board
mutations -- mark-done, auto-promote, enforce-ready-dependencies, review sync,
auto-merge, etc.

**Board semantics**:

| Status | Meaning |
|--------|---------|
| `Backlog` | Triaged work, not yet committed |
| `Ready` | Dependency-safe queue, eligible for local execution |
| `In Progress` | Live local coding slot on this machine |
| `Review` | Open PR exists, active execution has ended |
| `Blocked` | Execution or review cannot proceed without intervention |
| `Done` | Merged and closed |

**Authority boundary**:

- *Consumer-owned*: rank and admit `Backlog` items into `Ready`, select next
  `Ready` issue, acquire local lease, claim `Ready -> In Progress`, create/reuse
  worktree, launch `codex exec`, track heartbeats, reconcile stale `In Progress`
  cards, own `Review -> In Progress` for repair sessions, adopt qualifying open
  PRs, load repo-owned runtime workflow contracts, replay deferred mutations.
- *Workflow-owned*: `In Progress -> Review` sync, Codex review gate, Copilot
  review signal, required CI checks, review rescue/merge re-evaluation,
  auto-merge enablement, `Review -> Done`, merge-driven dependency truth.

**Operating model**: This machine is the sole coding executor authority for the
three protected repos. `codex` is the automated coding worker; `claude` handles
routing, review, and handoff; `human` is a manual lane.

**Provenance contract**: Every consumer session emits a deterministic
machine-readable marker (session ID, issue ref, repo prefix, branch name,
executor). A PR is valid for protected coding lanes only when it links the
issue, contains valid provenance, and its provenance issue ref matches.

**WORKFLOW.md**: Each protected repo ships a `WORKFLOW.md` with YAML front
matter (`poll_interval_seconds`, `codex_timeout_seconds`, `max_retries`, etc.)
and a Markdown prompt body. Central config (`board-automation-config.json`)
always wins for governance; `WORKFLOW.md` may override only repo-local runtime
behavior.

**Observability**: `status --json` exposes active slots and sessions, loaded
workflow metadata, repo availability, degraded mode, deferred-action count,
control-plane health state (`healthy`, `degraded_recovering`, `degraded_stuck`),
and active workers by slot with session kind.

### 3. Architecture (As-Built)

The codebase follows a hexagonal-lite pattern with an explicit application layer
(per crew ADR-007 principles):

```
application/ Use-case coordination -- entry shells delegate here
domain/      Pure policy -- stdlib only, zero outer-layer imports
ports/       Protocol classes -- domain/ + stdlib only
adapters/    Capability-specific mechanism implementations over transport/store code
runtime/     Composition root -- wiring adapters to ports per command/cycle
```

**Application modules** (behavior-preserving extraction targets now living out of
entry shells):

| Area | Modules |
|------|---------|
| Consumer | `application/consumer/cycle.py`, `preflight.py`, `preflight_runtime.py`, `reconciliation.py`, `recovery.py`, `daemon.py`, `status.py` |
| Control plane | `application/control_plane/tick.py` |
| Automation | `application/automation/admit_backlog.py`, `admission_helpers.py`, `auto_promote.py`, `ready_claim.py`, `ready_dependencies.py`, `ready_wiring.py`, `review_sync.py`, `review_rescue.py`, `review_wiring.py`, `codex_gate.py`, `audit_in_progress.py`, `dispatch_agent.py`, `rebalance.py`, `execution_policy.py` |

**Domain modules** (pure policy, independently testable):

| Module | Responsibility |
|--------|---------------|
| `models.py` | 25 domain types (policy outcomes, state, port contracts) |
| `review_queue_policy.py` | Blocker classification, escalation ceilings, retry backoff, requeue-vs-escalate |
| `repair_policy.py` | Branch patterns, provenance markers, acceptance criteria |
| `scheduling_policy.py` | WIP limits, priority ranking, admission watermarks |
| `verdict_policy.py` | Verdict marker format, backfill eligibility |
| `launch_policy.py` | PR candidate classification, session kind, reconciliation truth table |
| `rescue_policy.py` | Rescue decision table (13 branches) |
| `automerge_policy.py` | Auto-merge gate evaluation |
| `resolution_policy.py` | Resolution normalization, autoclose decisions |

**Dependency rule**: `domain/` imports ONLY stdlib + other `domain/` modules.
Prohibited: `board_io`, `consumer_db`, `github_http`, `subprocess`, `sqlite3`,
`logging`, `os.environ`.

**Port protocols** (9 typed boundaries):

| Port | Purpose |
|------|---------|
| `ReviewStatePort` | Review queue state queries |
| `PullRequestPort` | PR queries and gate status |
| `BoardMutationPort` | Board state transitions |
| `SessionStorePort` | Session/lease persistence (13 methods) |
| `WorktreePort` | Worktree creation and management |
| `IssueContextPort` | Issue title/body/labels reads for launch preparation |
| `ProcessRunnerPort` | Codex/process execution boundary |
| `ServiceControlPort` | Service-state checks for control-plane health |
| `ReadyFlowPort` | Shared ready-queue orchestration boundary used by consumer/control plane |

**Composition root** (`runtime/wiring.py`): Builds per-command / per-cycle
adapter bundles for GitHub-backed ports, session store access, and
worktree/process access. Orchestrators consume these bundles instead of
choosing concrete adapters inline.

**Capability adapters**: GitHub runtime access is split across adapter modules
by responsibility (`pull_requests`, `review_state`, `board_mutation`,
transport/types) instead of routing through a single giant shim.
`SqliteSessionStore` delegates to `adapters.consumer_db_store.ConsumerDB` for
the persistence boundary.

**Wired use cases** (runtime-owned composition, no adapter imports from entry shells):

- `application/consumer/cycle.py` -- consumer claimed-session path
- `application/control_plane/tick.py` -- control-plane tick orchestration
- `application/automation/*` -- extracted ready/review/execution flows
- `board_graph.py` -- typed input only, no adapter/shim reads

**Current state**: Runtime orchestrators no longer import the deleted top-level
compatibility modules `board_io.py`, `consumer_db.py`, or `github_http.py`.
Those audited import paths were removed after fixed-scope verification and
their responsibilities now live in canonical adapter modules:

- `GitHubCliAdapter` remains as an adapter-level compatibility surface while
  runtime ownership is split across capability adapters.
- Persistence ownership lives in `adapters.consumer_db_store`.
- Direct GitHub HTTP transport ownership lives in
  `adapters.github_http_transport`.
- Adapter-internal types (`CodexReviewVerdict`, `PullRequestViewPayload`,
  `MetricEvent`, `RecoveredLease`) remain owned by adapter/mechanism modules.
- `board_consumer.py` no longer imports `board_automation.py`; consumer launch,
  review, daemon, recovery, reconciliation, codex, and support lanes now
  delegate through focused `consumer_*_wiring.py` / helper modules plus
  `application/consumer/*`.
- `board_automation.py` is primarily port factories, shell-facing re-exports,
  and CLI/parser glue; ready/review/execution/state/admission/CLI lanes now live
  in `automation_*_wiring.py`, `automation_cli_handlers.py`, and
  `application/automation/*`.
- Remaining shell weight is concentrated in adapter-level compatibility helpers
  and port-factory seams, not in top-level shim files.

**Deferred work**: Continued reduction of adapter-level compatibility surfaces
(for example `GitHubCliAdapter`) and any future persistence decomposition
beyond `adapters.consumer_db_store` remain follow-up work, not architectural
blockers.

### 4. Cross-Repo Relationship

**What stays in startupai-crew**:

- 16 GitHub Actions workflows -- triggered by events in that repo, repointed to
  checkout this repo and run controller code at runtime.
- 7 self-contained workflows -- tag-contract, 3 diagnostic-gate, 3 diagnostic-lock.
- `systemd/startupai-consumer.service` stub pointing to canonical copy here.
- Tombstone ADRs (018, 019) referencing this document.

**Bridge pattern**: Workflows use `actions/checkout` to pull this repo, then
`uv sync && uv run python -m startupai_controller.<module> <subcmd>`.

```yaml
- uses: actions/checkout@v4
  with:
    repository: StartupAI-site/startupai-project-controller
    path: controller
- uses: astral-sh/setup-uv@v3
- run: cd controller && uv sync --group dev
- run: cd controller && uv run python -m startupai_controller.board_automation <subcmd>
```

**Config governance** lives in this repo:

- `config/board-automation-config.json` -- execution authority, protected repos,
  provenance rules, concurrency, feature flags.
- `config/critical-paths.json` -- dependency ordering for critical-path issues.
- `config/project-field-sync-config.json` -- field sync definitions.
- `config/codex_session_result.schema.json` -- session result validation.

## Consequences

### Positive

- Board state reflects real execution truth; stale `In Progress` cards self-heal.
- Repo-local runtime tuning via `WORKFLOW.md` without daemon restart.
- Domain policy functions are independently testable with no mocks.
- Policy changes cannot accidentally introduce GitHub/SQLite side effects.
- Independent CI runs in ~9s without heavy CrewAI/Supabase deps.
- Lighter startupai-crew focused on its actual purpose.
- Significant automation/control-plane behavior now lives in dedicated
  application use-case modules instead of entry-shell command bodies.
- Entrypoints no longer depend on each other at runtime; `board_consumer.py`
  now reaches automation behavior through neutral bridge modules instead of
  importing `board_automation.py`.

### Tradeoffs

- ~30s overhead per workflow run for checkout + `uv sync` (acceptable for
  cron-scheduled workflows).
- Operators must clone this repo for local consumer management.
- The local machine remains a strong single point of execution authority.
- Provenance remains mandatory for protected coding PRs.
- Missing or invalid repo workflows disable dispatch for that repo.
- Ambiguous PR states bias toward `Blocked` (safer but noisier).
- Adapter-level compatibility surfaces (for example `GitHubCliAdapter`) and
  test-oriented wrapper seams remain where they still buy stable monkeypatch
  boundaries, even though the top-level shim files are gone.
- The future long-lived runner remains deferred behind an interface boundary.
