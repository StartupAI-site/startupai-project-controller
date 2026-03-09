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
| `review_queue_policy.py` | Blocker classification, escalation ceilings, retry backoff |
| `repair_policy.py` | Branch patterns, provenance markers, acceptance criteria |
| `scheduling_policy.py` | WIP limits, priority ranking, admission watermarks |
| `verdict_policy.py` | Verdict marker format, backfill eligibility |
| `launch_policy.py` | PR candidate classification decision table |
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

## Consequences

- Domain functions are independently testable with no mocks
- Policy changes cannot accidentally introduce side effects
- Orchestration shells become read-via-port → call-domain → apply-via-port
- Characterization tests lock down behavior before each extraction
- Re-export shims add temporary maintenance burden (removal tracked separately)

## Scope

Strict behavior preservation — no semantic changes to queue behavior,
retry policy, board state machine, or auto-merge logic. No production
deploy or restart.
