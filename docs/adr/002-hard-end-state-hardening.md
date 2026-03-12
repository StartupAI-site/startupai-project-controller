# ADR-002: Hard-End-State Hardening Program

**Status**: Accepted
**Date**: 2026-03-12
**Decision Makers**: Chris Walker, Codex
**Supersedes**: none

## Context

The controller already has a strong extracted architecture, but it still carries
transitional compatibility seams, large mixed-responsibility modules, and
machine-consumed JSON/report outputs whose contracts are real in practice but
not yet frozen explicitly.

The next phase of work is not a feature delivery phase. It is a hardening
program intended to move the repo to a stricter end state without changing the
public CLI surface or operational semantics.

## Decision

### 1. Optimize for the hard end state

This program targets the strongest defensible architecture, not the shortest
credible path. It proceeds as a continuous multi-PR effort until the final
definition of done is satisfied.

### 2. Bounded contexts

The hardening work is organized around three bounded contexts:

- consumer/control-plane
- automation/review
- field sync

`project_field_sync.py` is treated as a separate bounded context rather than
being forced into the consumer/control-plane abstraction stack.

### 3. Contract freeze

The following machine-consumed outputs are treated as public operational
contracts and must be frozen with fixture/schema-backed tests before broad
refactoring:

- `board_consumer status --json`
- `board_control_plane tick --json`
- field-sync audit/sync JSON outputs
- codex session result schema interactions

For these contracts, semantic compatibility means the same keys, nesting,
required null/default behavior, and derived field behavior. Key order and
whitespace are not authoritative unless a consumer is proven to depend on raw
text.

### 4. Compatibility shim policy

`board_io.py`, `consumer_db.py`, and `github_http.py` remain deprecated facades
initially. Internal application/runtime code may not depend on them.

Facade removal requires one of two conditions:

- a fixed-scope audit proves there are no remaining consumers and the removal PR
  records that evidence, or
- an ADR explicitly approves a breaking change and the release notes document it

If code-search audit is clean but undocumented/manual consumers cannot be ruled
out, facades stay in place through at least one merged deployment cycle in
which dependent automation runs successfully without shim use, or until explicit
human signoff approves removal.

Deprecation is documentation-first. Runtime warnings are optional later and only
if they are proven low-risk.

### 5. Architecture and typing rules

- `domain/` stays pure and infrastructure-free.
- `application/` depends only on `domain/` and `ports/`.
- Adapters own normalization from transport/persistence/process data into typed
  inward-facing objects.
- Runtime composes concrete adapters; shells only parse CLI and delegate.
- One adapter-local raw payload layer is allowed for GitHub GraphQL/JSON, but
  raw transport dictionaries may not cross into `application/`.
- `Any` is forbidden in `application/` and `runtime/` except for explicitly
  tracked temporary debt.

### 6. Hotspot governance

The hardening program applies extra ratchets to this hotspot set:

- `adapters/pull_requests.py`
- `consumer_review_queue_helpers.py`
- `consumer_operational_wiring.py`
- `application/automation/ready_wiring.py`
- `project_field_sync.py`

Hotspot decomposition is governed by explicit responsibility boundaries recorded
in ADR notes and enforced via review checklists. This program does not rely on
ad hoc numeric decomposition metrics to decide whether a split is successful.

### 7. Autonomous execution rule

The work proceeds phase by phase without waiting for additional human prompts:

1. implement phase work on a branch/worktree
2. open the PR
3. get required checks green
4. merge
5. refresh from latest `origin/main`
6. start the next phase from fresh mainline state

Execution pauses only for blockers that cannot be resolved from the codebase,
docs, or established contracts.

## Consequences

### Positive

- Large refactors gain fixture-backed contract safety.
- Compatibility decisions become explicit rather than incidental.
- The repo can harden iteratively without losing architectural direction.
- Future PRs can be judged against documented bounded contexts and hotspot rules.

### Tradeoffs

- The first milestone adds tests and ADR material before major code movement.
- Deprecated facades may remain longer than pure refactoring would prefer.
- The program is longer than a minimum-change cleanup path.
