# Codex Resume: Hard-End-State Hardening

This note exists to let a fresh Codex session resume the controller hardening
program without losing the approved plan, the active worktree, or the
autonomous execution rules.

This file is a handoff note, not the plan source of truth.

The authoritative plan and execution rules are:

- `docs/adr/002-hard-end-state-hardening.md`
- `docs/runbooks/autonomous-phase-execution.md`

## Resume From Here

- Main checkout: `/home/chris/projects/startupai-project-controller`
- Active worktree: `/home/chris/projects/worktrees/controller/refactor/controller-10-10-phase-57`
- Active branch: `refactor/controller-10-10-phase-57`
- Fresh-main baseline already includes merged work through `origin/main` commit `e208611`

Do not resume from the main checkout. Continue from the phase-57 worktree.

For this repository, continue using the existing manual `git worktree` flow
under `/home/chris/projects/worktrees/controller/...`. Do not assume the shared
`wt-create.sh` helper has a controller mapping.

## First Files To Read

1. `docs/adr/002-hard-end-state-hardening.md`
2. `docs/runbooks/autonomous-phase-execution.md`
3. `CLAUDE.md`
4. `AGENTS.md`

## Operating Rules Already Approved

- Continue autonomously across phases and PRs until the Definition of Done in
  `docs/adr/002-hard-end-state-hardening.md` is satisfied or a real blocker is
  encountered.
- Do not ask the user for routine next steps.
- After opening a PR:
  - start a background poller that runs `gh pr checks --required` every 120
    seconds
  - keep a shorter foreground watch while actively working
  - treat the first green CI result as an immediate merge trigger
- A green PR may not remain open across another poll cycle.
- After merge:
  - stop the poller
  - verify merged state
  - fetch `origin/main`
  - create the next fresh worktree from latest mainline state
  - continue immediately
- Use `apply_patch` for edits.
- Preserve public CLI and JSON/report contracts.

## Current Program State

Recent merged phases:

- `PR #66` through `PR #98`

Current unmerged phase-57 batch:

- `src/startupai_controller/payload_types.py`
- `src/startupai_controller/control_plane_runtime.py`
- `src/startupai_controller/consumer_runtime_support_wiring.py`
- `src/startupai_controller/consumer_types.py`
- `src/startupai_controller/consumer_workflow.py`
- `src/startupai_controller/ports/ready_flow.py`
- `src/startupai_controller/application/control_plane/tick.py`
- `src/startupai_controller/application/automation/ready_claim.py`
- `src/startupai_controller/application/consumer/cycle.py`
- `src/startupai_controller/application/consumer/daemon.py`
- `src/startupai_controller/application/consumer/preflight.py`
- `src/startupai_controller/application/consumer/preflight_runtime.py`
- `src/startupai_controller/application/consumer/reconciliation.py`
- `src/startupai_controller/application/consumer/status.py`
- `tests/test_architecture_boundaries.py`

Phase-57 batch summary:

- removes the remaining literal `Any` usage from `application/` and preserves
  `runtime/` at zero literal `Any` usage
- introduces shared payload aliases in `payload_types.py` so status/admission
  contracts stay explicit without reintroducing shell dependencies
- tightens `PreparedCycleContext`, ready-flow payloads, and control-plane health
  payload types around the frozen machine-consumed JSON surfaces
- adds an architecture ratchet proving `application/` and `runtime/` do not
  import or reference literal `Any`

Latest successful validation on the current phase-57 worktree:

- `python3 -m py_compile` on the edited application/shared payload files and
  `tests/test_architecture_boundaries.py`: passed
- targeted `mypy --follow-imports=silent` on the edited application/shared
  payload files: passed
- targeted `pytest` on architecture-boundary, consumer, control-plane,
  automation, and contract-output slices: `341 passed`
- full suite: `891 passed`

No PR is open yet for phase 57. No poller should be running until the next PR
is opened.

## Hard-End-State Assessment

Current assessment on the phase-57 worktree:

- consumer/control-plane bounded context: complete
- automation/review bounded context: complete
- field sync bounded context: complete
- literal `Any` usage in `application/`: `0`
- literal `Any` usage in `runtime/`: `0`
- public CLI and JSON/report contract tests: green
- full test suite: green

The remaining large shell modules are compatibility-facing delegators with
architecture-boundary ratchets in place. They are no longer carrying mixed
application/runtime responsibilities that require another extraction phase.

## Recommended Next Batch

If phase 57 is not yet merged, ship it as the final hard-end-state batch.

If phase 57 is already merged, the hard-end-state hardening program is complete
and no further refactor batch is recommended.

## Fresh-Session Prompt

Use this prompt if a new Codex session needs a direct resume instruction:

```text
Finish shipping the final hard-end-state phase for startupai-project-controller from the existing worktree, without supervision, and stop only after the PR is merged or a real blocker is hit.

Resume from:
- main checkout: /home/chris/projects/startupai-project-controller
- active worktree: /home/chris/projects/worktrees/controller/refactor/controller-10-10-phase-57
- active branch: refactor/controller-10-10-phase-57

Read first:
- /home/chris/projects/startupai-project-controller/docs/adr/002-hard-end-state-hardening.md
- /home/chris/projects/startupai-project-controller/docs/runbooks/autonomous-phase-execution.md
- /home/chris/projects/startupai-project-controller/docs/runbooks/codex-resume-hardening.md
- /home/chris/projects/startupai-project-controller/CLAUDE.md
- /home/chris/projects/startupai-project-controller/AGENTS.md

Operating rules already approved:
- work autonomously across phases and PRs
- open a PR when the batch is ready
- start a background poller every 120 seconds once the PR is open
- merge immediately on first green CI result
- stop the poller after merge
- fetch latest origin/main after merge
- verify the hard-end-state on fresh mainline state

Current state:
- latest merged PRs: #66 through #98
- no PR is open yet for phase 57
- latest full local validation on phase 57 was 891 passed
- phase 57 is intended to be the final hard-end-state batch
```
