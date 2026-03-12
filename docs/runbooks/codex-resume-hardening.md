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
- Active worktree: `/home/chris/projects/worktrees/controller/refactor/controller-10-10-phase-26`
- Active branch: `refactor/controller-10-10-phase-26`
- Fresh-main baseline already includes merged work through `origin/main` commit `d741934`

Do not resume from the main checkout. Continue from the phase-26 worktree.

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

- `PR #66` `refactor: type launch claim wiring cluster`
- `PR #67` `refactor: type review handoff wiring cluster`

Latest successful validation on the just-merged work:

- targeted `mypy` on touched modules: passed
- targeted `pytest`: passed
- full suite: `871 passed`

No PR is open at the moment. No poller should be running until the next PR is
opened.

## Most Important Remaining Hotspots

Remaining `Any` density and structural hotspots at the time this note was
written:

- `117` `Any` usages in `src/startupai_controller/consumer_cycle_wiring.py`
- `101` `Any` usages in `src/startupai_controller/consumer_operational_wiring.py`
- `69` `Any` usages in `src/startupai_controller/consumer_execution_outcome_wiring.py`
- `79` `Any` usages in `src/startupai_controller/consumer_review_queue_helpers.py`
- `2325` lines in `src/startupai_controller/adapters/pull_requests.py`
- `1377` lines in `src/startupai_controller/project_field_sync.py`

Bounded-context completion estimate at handoff time:

- consumer/control-plane: about 80%
- automation/review: about 60%
- field sync: about 15-20%
- overall program: about 70%

## Recommended Next Batch

Pick the next larger responsibility-cluster batch from the phase-26 worktree.

The strongest next target is the execution/finalization cluster:

- `src/startupai_controller/consumer_cycle_wiring.py`
- `src/startupai_controller/consumer_operational_wiring.py`
- `src/startupai_controller/consumer_execution_outcome_wiring.py`

After that, the biggest structural work still pending is:

- `src/startupai_controller/adapters/pull_requests.py`
- `src/startupai_controller/project_field_sync.py`

## Fresh-Session Prompt

Use this prompt if a new Codex session needs a direct resume instruction:

```text
Continue the approved hard-end-state refactor plan for startupai-project-controller from the existing worktree, without supervision, until the Definition of Done in docs/adr/002-hard-end-state-hardening.md is satisfied or a real blocker is hit.

Resume from:
- main checkout: /home/chris/projects/startupai-project-controller
- active worktree: /home/chris/projects/worktrees/controller/refactor/controller-10-10-phase-26
- active branch: refactor/controller-10-10-phase-26

Read first:
- /home/chris/projects/startupai-project-controller/docs/adr/002-hard-end-state-hardening.md
- /home/chris/projects/startupai-project-controller/docs/runbooks/autonomous-phase-execution.md
- /home/chris/projects/startupai-project-controller/docs/runbooks/codex-resume-hardening.md
- /home/chris/projects/startupai-project-controller/CLAUDE.md
- /home/chris/projects/startupai-project-controller/AGENTS.md

Operating rules already approved:
- work autonomously across phases and PRs
- open a PR when a batch is ready
- start a background poller every 120 seconds once the PR is open
- merge immediately on first green CI result
- stop the poller after merge
- cut the next fresh worktree from latest origin/main
- continue immediately without asking for routine confirmation

Current state:
- latest merged PRs: #66 and #67
- no PR is open right now
- latest full local validation was 871 passed
- next best batch is the execution/finalization cluster in consumer_cycle_wiring.py, consumer_operational_wiring.py, and consumer_execution_outcome_wiring.py
```
