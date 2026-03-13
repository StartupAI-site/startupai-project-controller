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
- Active worktree: `/home/chris/projects/worktrees/controller/refactor/controller-10-10-phase-56`
- Active branch: `refactor/controller-10-10-phase-56`
- Fresh-main baseline already includes merged work through `origin/main` commit `17ab3aa`

Do not resume from the main checkout. Continue from the phase-56 worktree.

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
- `PR #68` `refactor: type execution finalization wiring cluster`
- `PR #69` `refactor: split review queue state processing cluster`
- `PR #70` `refactor: extract pr review state adapter cluster`
- `PR #71` `refactor: type review comment operational cluster`
- `PR #72` `refactor: type launch cycle support cluster`
- `PR #73` `refactor: split project field sync core query cluster`
- `PR #74` `refactor: split project field sync mutation ops cluster`
- `PR #75` `refactor: split pr board issue support cluster`
- `PR #76` `refactor: split session execution wiring cluster`
- `PR #77` `refactor: split deferred replay wiring cluster`
- `PR #78` `refactor: split reconciliation recovery wiring cluster`
- `PR #79` `refactor: split comment pr shell wiring cluster`
- `PR #80` `refactor: type codex session result cluster`
- `PR #81` `refactor: split review queue drain processing cluster`
- `PR #82` `refactor: split pr batch query cluster`
- `PR #83` `refactor: split pr support helper cluster`
- `PR #84` `refactor: type codex context wiring cluster`
- `PR #85` `refactor: type launch worktree helper cluster`
- `PR #86` `refactor: split runtime support helper cluster`
- `PR #87` `refactor: split launch runtime support cluster`
- `PR #88` `refactor: split resolution support cluster`
- `PR #89` `refactor: split prepared cycle wiring cluster`
- `PR #90` `refactor: split review queue preparation cluster`
- `PR #91` `refactor: split claimed session wiring cluster`
- `PR #92` `refactor: split pr compatibility surface cluster`
- `PR #93` `refactor: split prepared launch claim cluster`
- `PR #94` `refactor: split review queue group cluster`
- `PR #95` `refactor: type launch resolution support cluster`
- `PR #96` `refactor: split field sync shell cluster`
- `PR #97` `refactor: split review queue group processing cluster`

Current unmerged phase-56 batch:

- `src/startupai_controller/consumer_launch_claim_wiring.py`
- `src/startupai_controller/consumer_launch_preparation_wiring.py`
- `tests/test_architecture_boundaries.py`
- `docs/runbooks/codex-resume-hardening.md`

Phase-56 batch summary:

- extracts the launch-selection/preparation/error-handling cluster from `consumer_launch_claim_wiring.py` into the new `consumer_launch_preparation_wiring.py` module
- reduces `consumer_launch_claim_wiring.py` from `622` to `364` lines while keeping the outer launch/claim helper surface stable through aliases
- adds architecture ratchets so the launch/claim shell must route launch preparation through the dedicated module

Latest successful validation on the current phase-56 worktree:

- `python3 -m py_compile` on `consumer_launch_claim_wiring.py`, `consumer_launch_preparation_wiring.py`, and `tests/test_architecture_boundaries.py`: passed
- targeted `mypy --follow-imports=silent` on `consumer_launch_claim_wiring.py` and `consumer_launch_preparation_wiring.py`: passed
- targeted `pytest` on architecture-boundary and board-consumer slices: `184 passed`
- `uv run black --target-version py312` on `consumer_launch_claim_wiring.py`, `consumer_launch_preparation_wiring.py`, and `tests/test_architecture_boundaries.py`: passed
- full suite: `890 passed`

No PR is open yet for phase 56. No poller should be running until the next PR
is opened.

## Most Important Remaining Hotspots

Remaining structural hotspots after the phase-56 launch-preparation split:

- `502` lines in `src/startupai_controller/consumer_review_queue_wiring.py`
- `475` lines in `src/startupai_controller/consumer_operational_wiring.py`
- `426` lines in `src/startupai_controller/project_field_sync_operations.py`
- `364` lines in `src/startupai_controller/consumer_launch_claim_wiring.py`
- `339` lines in `src/startupai_controller/consumer_review_queue_processing.py`
- `326` lines in `src/startupai_controller/consumer_launch_preparation_wiring.py`
- `291` lines in `src/startupai_controller/project_field_sync.py`
- `289` lines in `src/startupai_controller/consumer_review_queue_group_processing.py`
- `17` lines in `src/startupai_controller/project_field_sync.py::_run_single_sync`
- `0` `Any` usages in `src/startupai_controller/consumer_context_helpers.py`
- `0` `Any` usages in `src/startupai_controller/consumer_launch_runtime_support_wiring.py`
- `0` `Any` usages in `src/startupai_controller/consumer_resolution_support_wiring.py`

Bounded-context completion estimate at handoff time:

- consumer/control-plane: about 99%
- automation/review: about 99.5%
- field sync: about 90-95%
- overall program: about 99%

## Recommended Next Batch

If phase 56 is not yet merged, finish shipping the launch-preparation split:

- `src/startupai_controller/consumer_launch_claim_wiring.py`
- `src/startupai_controller/consumer_launch_preparation_wiring.py`
- `tests/test_architecture_boundaries.py`

If phase 56 is already merged, the strongest remaining targets are:

- finishing the remaining top-level review-queue shell ratchet inside `src/startupai_controller/consumer_review_queue_wiring.py`
- finishing any last operational-shell ratchet inside `src/startupai_controller/consumer_operational_wiring.py` if the launch/claim and review-queue shells still leave mixed orchestration there

## Fresh-Session Prompt

Use this prompt if a new Codex session needs a direct resume instruction:

```text
Continue the approved hard-end-state refactor plan for startupai-project-controller from the existing worktree, without supervision, until the Definition of Done in docs/adr/002-hard-end-state-hardening.md is satisfied or a real blocker is hit.

Resume from:
- main checkout: /home/chris/projects/startupai-project-controller
- active worktree: /home/chris/projects/worktrees/controller/refactor/controller-10-10-phase-56
- active branch: refactor/controller-10-10-phase-56

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
- latest merged PRs: #66 through #97
- no PR is open yet for phase 56
- latest full local validation on phase 56 was 890 passed
- current batch is the launch-preparation split; next batch after merge is the remaining top-level review-queue or operational shell ratchet
```
