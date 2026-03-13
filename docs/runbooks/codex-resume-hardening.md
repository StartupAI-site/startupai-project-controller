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
- Active worktree: `/home/chris/projects/worktrees/controller/refactor/controller-10-10-phase-53`
- Active branch: `refactor/controller-10-10-phase-53`
- Fresh-main baseline already includes merged work through `origin/main` commit `e8c0d1a`

Do not resume from the main checkout. Continue from the phase-53 worktree.

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

Current unmerged phase-53 batch:

- `src/startupai_controller/consumer_context_helpers.py`
- `src/startupai_controller/consumer_launch_runtime_support_wiring.py`
- `src/startupai_controller/consumer_resolution_support_wiring.py`
- `docs/runbooks/codex-resume-hardening.md`

Phase-53 batch summary:

- removes the remaining literal `Any` pockets from the extracted issue-context, launch/runtime support, and resolution-verification support shells
- aligns those helper seams with concrete controller types including `ConsumerConfig`, `CriticalPathConfig`, `ProjectItemSnapshot`, `PreparedCycleContext`, `PreparedLaunchContext`, `CycleResult`, `DaemonRuntime`, and `ResolutionPayload`
- keeps the runtime composition and public CLI/report behavior unchanged while making the wrapper contracts explicit and mypy-checkable

Latest successful validation on the current phase-53 worktree:

- `python3 -m py_compile` on `consumer_context_helpers.py`, `consumer_launch_runtime_support_wiring.py`, and `consumer_resolution_support_wiring.py`: passed
- targeted `mypy --follow-imports=silent` on the same three files: passed
- targeted `pytest` on architecture-boundary and board-consumer slices: `181 passed`
- `uv run black --target-version py312` on the same three files: passed
- full suite: `887 passed`

No PR is open yet for phase 53. No poller should be running until the next PR
is opened.

## Most Important Remaining Hotspots

Remaining structural hotspots after the phase-53 typing-cleanup split:

- `622` lines in `src/startupai_controller/consumer_launch_claim_wiring.py`
- `502` lines in `src/startupai_controller/consumer_review_queue_wiring.py`
- `596` lines in `src/startupai_controller/consumer_review_queue_processing.py`
- `475` lines in `src/startupai_controller/consumer_operational_wiring.py`
- `336` lines in `src/startupai_controller/consumer_launch_runtime_support_wiring.py`
- `311` lines in `src/startupai_controller/project_field_sync.py`
- `0` `Any` usages in `src/startupai_controller/consumer_context_helpers.py`
- `0` `Any` usages in `src/startupai_controller/consumer_launch_runtime_support_wiring.py`
- `0` `Any` usages in `src/startupai_controller/consumer_resolution_support_wiring.py`

Bounded-context completion estimate at handoff time:

- consumer/control-plane: about 99%
- automation/review: about 99%
- field sync: about 65-70%
- overall program: about 99%

## Recommended Next Batch

If phase 53 is not yet merged, finish shipping the remaining typing-cleanup
split:

- `src/startupai_controller/consumer_context_helpers.py`
- `src/startupai_controller/consumer_launch_runtime_support_wiring.py`
- `src/startupai_controller/consumer_resolution_support_wiring.py`

If phase 53 is already merged, the strongest remaining targets are:

- finishing the remaining review-queue orchestration split inside `src/startupai_controller/consumer_review_queue_processing.py`
- finishing the remaining launch/claim shell ratchet inside `src/startupai_controller/consumer_launch_claim_wiring.py`
- finishing the remaining field-sync shell ratchet inside `src/startupai_controller/project_field_sync.py`

## Fresh-Session Prompt

Use this prompt if a new Codex session needs a direct resume instruction:

```text
Continue the approved hard-end-state refactor plan for startupai-project-controller from the existing worktree, without supervision, until the Definition of Done in docs/adr/002-hard-end-state-hardening.md is satisfied or a real blocker is hit.

Resume from:
- main checkout: /home/chris/projects/startupai-project-controller
- active worktree: /home/chris/projects/worktrees/controller/refactor/controller-10-10-phase-53
- active branch: refactor/controller-10-10-phase-53

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
- latest merged PRs: #66 through #94
- no PR is open yet for phase 53
- latest full local validation on phase 53 was 887 passed
- current batch is the remaining typing cleanup split; next batch after merge is the remaining review-queue, launch/claim, or field-sync shell ratchet
```
