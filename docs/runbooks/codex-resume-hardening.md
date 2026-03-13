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
- Active worktree: `/home/chris/projects/worktrees/controller/refactor/controller-10-10-phase-45`
- Active branch: `refactor/controller-10-10-phase-45`
- Fresh-main baseline already includes merged work through `origin/main` commit `a5b457e`

Do not resume from the main checkout. Continue from the phase-45 worktree.

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

Current unmerged phase-45 batch:

- `src/startupai_controller/consumer_launch_runtime_support_wiring.py`
- `src/startupai_controller/consumer_support_wiring.py`
- `tests/test_architecture_boundaries.py`

Phase-45 batch summary:

- extracts the remaining launch/runtime-support helper cluster from `consumer_support_wiring.py` into the new `consumer_launch_runtime_support_wiring.py` module
- preserves the existing `consumer_support_wiring.py` surface as a thin compatibility layer for resolution verification plus re-exported launch/runtime support seams
- extends architecture-boundary coverage so `consumer_support_wiring.py` must route both runtime/control and launch/runtime support through dedicated modules
- reduces `consumer_support_wiring.py` from `402` to `157` lines and cuts its literal `Any` count from `55` to `18`

Latest successful validation on the current phase-45 worktree:

- `python3 -m py_compile` on `consumer_launch_runtime_support_wiring.py`, `consumer_support_wiring.py`, and `tests/test_architecture_boundaries.py`: passed
- targeted `mypy --follow-imports=silent` on `consumer_launch_runtime_support_wiring.py` and `consumer_support_wiring.py`: passed
- targeted `pytest` on architecture-boundary, board-consumer, characterization-hot-path, and contract-output slices: `210 passed`
- `uv run black --check` on `consumer_launch_runtime_support_wiring.py`, `consumer_support_wiring.py`, and `tests/test_architecture_boundaries.py`: passed
- full suite: `882 passed`

No PR is open yet for phase 45. No poller should be running until the next PR
is opened.

## Most Important Remaining Hotspots

Remaining structural hotspots after the phase-42 codex/context typing split:

- `1207` lines in `src/startupai_controller/consumer_operational_wiring.py`
- `1090` lines in `src/startupai_controller/adapters/pull_requests.py`
- `830` lines in `src/startupai_controller/consumer_review_queue_processing.py`
- `623` lines in `src/startupai_controller/consumer_review_queue_wiring.py`
- `488` lines in `src/startupai_controller/consumer_cycle_wiring.py`
- `466` lines in `src/startupai_controller/application/consumer/launch.py`
- `438` lines in `src/startupai_controller/consumer_codex_comment_wiring.py`
- `433` lines in `src/startupai_controller/consumer_review_queue_drain_processing.py`
- `372` lines in `src/startupai_controller/consumer_runtime_support_wiring.py`
- `379` lines in `src/startupai_controller/consumer_codex_helpers.py`
- `274` lines in `src/startupai_controller/consumer_launch_runtime_support_wiring.py`
- `375` lines in `src/startupai_controller/adapters/pull_request_query_helpers.py`
- `367` lines in `src/startupai_controller/adapters/pull_request_board_helpers.py`
- `339` lines in `src/startupai_controller/adapters/pull_request_batch_queries.py`
- `291` lines in `src/startupai_controller/consumer_launch_support_wiring.py`
- `311` lines in `src/startupai_controller/consumer_launch_helpers.py`
- `183` lines in `src/startupai_controller/consumer_worktree_helpers.py`
- `157` lines in `src/startupai_controller/consumer_support_wiring.py`
- `168` lines in `src/startupai_controller/consumer_codex_runtime_wiring.py`
- `0` `Any` usages in `src/startupai_controller/consumer_codex_comment_wiring.py`
- `0` `Any` usages in `src/startupai_controller/consumer_codex_runtime_wiring.py`
- `0` `Any` usages in `src/startupai_controller/consumer_codex_helpers.py`
- `0` `Any` usages in `src/startupai_controller/consumer_cycle_wiring.py`
- `0` `Any` usages in `src/startupai_controller/adapters/pull_requests.py`
- `0` `Any` usages in `src/startupai_controller/application/consumer/launch.py`
- `0` `Any` usages in `src/startupai_controller/consumer_launch_helpers.py`
- `0` `Any` usages in `src/startupai_controller/consumer_launch_support_wiring.py`
- `0` `Any` usages in `src/startupai_controller/consumer_worktree_helpers.py`
- `0` `Any` usages in `src/startupai_controller/consumer_runtime_support_wiring.py`
- remaining `Any` pockets are still concentrated in `consumer_launch_runtime_support_wiring.py`, `consumer_support_wiring.py`, `consumer_context_helpers.py`, and field-sync support modules

Bounded-context completion estimate at handoff time:

- consumer/control-plane: about 98%
- automation/review: about 97%
- field sync: about 60-65%
- overall program: about 98%

## Recommended Next Batch

If phase 45 is not yet merged, finish shipping the launch/runtime support split:

- `src/startupai_controller/consumer_launch_runtime_support_wiring.py`
- `src/startupai_controller/consumer_support_wiring.py`
- `tests/test_architecture_boundaries.py`

Once phase 45 is merged, the strongest next target is still the remaining
resolution-support and operational cluster:

- `src/startupai_controller/consumer_support_wiring.py`
- `src/startupai_controller/consumer_operational_wiring.py`
- `src/startupai_controller/consumer_review_queue_processing.py`

After that, the biggest structural work still pending is:

- finishing the remaining payload/probe split inside `src/startupai_controller/adapters/pull_requests.py`
- finishing the remaining resolution-verification cleanup inside `src/startupai_controller/consumer_support_wiring.py`
- finishing the remaining typing cleanup inside `src/startupai_controller/consumer_launch_runtime_support_wiring.py`
- finishing the remaining claim/reconciliation shell split inside `src/startupai_controller/consumer_operational_wiring.py`
- any final field-sync follow-up if the operations/query modules still need another ratchet

## Fresh-Session Prompt

Use this prompt if a new Codex session needs a direct resume instruction:

```text
Continue the approved hard-end-state refactor plan for startupai-project-controller from the existing worktree, without supervision, until the Definition of Done in docs/adr/002-hard-end-state-hardening.md is satisfied or a real blocker is hit.

Resume from:
- main checkout: /home/chris/projects/startupai-project-controller
- active worktree: /home/chris/projects/worktrees/controller/refactor/controller-10-10-phase-45
- active branch: refactor/controller-10-10-phase-45

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
- latest merged PRs: #66, #67, #68, #69, #70, #71, #72, #73, #74, #75, #76, #77, #78, #79, #80, #81, #82, #83, #84, #85, and #86
- no PR is open yet for phase 45
- latest full local validation on phase 45 was 882 passed
- current batch is the launch/runtime support split; next batch after merge is still the remaining resolution-support and operational cluster
```
