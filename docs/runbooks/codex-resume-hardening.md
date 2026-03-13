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
- Active worktree: `/home/chris/projects/worktrees/controller/refactor/controller-10-10-phase-43`
- Active branch: `refactor/controller-10-10-phase-43`
- Fresh-main baseline already includes merged work through `origin/main` commit `f2b246d`

Do not resume from the main checkout. Continue from the phase-43 worktree.

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

Current unmerged phase-43 batch:

- `src/startupai_controller/application/consumer/launch.py`
- `src/startupai_controller/consumer_launch_helpers.py`
- `src/startupai_controller/consumer_launch_support_wiring.py`
- `src/startupai_controller/consumer_support_wiring.py`
- `src/startupai_controller/consumer_worktree_helpers.py`

Phase-43 batch summary:

- types the remaining launch and worktree helper seam around `ConsumerConfig`, `IssueRef`, `SessionStorePort`, `WorktreePort`, and `ConsumerRuntimeStatePort`
- adds precise subprocess runner aliases to launch-layer helper paths without violating application-layer architecture boundaries
- rewires worktree wrapper signatures in `consumer_support_wiring.py` to expose concrete path, subprocess, session store, and worktree port contracts
- removes literal `Any` usage from `application/consumer/launch.py`, `consumer_launch_helpers.py`, `consumer_launch_support_wiring.py`, and `consumer_worktree_helpers.py`

Latest successful validation on the current phase-43 worktree:

- `python3 -m py_compile` on `application/consumer/launch.py`, `consumer_launch_helpers.py`, `consumer_launch_support_wiring.py`, `consumer_support_wiring.py`, and `consumer_worktree_helpers.py`: passed
- targeted `mypy --follow-imports=silent` on `application/consumer/launch.py`, `consumer_launch_helpers.py`, `consumer_launch_support_wiring.py`, and `consumer_worktree_helpers.py`: passed
- targeted `pytest` on architecture-boundary, board-consumer, board-control-plane, local-process-adapter, and contract-output slices: `186 passed`
- `uv run black --check .`: passed
- full suite: `881 passed`

No PR is open yet for phase 43. No poller should be running until the next PR
is opened.

## Most Important Remaining Hotspots

Remaining structural hotspots after the phase-42 codex/context typing split:

- `1206` lines in `src/startupai_controller/consumer_operational_wiring.py`
- `1089` lines in `src/startupai_controller/adapters/pull_requests.py`
- `829` lines in `src/startupai_controller/consumer_review_queue_processing.py`
- `623` lines in `src/startupai_controller/consumer_review_queue_wiring.py`
- `488` lines in `src/startupai_controller/consumer_cycle_wiring.py`
- `466` lines in `src/startupai_controller/application/consumer/launch.py`
- `438` lines in `src/startupai_controller/consumer_codex_comment_wiring.py`
- `433` lines in `src/startupai_controller/consumer_review_queue_drain_processing.py`
- `379` lines in `src/startupai_controller/consumer_codex_helpers.py`
- `375` lines in `src/startupai_controller/adapters/pull_request_query_helpers.py`
- `367` lines in `src/startupai_controller/adapters/pull_request_board_helpers.py`
- `339` lines in `src/startupai_controller/adapters/pull_request_batch_queries.py`
- `291` lines in `src/startupai_controller/consumer_launch_support_wiring.py`
- `311` lines in `src/startupai_controller/consumer_launch_helpers.py`
- `183` lines in `src/startupai_controller/consumer_worktree_helpers.py`
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
- remaining `Any` pockets are still concentrated in `consumer_support_wiring.py`, `consumer_context_helpers.py`, and field-sync support modules

Bounded-context completion estimate at handoff time:

- consumer/control-plane: about 98%
- automation/review: about 97%
- field sync: about 60-65%
- overall program: about 98%

## Recommended Next Batch

If phase 43 is not yet merged, finish shipping the launch/worktree typing split:

- `src/startupai_controller/application/consumer/launch.py`
- `src/startupai_controller/consumer_launch_helpers.py`
- `src/startupai_controller/consumer_launch_support_wiring.py`
- `src/startupai_controller/consumer_support_wiring.py`
- `src/startupai_controller/consumer_worktree_helpers.py`

Once phase 43 is merged, the strongest next target is still the remaining
review-processing and consumer shell cluster:

- `src/startupai_controller/consumer_operational_wiring.py`
- `src/startupai_controller/consumer_review_queue_processing.py`
- `src/startupai_controller/consumer_support_wiring.py`

After that, the biggest structural work still pending is:

- finishing the remaining payload/probe split inside `src/startupai_controller/adapters/pull_requests.py`
- finishing the remaining claim/reconciliation shell split inside `src/startupai_controller/consumer_operational_wiring.py`
- finishing the remaining launch/support shell cleanup inside `src/startupai_controller/consumer_support_wiring.py`
- any final field-sync follow-up if the operations/query modules still need another ratchet

## Fresh-Session Prompt

Use this prompt if a new Codex session needs a direct resume instruction:

```text
Continue the approved hard-end-state refactor plan for startupai-project-controller from the existing worktree, without supervision, until the Definition of Done in docs/adr/002-hard-end-state-hardening.md is satisfied or a real blocker is hit.

Resume from:
- main checkout: /home/chris/projects/startupai-project-controller
- active worktree: /home/chris/projects/worktrees/controller/refactor/controller-10-10-phase-43
- active branch: refactor/controller-10-10-phase-43

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
- latest merged PRs: #66, #67, #68, #69, #70, #71, #72, #73, #74, #75, #76, #77, #78, #79, #80, #81, #82, #83, and #84
- no PR is open yet for phase 43
- latest full local validation on phase 43 was 881 passed
- current batch is the launch/worktree typing split; next batch after merge is still the remaining review-processing and consumer shell cluster
```
