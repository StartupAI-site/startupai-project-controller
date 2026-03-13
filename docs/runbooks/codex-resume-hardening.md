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
- Active worktree: `/home/chris/projects/worktrees/controller/refactor/controller-10-10-phase-41`
- Active branch: `refactor/controller-10-10-phase-41`
- Fresh-main baseline already includes merged work through `origin/main` commit `350ed1f`

Do not resume from the main checkout. Continue from the phase-41 worktree.

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

Current unmerged phase-41 batch:

- `src/startupai_controller/adapters/pull_request_board_helpers.py`
- `src/startupai_controller/adapters/pull_request_query_helpers.py`
- `src/startupai_controller/adapters/pull_request_support.py`
- `src/startupai_controller/adapters/pull_requests.py`
- `tests/test_architecture_boundaries.py`

Phase-41 batch summary:

- extracts board/project mutations into `adapters/pull_request_board_helpers.py`
- extracts issue/PR query helpers into `adapters/pull_request_query_helpers.py`
- rewires `adapters/pull_requests.py` to depend on the dedicated helper modules directly instead of the support shim
- reduces `adapters/pull_request_support.py` from `706` lines to `61` lines
- removes the last literal `Any` usage from `adapters/pull_request_support.py`
- adds architecture-boundary coverage so `pull_requests.py` cannot regress back to the monolithic support shim

Latest successful validation on the current phase-41 worktree:

- `python3 -m py_compile` on `pull_request_board_helpers.py`, `pull_request_query_helpers.py`, `pull_request_support.py`, `pull_requests.py`, and `tests/test_architecture_boundaries.py`: passed
- targeted `mypy` on `pull_request_board_helpers.py`, `pull_request_query_helpers.py`, `pull_request_support.py`, and `pull_requests.py`: passed
- targeted `pytest` on architecture-boundary, board-consumer, review-state-adapter, github-cli-adapter, and contract-output slices: `218 passed`
- `uv run black --check .`: passed
- full suite: `881 passed`

No PR is open yet for phase 41. No poller should be running until the next PR
is opened.

## Most Important Remaining Hotspots

Remaining structural hotspots after the phase-41 PR-support split:

- `1206` lines in `src/startupai_controller/consumer_operational_wiring.py`
- `1089` lines in `src/startupai_controller/adapters/pull_requests.py`
- `829` lines in `src/startupai_controller/consumer_review_queue_processing.py`
- `623` lines in `src/startupai_controller/consumer_review_queue_wiring.py`
- `441` lines in `src/startupai_controller/consumer_comment_pr_shell_wiring.py`
- `439` lines in `src/startupai_controller/consumer_codex_comment_wiring.py`
- `433` lines in `src/startupai_controller/consumer_review_queue_drain_processing.py`
- `375` lines in `src/startupai_controller/adapters/pull_request_query_helpers.py`
- `367` lines in `src/startupai_controller/adapters/pull_request_board_helpers.py`
- `339` lines in `src/startupai_controller/adapters/pull_request_batch_queries.py`
- `882` lines in `src/startupai_controller/consumer_execution_outcome_wiring.py`
- `647` lines in `src/startupai_controller/application/consumer/execution.py`
- `464` lines in `src/startupai_controller/consumer_cycle_wiring.py`
- `378` lines in `src/startupai_controller/consumer_deferred_action_helpers.py`
- `259` lines in `src/startupai_controller/consumer_reconciliation_wiring.py`
- `331` lines in `src/startupai_controller/consumer_session_execution_wiring.py`
- `377` lines in `src/startupai_controller/project_field_sync_operations.py`
- `352` lines in `src/startupai_controller/project_field_sync_core.py`
- `341` lines in `src/startupai_controller/project_field_sync_queries.py`
- `252` lines in `src/startupai_controller/project_field_sync_mutations.py`
- `168` lines in `src/startupai_controller/project_field_sync.py`
- `61` lines in `src/startupai_controller/adapters/pull_request_support.py`
- `0` `Any` usages in `src/startupai_controller/adapters/pull_requests.py`
- `0` `Any` usages in `src/startupai_controller/adapters/pull_request_support.py`
- `0` `Any` usages in `src/startupai_controller/adapters/pull_request_board_helpers.py`
- `0` `Any` usages in `src/startupai_controller/adapters/pull_request_query_helpers.py`
- `1` `Any` usage in `src/startupai_controller/consumer_codex_comment_wiring.py`
- `3` `Any` usages in `src/startupai_controller/consumer_cycle_wiring.py`
- `3` `Any` usages in `src/startupai_controller/project_field_sync_operations.py`
- `4` `Any` usages in `src/startupai_controller/project_field_sync_queries.py`
- `2` `Any` usages in `src/startupai_controller/project_field_sync_core.py`
- `2` `Any` usages in `src/startupai_controller/consumer_launch_support_wiring.py`

Bounded-context completion estimate at handoff time:

- consumer/control-plane: about 97-98%
- automation/review: about 97%
- field sync: about 60-65%
- overall program: about 98%

## Recommended Next Batch

If phase 41 is not yet merged, finish shipping the PR support-helper split:

- `src/startupai_controller/adapters/pull_request_board_helpers.py`
- `src/startupai_controller/adapters/pull_request_query_helpers.py`
- `src/startupai_controller/adapters/pull_request_support.py`
- `src/startupai_controller/adapters/pull_requests.py`

Once phase 41 is merged, the strongest next target is still the remaining
review-processing and consumer shell cluster:

- `src/startupai_controller/consumer_operational_wiring.py`
- `src/startupai_controller/consumer_review_queue_processing.py`
- `src/startupai_controller/consumer_codex_comment_wiring.py`

After that, the biggest structural work still pending is:

- finishing the remaining payload/probe split inside `src/startupai_controller/adapters/pull_requests.py`
- finishing the remaining claim/reconciliation shell split inside `src/startupai_controller/consumer_operational_wiring.py`
- finishing the remaining prompt/input typing cleanup around `src/startupai_controller/consumer_codex_runtime_wiring.py`, `src/startupai_controller/consumer_codex_helpers.py`, and `src/startupai_controller/consumer_codex_comment_wiring.py`
- deeper helper typing around `src/startupai_controller/consumer_launch_helpers.py`
- worktree helper typing around `src/startupai_controller/consumer_worktree_helpers.py`
- any final field-sync follow-up if the operations/query modules still need another ratchet

## Fresh-Session Prompt

Use this prompt if a new Codex session needs a direct resume instruction:

```text
Continue the approved hard-end-state refactor plan for startupai-project-controller from the existing worktree, without supervision, until the Definition of Done in docs/adr/002-hard-end-state-hardening.md is satisfied or a real blocker is hit.

Resume from:
- main checkout: /home/chris/projects/startupai-project-controller
- active worktree: /home/chris/projects/worktrees/controller/refactor/controller-10-10-phase-41
- active branch: refactor/controller-10-10-phase-41

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
- latest merged PRs: #66, #67, #68, #69, #70, #71, #72, #73, #74, #75, #76, #77, #78, #79, #80, #81, and #82
- no PR is open yet for phase 41
- latest full local validation on phase 41 was 881 passed
- current batch is the PR support-helper split; next batch after merge is still the remaining review-processing and consumer shell cluster
```
