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
- Active worktree: `/home/chris/projects/worktrees/controller/refactor/controller-10-10-phase-40`
- Active branch: `refactor/controller-10-10-phase-40`
- Fresh-main baseline already includes merged work through `origin/main` commit `3716918`

Do not resume from the main checkout. Continue from the phase-40 worktree.

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

Current unmerged phase-40 batch:

- `src/startupai_controller/adapters/github_types.py`
- `src/startupai_controller/adapters/pull_request_batch_queries.py`
- `src/startupai_controller/adapters/pull_request_support.py`
- `src/startupai_controller/adapters/pull_requests.py`
- `tests/test_architecture_boundaries.py`

Phase-40 batch summary:

- extracts the batched GraphQL PR hydration logic into `adapters/pull_request_batch_queries.py`
- promotes typed GitHub actor/comment/review/status payload nodes into `adapters/github_types.py`
- rewires `adapters/pull_requests.py` to delegate batched view/probe hydration through the new helper module while preserving single-PR behavior
- reduces `adapters/pull_requests.py` from `1321` lines to `1084` lines
- removes literal `Any` usage from `adapters/pull_requests.py` and leaves `adapters/pull_request_support.py` at one remaining `Any` seam

Latest successful validation on the current phase-40 worktree:

- `python3 -m py_compile` on `github_types.py`, `pull_request_support.py`, `pull_request_batch_queries.py`, `pull_requests.py`, and `tests/test_architecture_boundaries.py`: passed
- targeted `mypy` on `github_types.py`, `pull_request_support.py`, `pull_request_batch_queries.py`, `pull_requests.py`, and `pull_request_review_state.py`: passed
- targeted `pytest` on architecture-boundary, board-consumer, review-state-adapter, github-cli-adapter, and contract-output slices: `216 passed`
- full suite: `879 passed`

No PR is open yet for phase 40. No poller should be running until the next PR
is opened.

## Most Important Remaining Hotspots

Remaining structural hotspots after the phase-34 session-execution split:

- `1206` lines in `src/startupai_controller/consumer_operational_wiring.py`
- `829` lines in `src/startupai_controller/consumer_review_queue_processing.py`
- `1084` lines in `src/startupai_controller/adapters/pull_requests.py`
- `706` lines in `src/startupai_controller/adapters/pull_request_support.py`
- `623` lines in `src/startupai_controller/consumer_review_queue_wiring.py`
- `433` lines in `src/startupai_controller/consumer_review_queue_drain_processing.py`
- `339` lines in `src/startupai_controller/adapters/pull_request_batch_queries.py`
- `441` lines in `src/startupai_controller/consumer_comment_pr_shell_wiring.py`
- `439` lines in `src/startupai_controller/consumer_codex_comment_wiring.py`
- `882` lines in `src/startupai_controller/consumer_execution_outcome_wiring.py`
- `647` lines in `src/startupai_controller/application/consumer/execution.py`
- `464` lines in `src/startupai_controller/consumer_cycle_wiring.py`
- `378` lines in `src/startupai_controller/consumer_deferred_action_helpers.py`
- `259` lines in `src/startupai_controller/consumer_reconciliation_wiring.py`
- `331` lines in `src/startupai_controller/consumer_session_execution_wiring.py`
- `352` lines in `src/startupai_controller/project_field_sync_core.py`
- `377` lines in `src/startupai_controller/project_field_sync_operations.py`
- `341` lines in `src/startupai_controller/project_field_sync_queries.py`
- `252` lines in `src/startupai_controller/project_field_sync_mutations.py`
- `168` lines in `src/startupai_controller/project_field_sync.py`
- `263` lines in `src/startupai_controller/consumer_launch_support_wiring.py`
- `556` lines in `src/startupai_controller/consumer_claim_wiring.py`
- `443` lines in `src/startupai_controller/adapters/pull_request_review_state.py`
- `251` lines in `src/startupai_controller/consumer_launch_helpers.py`
- `0` `Any` usages in `src/startupai_controller/adapters/pull_requests.py`
- `1` `Any` usage in `src/startupai_controller/adapters/pull_request_support.py`
- `0` `Any` usages in `src/startupai_controller/control_plane_rescue.py`
- `0` `Any` usages in `src/startupai_controller/consumer_deferred_action_helpers.py`
- `0` `Any` usages in `src/startupai_controller/consumer_deferred_action_wiring.py`
- `0` `Any` usages in `src/startupai_controller/consumer_reconciliation_wiring.py`
- `0` `Any` usages in `src/startupai_controller/consumer_session_execution_wiring.py`
- `0` `Any` usages in `src/startupai_controller/consumer_operational_wiring.py`
- `1` `Any` usage in `src/startupai_controller/consumer_codex_comment_wiring.py`
- `0` `Any` usages in `src/startupai_controller/consumer_comment_pr_shell_wiring.py`
- `0` `Any` usages in `src/startupai_controller/project_field_sync.py`
- `3` `Any` usages in `src/startupai_controller/project_field_sync_operations.py`
- `4` `Any` usages in `src/startupai_controller/project_field_sync_queries.py`
- `0` `Any` usages in `src/startupai_controller/project_field_sync_mutations.py`
- `2` `Any` usages in `src/startupai_controller/project_field_sync_core.py`
- `0` `Any` usages in `src/startupai_controller/consumer_comment_pr_wiring.py`
- `0` `Any` usages in `src/startupai_controller/consumer_comment_pr_helpers.py`
- `3` `Any` usages in `src/startupai_controller/consumer_cycle_wiring.py`
- `2` `Any` usages in `src/startupai_controller/consumer_launch_support_wiring.py`
- `0` `Any` usages in `src/startupai_controller/consumer_claim_wiring.py`

Bounded-context completion estimate at handoff time:

- consumer/control-plane: about 97-98%
- automation/review: about 95%
- field sync: about 60-65%
- overall program: about 97%

## Recommended Next Batch

If phase 40 is not yet merged, finish shipping the PR batch-query split:

- `src/startupai_controller/adapters/github_types.py`
- `src/startupai_controller/adapters/pull_request_batch_queries.py`
- `src/startupai_controller/adapters/pull_requests.py`

Once phase 40 is merged, the strongest next target is still the remaining
review-processing and operational/adapter shell cluster:

- `src/startupai_controller/consumer_operational_wiring.py`
- `src/startupai_controller/consumer_review_queue_processing.py`
- `src/startupai_controller/adapters/pull_request_support.py`

After that, the biggest structural work still pending is:

- finishing the remaining payload/probe split inside `src/startupai_controller/adapters/pull_requests.py`
- finishing the remaining support/helper split inside `src/startupai_controller/adapters/pull_request_support.py`
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
- active worktree: /home/chris/projects/worktrees/controller/refactor/controller-10-10-phase-40
- active branch: refactor/controller-10-10-phase-40

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
- latest merged PRs: #66, #67, #68, #69, #70, #71, #72, #73, #74, #75, #76, #77, #78, #79, #80, and #81
- no PR is open yet for phase 40
- latest full local validation on phase 40 was 879 passed
- current batch is the PR batch-query split; next batch after merge is still the remaining review-processing and operational/adapter shell cluster
```
