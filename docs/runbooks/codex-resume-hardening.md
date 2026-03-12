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
- Active worktree: `/home/chris/projects/worktrees/controller/refactor/controller-10-10-phase-33`
- Active branch: `refactor/controller-10-10-phase-33`
- Fresh-main baseline already includes merged work through `origin/main` commit `b785dfd`

Do not resume from the main checkout. Continue from the phase-33 worktree.

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

Latest successful validation on the current phase-33 worktree:

- targeted `mypy` on `adapters/pull_requests.py`, `adapters/pull_request_support.py`, and `adapters/github_cli.py`: passed
- targeted `pytest` on adapter/review-state/boundary/board-consumer slices: `205 passed`
- full suite: `872 passed`

No PR is open yet for phase 33. No poller should be running until the next PR
is opened.

## Most Important Remaining Hotspots

Remaining structural hotspots after the phase-33 PR board/issue support split:

- `984` lines in `src/startupai_controller/consumer_review_queue_processing.py`
- `1333` lines in `src/startupai_controller/consumer_operational_wiring.py`
- `1319` lines in `src/startupai_controller/adapters/pull_requests.py`
- `698` lines in `src/startupai_controller/adapters/pull_request_support.py`
- `623` lines in `src/startupai_controller/consumer_review_queue_wiring.py`
- `466` lines in `src/startupai_controller/consumer_codex_comment_wiring.py`
- `464` lines in `src/startupai_controller/consumer_cycle_wiring.py`
- `352` lines in `src/startupai_controller/project_field_sync_core.py`
- `377` lines in `src/startupai_controller/project_field_sync_operations.py`
- `341` lines in `src/startupai_controller/project_field_sync_queries.py`
- `252` lines in `src/startupai_controller/project_field_sync_mutations.py`
- `168` lines in `src/startupai_controller/project_field_sync.py`
- `263` lines in `src/startupai_controller/consumer_launch_support_wiring.py`
- `556` lines in `src/startupai_controller/consumer_claim_wiring.py`
- `443` lines in `src/startupai_controller/adapters/pull_request_review_state.py`
- `251` lines in `src/startupai_controller/consumer_launch_helpers.py`
- `3` `Any` usages in `src/startupai_controller/adapters/pull_requests.py`
- `5` `Any` usages in `src/startupai_controller/adapters/pull_request_support.py`
- `0` `Any` usages in `src/startupai_controller/project_field_sync.py`
- `3` `Any` usages in `src/startupai_controller/project_field_sync_operations.py`
- `4` `Any` usages in `src/startupai_controller/project_field_sync_queries.py`
- `0` `Any` usages in `src/startupai_controller/project_field_sync_mutations.py`
- `2` `Any` usages in `src/startupai_controller/project_field_sync_core.py`
- `4` `Any` usages in `src/startupai_controller/consumer_codex_comment_wiring.py`
- `3` `Any` usages in `src/startupai_controller/consumer_comment_pr_wiring.py`
- `3` `Any` usages in `src/startupai_controller/consumer_comment_pr_helpers.py`
- `3` `Any` usages in `src/startupai_controller/consumer_cycle_wiring.py`
- `2` `Any` usages in `src/startupai_controller/consumer_launch_support_wiring.py`
- `2` `Any` usages in `src/startupai_controller/consumer_operational_wiring.py`
- `0` `Any` usages in `src/startupai_controller/consumer_claim_wiring.py`
- `5` `Any` usages in `src/startupai_controller/control_plane_rescue.py`

Bounded-context completion estimate at handoff time:

- consumer/control-plane: about 94-95%
- automation/review: about 90-91%
- field sync: about 60-65%
- overall program: about 92-93%

## Recommended Next Batch

If phase 33 is not yet merged, finish shipping the current PR board/issue
support extraction:

- `src/startupai_controller/adapters/pull_requests.py`
- `src/startupai_controller/adapters/pull_request_support.py`

Once phase 33 is merged, the strongest next target is the remaining
review-processing and typed-shell cluster:

- `src/startupai_controller/consumer_review_queue_processing.py`
- `src/startupai_controller/consumer_operational_wiring.py`
- `src/startupai_controller/control_plane_rescue.py`

After that, the biggest structural work still pending is:

- finishing the remaining payload/probe split inside `src/startupai_controller/adapters/pull_requests.py`
- deeper helper typing around `src/startupai_controller/consumer_launch_helpers.py`
- worktree helper typing around `src/startupai_controller/consumer_worktree_helpers.py`
- the remaining typed-shell cleanup around control-plane rescue and comment/replay wiring
- any final field-sync follow-up if the operations/query modules still need another ratchet

## Fresh-Session Prompt

Use this prompt if a new Codex session needs a direct resume instruction:

```text
Continue the approved hard-end-state refactor plan for startupai-project-controller from the existing worktree, without supervision, until the Definition of Done in docs/adr/002-hard-end-state-hardening.md is satisfied or a real blocker is hit.

Resume from:
- main checkout: /home/chris/projects/startupai-project-controller
- active worktree: /home/chris/projects/worktrees/controller/refactor/controller-10-10-phase-33
- active branch: refactor/controller-10-10-phase-33

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
- latest merged PRs: #66, #67, #68, #69, #70, #71, #72, #73, and #74
- no PR is open yet for phase 33
- latest full local validation on phase 33 was 872 passed
- current batch is the PR board/issue support extraction; next batch after merge is the review-processing and typed-shell cluster
```
