# Codex Handoff: PR #115

Branch: `fix/stuck-drain-attribution`  
PR: `#115`  
Current head: `e461eb5` (`fix: repair stale ready review items directly`)

## What This Branch Already Fixes

- `029f094` `fix: interrupt stuck drain executions`
- `b623e00` `fix: tighten short burn drain handling`
- `3c76af3` `fix: bound preflight harness probes`
- `fbadf2f` `fix: reconcile stale ready review ownership`
- `2df54ea` `fix: reap timed out probe processes`
- `e461eb5` `fix: repair stale ready review items directly`

Behavior covered by the branch:

- stuck external execution during drain is classified and interrupted
- shutdown attribution is frozen at the first forced interrupt signal
- multiline workflow-error attribution is stricter
- stale board `Ready` items with surviving local review ownership are repaired
  directly back to `Review`
- timed-out auxiliary harness probes are reaped instead of lingering

## Live Evidence So Far

Primary validation run:

- Run dir: `/home/chris/.local/share/startupai/test-runs/20260315T165022Z`
- Summary: `/home/chris/.local/share/startupai/test-runs/20260315T165022Z/summary.json`
- Final local status:
  `/home/chris/.local/share/startupai/test-runs/20260315T165022Z/artifacts/final/status-local.json`

What that run proved:

- stale `Ready -> Review` repair now sticks on the live GitHub board
- `crew#10`, `crew#22`, and `crew#23` were observed back in `Review`
- final local state was clean: `active_leases=0`, `workers=[]`
- shutdown was natural
- drain timing held: `drain_request_slip_seconds=0.000264978...`
- final admission state ended at `ready_count=0`, `eligible_count=0`

What that run did not prove:

- no worker survived into drain
- `drain_stuck_external_execution` was not exercised live
- `shutdown_signal_sent_at` was not exercised live
- multiline generic `workflow_error` attribution was not exercised live

## Current Reality

The main unresolved problem is no longer obvious controller breakage. It is
validation signal:

- the board often yields `eligible_count=0`
- opportunistic short burns can be operationally healthy while proving only
  board-truth and shutdown timing behavior

There is also harness instrumentation noise still present in short burns:

- `preflight one-shot --dry-run timed out after 30.0 seconds`
- `tick-dry-run timed out after 30.0 seconds`

Those do not currently appear to break the main consumer run, but they do make
the summary noisier than it should be.

## Recommended Next Step

Choose one of these deliberately:

1. Merge `PR #115` now if test coverage plus partial live proof is enough.
2. Do one more controlled short burn with a genuinely claimable `Ready` item so
   the branch can live-prove:
   - `drain_stuck_external_execution`
   - `shutdown_signal_sent_at`
   - multiline generic workflow-error attribution

If continuing validation, do not rely on passive board waiting while the
controller is stopped. Start from a deliberately claimable issue.

## Suggested Operator Commands

Check current local state:

```bash
uv run python -m startupai_controller.board_consumer status --json --local-only
```

Recover interrupted local state safely:

```bash
uv run python -m startupai_controller.board_consumer recover-interrupted --json
```

Clear drain before a short burn:

```bash
uv run python -m startupai_controller.board_consumer resume
```

Run the standard short burn:

```bash
uv run python tools/limited_live_test.py \
  --duration-seconds 900 \
  --confirm-single-consumer \
  --confirmation-note "codex targeted validation"
```

Reapply drain after validation:

```bash
uv run python -m startupai_controller.board_consumer drain
```

## Handoff Prompt

Use this to start the next Codex session:

```text
Continue work on startupai-project-controller in worktree:
/home/chris/projects/worktrees/controller/fix/stuck-drain-attribution

Branch: fix/stuck-drain-attribution
PR: #115
Head: e461eb5

Read these first:
- docs/runbooks/consumer-recovery.md
- docs/runbooks/codex-handoff-pr115-2026-03-15.md

Current state:
- stale Ready -> Review board repair is live-proven fixed
- timed-out auxiliary probe processes are reaped correctly
- short burns are operationally healthy
- remaining unproven live paths are:
  - drain_stuck_external_execution
  - shutdown_signal_sent_at
  - multiline generic workflow-error attribution

Most likely next action:
- either merge PR #115 now
- or run one controlled 15-minute validation with a genuinely claimable Ready
  item to exercise the remaining runtime path

Do not spend another opportunistic burn on a board state with eligible_count=0.
```
