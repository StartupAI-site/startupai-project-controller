# Limited Live Test Runbook

Supervised one-hour burn-in for the refactored controller plus a first-pass
GitHub transport assessment.

## Purpose

Use this harness when you want to:

- run the real consumer against the normal live `Ready` queue for one hour
- collect structured baseline, timeline, and final diagnostics
- request a graceful drain at the hour mark
- leave the controller stopped and drained for review
- assess whether the current GitHub transport is good enough for live use

This is a proving exercise, not a transport migration. It uses the current
controller exactly as shipped. It does not switch to GitHub MCP.

## Latest Burn-In Result

The latest completed live run at
`~/.local/share/startupai/test-runs/20260313T185906Z/` concluded:

- `current transport acceptable but needs deeper instrumentation`

Operational follow-up policy from that run:

- keep the current GitHub transport layer for now
- do **not** migrate to GitHub MCP in the next defect batch
- fix controller workflow defects first
- persist deeper transport metrics in existing status/SLO outputs so the next
  burn-in can compare controller failures against transport behavior more
  precisely

Observed live defects from that run:

- repeated PR creation failures from invalid unpublished head branches
- one transient degraded window from an `In Progress` board-transition race

Observed review anomalies that still require artifact-first validation before
any policy change:

- `missing-copilot-review`
- missing codex verdict marker escalation on `app#126` / PR `#219`

Artifact-first validation command:

```bash
uv run python tools/validate_burnin_review_anomalies.py --write-report
```

Latest artifact-first validation result for `20260313T185906Z`:

- the burn-in artifacts contain repeated log evidence for both
  `missing-copilot-review` and missing codex verdict marker on
  `app#126` / PR `#219`
- matching status snapshots confirm the same issue/session context
- no contradictory artifact evidence shows controller misclassification
- therefore no review-policy change is included in this defect batch

## Safety Model

- Real board and PR actions are allowed.
- The harness runs the consumer as a supervised child process, not under
  `systemd`.
- "Drain the board" means graceful consumer drain only:
  stop new claims, let active work finish, then stop.
- It does **not** mass-mutate `Ready` or `In Progress` cards into terminal
  states.

## Required Safety Checks

1. Verify no other machine is running a consumer.
   This repository does not provide a distributed exclusivity lock. The
   cross-machine single-consumer check is therefore procedural and must be
   performed by the operator before starting the test.

2. Verify the local systemd service is not active.

   ```bash
   systemctl --user status startupai-consumer
   ```

3. Verify GitHub auth is healthy.

   ```bash
   gh auth status
   ```

4. Verify a live board read succeeds.

   ```bash
   uv run python -m startupai_controller.board_consumer status --json
   uv run python -m startupai_controller.board_consumer one-shot --dry-run --json
   ```

## Run Command

```bash
uv run python tools/limited_live_test.py \
  --confirm-single-consumer \
  --confirmation-note "verified all other consumer hosts are stopped"
```

Optional overrides:

- `--db-path <path>`
- `--consumer-interval-seconds <seconds>`
- `--command-timeout-seconds <seconds>`
- `--duration-seconds <seconds>`
- `--local-snapshot-seconds <seconds>`
- `--full-snapshot-seconds <seconds>`

Default behavior:

- live run duration: `3600` seconds
- local-only status snapshots every `300` seconds
- full status/SLO/tick snapshots every `900` seconds
- drain poll cadence: every `30` seconds
- maximum graceful drain window: `900` seconds
- auxiliary command timeout: `300` seconds

`--command-timeout-seconds` applies only to auxiliary commands such as:

- preflight readiness checks
- baseline snapshots
- timeline snapshots
- final diagnostics

It does **not** apply to the supervised consumer child process.

## Artifact Layout

Artifacts are written under:

`~/.local/share/startupai/test-runs/<UTC timestamp>/`

Expected contents:

- `run_meta.json`
- `summary.json`
- `summary.md`
- `artifacts/backups/state-root/`
- `artifacts/baseline/`
- `artifacts/timeline/`
- `artifacts/final/`
- `artifacts/logs/consumer-run.log`

`run_meta.json` records:

- git commit
- hostname
- state/config paths
- child command line
- the operator's procedural single-consumer confirmation

## What the Harness Captures

### Baseline and Final

- `board_consumer status --json`
- `board_consumer status --json --local-only`
- `board_consumer report-slo --json`
- `board_consumer report-slo --json --local-only`
- `board_control_plane tick --json --dry-run`
- `board_consumer one-shot --dry-run --json`

Final diagnostics also include:

- `board_consumer reconcile --dry-run`

### Timeline

Every 5 minutes:

- `board_consumer status --json --local-only`

Every 15 minutes:

- `board_consumer status --json`
- `board_consumer report-slo --json`
- `board_control_plane tick --json --dry-run`

Immediate extra snapshot bundle when first observed:

- `degraded=true`
- `claim_suppressed_until` becomes non-empty
- the consumer exits unexpectedly

If an auxiliary snapshot command times out:

- the timeout is recorded in artifact metadata and the final summary
- the harness continues where safe
- a timeout is evidence for review, not automatically a harness failure

If a preflight/readiness command times out:

- the run aborts before the consumer child starts

## Shutdown Behavior

At the one-hour mark the harness runs:

```bash
uv run python -m startupai_controller.board_consumer drain
```

Then it polls local status every 30 seconds and waits for the consumer to stop
on its own.

If the consumer does not exit within the drain window:

1. send `SIGINT`
2. wait 60 seconds
3. send `SIGTERM` if still alive

The harness does **not** call `resume`. The controller stays drained and
stopped after the test.

## Report Interpretation

The summary separates:

- **consumer-loop evidence**
  - degraded periods and reasons
  - claim-suppression windows and reasons
  - rate-limit and mutation timestamps
  - SLO output
  - transport-related log highlights from `consumer-run.log`
- **control-plane proxy evidence**
  - `github_request_counts`
  - control-plane health transitions

Important caveat:

`github_request_counts` come from periodic
`board_control_plane tick --json --dry-run` snapshots. They are a proxy for
GitHub activity visibility, not a direct measurement of every live
consumer-loop request.

The report ends with exactly one conclusion:

- `current transport acceptable for live use`
- `current transport acceptable but needs deeper instrumentation`
- `current transport needs a narrow GitHub MCP comparison spike before broader use`
- `test inconclusive due to insufficient board activity`

## After the Run

1. Confirm the controller is still stopped and drained.
2. Review `summary.md`, `summary.json`, and `consumer-run.log`.
3. Compare baseline vs final status and SLO payloads.
4. Treat `one-shot --dry-run --json` with `exit_class=idle` as valid evidence rather
   than a harness failure.
5. For short smoke runs, forced shutdown is only acceptable when the remaining
   active workers are explicitly classified as `finishing_inflight_execution`.
6. Decide whether the next step is:
   - no transport change
   - deeper transport instrumentation
   - a narrow GitHub MCP comparison spike
   - another burn-in during a busier workload window
