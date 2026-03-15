# Consumer Recovery Runbook

Procedures for stopping, restarting, and recovering the board consumer daemon.

## Burn-In Defaults

Use short burns by default:

- Default validation burn: `15 minutes`
- Escalate to `20 minutes` when you need a little more live traffic
- Use `1 hour` only for release-confidence or soak behavior

Recommended short-burn invocation:

```bash
uv run python tools/limited_live_test.py \
  --duration-seconds 900 \
  --confirm-single-consumer \
  --confirmation-note "operator note"
```

Interpretation guidance:

- A short burn is still valid if it produces no claims, as long as it proves
  controller transport, drain timing, and cleanup behavior.
- If a burn ends with `ready_count > 0` but `eligible_count = 0`, treat that as
  board/workflow state, not automatically as a selector bug.
- If a burn ends with `ready_count = 0` and `eligible_count = 0`, the consumer
  may have correctly healed stale board state or simply found no runnable work.

## Pre-Restart Checklist

1. **Verify no other machine is running a consumer.**
   Only one consumer instance should run at a time. The consumer uses a local
   SQLite database and does not have multi-instance coordination.

   ```bash
   # On each machine that might run the consumer:
   systemctl --user status startupai-consumer
   ```

2. **Check drain state.** If the consumer was drained intentionally, understand
   why before resuming.

   ```bash
   uv run python -m startupai_controller.board_consumer status --json --local-only
   ```

## Stop Procedure

```bash
systemctl --user stop startupai-consumer
systemctl --user status startupai-consumer   # Confirm stopped
```

## Backup State

The consumer's operational state lives in `~/.local/share/startupai/`:

```bash
# Backup before any recovery operation
cp -r ~/.local/share/startupai/ ~/.local/share/startupai.bak.$(date +%Y%m%d-%H%M%S)
```

Contents:
- `consumer.db` — SQLite database (session history, claim state, SLO data)
- `drain` — Drain control file (presence = drained)
- `outputs/` — Codex session output artifacts
- `workflow-state/` — Workflow execution state

## Machine Replacement

**The consumer is intentionally powered down. Do NOT start the service without
explicit human instruction.** Only one consumer instance may run at a time —
verify the old machine's consumer is fully stopped before proceeding.

When moving the consumer to a new machine:

1. **Stop the consumer on the old machine** and verify it is not running:
   ```bash
   # On the OLD machine:
   systemctl --user stop startupai-consumer
   systemctl --user status startupai-consumer   # Confirm: inactive (dead)
   ```
2. Install prerequisites: `git`, `gh`, `codex`, `uv`, Python 3.12+
3. Clone the controller repo
4. `uv sync --group dev`
5. Copy `~/.secrets/startupai` from the old machine
6. Install the systemd unit:
   ```bash
   cp systemd/startupai-consumer.service ~/.config/systemd/user/
   systemctl --user daemon-reload
   ```
7. Optionally copy `~/.local/share/startupai/` from the old machine (or let it
   rebuild — see State Recovery below)
8. Run a test one-shot:
   ```bash
   uv run python -m startupai_controller.board_consumer one-shot --dry-run
   ```
9. **Only with explicit human instruction**, start the service:
   ```bash
   systemctl --user start startupai-consumer
   ```

## State Recovery

**The GitHub Project board is the source of truth.** The local SQLite database
is reconstructible operational state — it caches board state and tracks session
history for performance and SLO reporting.

### Interrupted Runtime Cleanup

If the consumer was forced down during drain or stopped unexpectedly, recover
local runtime state before restarting:

```bash
uv run python -m startupai_controller.board_consumer recover-interrupted --json
```

This command is idempotent. A clean no-op is still success and reports
`recovered_leases: 0`.

### Stale `Ready` Items With Surviving Review Ownership

The consumer now reconciles one specific board-truth defect during preflight:

- if an issue is shown as `Ready` on the board
- but local state still proves Review ownership for a surviving PR
- the consumer repairs that item back to `Review`

This matters when the board appears to offer `Ready` work but admission still
reports `eligible_count = 0`.

Check local status:

```bash
uv run python -m startupai_controller.board_consumer status --json --local-only
```

Look at:

- `admission.ready_count`
- `admission.eligible_count`
- `review_queue`
- `review_summary`

If stale `Ready` repair is suspected, also inspect the live project status for
the issue:

```bash
gh issue view 10 --repo StartupAI-site/startupai-crew --json projectItems
```

Expected repaired state is `Review`, not `Ready`.

### Worst Case: Delete and Rebuild

If the local database is corrupted or lost:

```bash
# Stop the consumer
systemctl --user stop startupai-consumer

# Remove the database (board state will be rebuilt from GitHub)
rm ~/.local/share/startupai/consumer.db

# Run reconciliation to rebuild from board truth
uv run python -m startupai_controller.board_consumer reconcile

# Verify state looks correct
uv run python -m startupai_controller.board_consumer status --json --local-only

# Restart
systemctl --user start startupai-consumer
```

The consumer's reconciliation pass re-reads all board state from the GitHub
Project API and rebuilds local tracking state. Historical session data (SLO
metrics, past execution records) will be lost, but operational correctness
is restored.

## Post-Restart Verification

After any restart or recovery:

```bash
# 1. Check the service is running
systemctl --user status startupai-consumer

# 2. Check consumer status
uv run python -m startupai_controller.board_consumer status

# 3. Watch logs for errors
journalctl --user -u startupai-consumer -f --since "5 minutes ago"

# 4. Run a dry-run one-shot to verify it can read the board
uv run python -m startupai_controller.board_consumer one-shot --dry-run
```

Review-lane pressure is also visible in local status:

```bash
uv run python -m startupai_controller.board_consumer status --json --local-only
```

If `review_queue.pressure` is `severe`, treat that as workflow queue health,
not as a transport defect by itself.

## Harness Notes

The limited burn harness uses bounded auxiliary probes before and during a run:

- `board_consumer one-shot --dry-run --json`
- `board_control_plane tick --json --dry-run`
- periodic status snapshots

These probes are intentionally time-limited so they cannot wedge the burn.

Current expectation:

- timed-out auxiliary probes should be reaped cleanly
- the main consumer run should still proceed
- the machine should end with no lingering probe processes

If a summary reports one of these lines:

- `preflight one-shot --dry-run timed out after 30.0 seconds`
- `tick-dry-run timed out after 30.0 seconds`

that is instrumentation noise unless it prevents consumer startup, drain, or
final cleanup.

## Common Issues

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `gh: command not found` | gh CLI not installed | `sudo apt install gh` or equivalent |
| GitHub API 401 | Token expired | Re-authenticate: `gh auth login` |
| SQLite locked | Stale lock file or zombie process | Kill zombie, delete `.db-journal` |
| Drain file present | Intentional drain | Check why, then `rm ~/.local/share/startupai/drain` |
