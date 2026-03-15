# Consumer Recovery Runbook

Procedures for stopping, restarting, and recovering the board consumer daemon.

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

## Common Issues

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `gh: command not found` | gh CLI not installed | `sudo apt install gh` or equivalent |
| GitHub API 401 | Token expired | Re-authenticate: `gh auth login` |
| SQLite locked | Stale lock file or zombie process | Kill zombie, delete `.db-journal` |
| Drain file present | Intentional drain | Check why, then `rm ~/.local/share/startupai/drain` |
