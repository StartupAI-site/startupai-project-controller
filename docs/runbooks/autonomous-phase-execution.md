# Autonomous Phase Execution

This runbook defines how the hardening program advances from one PR to the next
without human supervision.

## Rule

A green CI poll means it is safe to merge and continue immediately.

## Required sequence per phase

1. Implement the phase on a worktree branch.
2. Run local validation.
3. Open the PR.
4. Immediately enable auto-merge when GitHub allows it.
5. Start a background poller that checks the PR every 120 seconds.
6. Keep a shorter foreground watch while the PR is active.
7. On the first all-green result:
   - merge immediately if the PR is still open
   - or, if auto-merge already merged it, stop waiting and continue
8. Stop the poller after merge.
9. Fetch `origin/main`.
10. Create the next worktree from fresh mainline state.
11. Start the next phase without waiting for user confirmation.

## Anti-stall rules

- The poller is actionable, not informational.
- A green PR may not sit open for another poll cycle.
- If GitHub API calls fail transiently, retry in the foreground until the PR
  state is known.
- A merge-ready PR takes priority over new analysis.

## Allowed pauses

Pause only for:

- a real architectural contradiction
- a compatibility blocker that cannot be resolved from the audited repo set
- repeated CI failure after diagnosis and corrective attempt
- an external decision that cannot be derived from code, docs, or contracts
