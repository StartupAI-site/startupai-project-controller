# CLAUDE.md - StartupAI Project Board Controller

## Quick Reference
- **Purpose**: GitHub Project board automation across 3 repos (crew, app, site)
- **Framework**: Pure Python (stdlib + PyYAML)
- **Deployment**: systemd user unit on DELL (intentionally powered down)
- **Origin**: Extracted from `startupai-crew/scripts/` — see `docs/adr/001-control-plane-extraction.md`

## Architecture

The controller manages the [StartupAI Alpha → Launch](https://github.com/orgs/StartupAI-site/projects/1) project board. It provides:

1. **Board Automation** (`board_automation.py`) — 16 subcommands for workflow-driven board mutations (mark-done, auto-promote, enforce-ready-dependencies, etc.)
2. **Board Consumer** (`board_consumer.py`) — Daemon that polls the board, claims `Ready` issues, dispatches codex sessions, and manages lifecycle
3. **Control Plane** (`board_control_plane.py`) — Sync/recovery operations (tick, field sync)
4. **Supporting modules** — board_io, board_graph, consumer_db, consumer_workflow, github_http, promote_ready, validate_critical_path_promotion, project_field_sync, resolution_proof, gh_cli_timeout

## Architectural Guardrails

This repo is a control plane. Reliability matters more than cleverness.

- Refactors are behavior-preserving by default. Do not change queue semantics, retry policy, board transitions, launch behavior, review behavior, or status semantics unless the task explicitly requires it.
- `domain/` is pure policy. No GitHub, SQLite, subprocess, env/config-loading, or shim imports.
- `ports/` define typed boundaries only where the domain needs isolation from external mechanisms.
- `adapters/` own GitHub, SQLite, worktree, subprocess, and transport mechanics.
- `board_consumer.py`, `board_automation.py`, and `board_control_plane.py` are application coordinators. They should orchestrate use cases, not accumulate new policy.
- New internal code must import from canonical paths (`domain/`, `ports/`, `adapters/`). Do not add new internal dependencies on `board_io.py`, `consumer_db.py`, or `github_http.py` except where explicitly documented as transitional.
- Do not bypass a port by calling an adapter directly from orchestration. If the port is missing capability, extend the port.
- Do not create new mega-modules. If you touch a large coordinator, prefer extracting one focused policy module instead of adding more branches.
- Before moving behavior, add characterization tests for the current behavior. Pure policy modules need direct unit tests.
- Prefer names that describe controller concepts: review, rescue, launch, repair, escalation, admission. Avoid `helpers`, `misc`, or generic wrappers that hide responsibility.
- Leave the code cleaner than you found it: each change should improve boundaries, not just relocate complexity.

## Directory Structure
```
src/startupai_controller/   # 13 Python modules
config/                      # Config and schema files
├── board-automation-config.json
├── critical-paths.json
├── project-field-sync-config.json
└── codex_session_result.schema.json
tests/                       # 11 test files (357 tests)
docs/adr/                    # Architecture decisions
systemd/                     # systemd user unit
```

## Commands
```bash
uv sync --group dev                                    # Install dependencies
uv run pytest tests/ -v                                # Run tests

# Board automation (used by GitHub Actions workflows in startupai-crew)
uv run python -m startupai_controller.board_automation --help
uv run python -m startupai_controller.board_automation mark-done --repo crew --pr 123

# Board consumer (daemon)
uv run python -m startupai_controller.board_consumer status
uv run python -m startupai_controller.board_consumer status --json --local-only
uv run python -m startupai_controller.board_consumer one-shot --dry-run
uv run python -m startupai_controller.board_consumer one-shot --issue crew#84
uv run python -m startupai_controller.board_consumer run

# Control plane
uv run python -m startupai_controller.board_control_plane tick --json

# Field sync
uv run python -m startupai_controller.project_field_sync sync-all --dry-run
uv run python -m startupai_controller.project_field_sync audit-completeness --strict
```

## Config Loading

Config files live in `config/` at repo root. Default paths are resolved at import time via:
```python
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_CONFIG_PATH = str(_REPO_ROOT / "config" / "critical-paths.json")
```

All config paths can be overridden via CLI arguments (`--file`, `--automation-config`, `--sync-config`).

## Relationship to startupai-crew

- **Workflows stay in startupai-crew**: 16 GitHub Actions workflows are triggered by events in that repo. Each checks out this repo and runs controller code.
- **This repo has no dependency on CrewAI/Modal/Supabase**: Pure Python + PyYAML.
- **Bridge pattern**: Workflows in crew do `actions/checkout` of this repo, then `uv sync && uv run python -m startupai_controller.<module> <subcmd>`.

## External Dependencies (Runtime)

| Dependency | Path | Purpose |
|------------|------|---------|
| Secrets | `~/.secrets/startupai` | Environment/credentials |
| Worktree script | `~/.claude/skills/worktree/scripts/wt-create.sh` | 1 reference in board_consumer.py |
| Repo main checkouts | `~/projects/{crew,app,site}/` | WORKFLOW.md loading |
| Worktree dirs | `~/projects/worktrees/` | Working directories for codex sessions |
| Runtime state | `~/.local/share/startupai/` | SQLite DB, outputs, drain file, workflow state |
| Binaries | `gh`, `git`, `systemctl`, `codex` | Required CLI tools |

`~/.codex/config.toml` is NOT read by controller code — the codex binary uses its own config internally.

## Service Lifecycle
```bash
# Install
cp systemd/startupai-consumer.service ~/.config/systemd/user/
systemctl --user daemon-reload

# Start/Stop
systemctl --user start startupai-consumer
systemctl --user stop startupai-consumer

# Logs
journalctl --user -u startupai-consumer -f

# Status
systemctl --user status startupai-consumer
```

**Note**: Consumer is intentionally powered down. Do NOT start without explicit instruction.

## Coding Standards
- Python 3.10+, type hints required
- Google-style docstrings
- Black formatter (88 char)
- snake_case naming
- stdlib-only + PyYAML (no heavy deps)

---
**Last Updated**: 2026-03-09
