# CLAUDE.md - StartupAI Project Board Controller

## Quick Reference
- **Purpose**: GitHub Project board automation across 3 repos (crew, app, site)
- **Framework**: Pure Python (stdlib + PyYAML)
- **Deployment**: systemd user unit on DELL (intentionally powered down)
- **Origin**: Extracted from `startupai-crew/scripts/` — see `docs/adr/001-architecture.md`

## Architecture

The controller manages the [StartupAI Alpha → Launch](https://github.com/orgs/StartupAI-site/projects/1) project board. It provides:

1. **Board Automation** (`board_automation.py`) — CLI/parser shell plus port-factory seam. Operational ready/review/execution/state/admission behavior now delegates through `automation_*_wiring.py`, `automation_cli_handlers.py`, and `application/automation/`.
2. **Board Consumer** (`board_consumer.py`) — Daemon entry shell over `application/consumer/` plus focused `consumer_*_wiring.py` bridge modules. It no longer imports `board_automation.py`.
3. **Control Plane** (`board_control_plane.py`) — Sync/recovery shell around `application/control_plane/`.
4. **Supporting layers** — `application/`, `domain/`, `ports/`, `adapters/`, `runtime/`, plus remaining compatibility shells such as `board_io.py`, `consumer_db.py`, and GitHub-facing compatibility modules.

Runtime wiring is assembled through `src/startupai_controller/runtime/wiring.py`.
Coordinators should depend on `domain/`, `ports/`, and runtime wiring only.
Compatibility shims (`board_io.py`, `consumer_db.py`, `github_http.py`) still
exist for legacy callers and tests, but runtime orchestrators do not import
them directly.

## Architectural Guardrails

This repo is a control plane. Reliability matters more than cleverness.

- Refactors are behavior-preserving by default. Do not change queue semantics, retry policy, board transitions, launch behavior, review behavior, or status semantics unless the task explicitly requires it.
- `domain/` is pure policy. No GitHub, SQLite, subprocess, env/config-loading, or shim imports.
- `application/` owns use-case coordination. It may depend on `domain/` and `ports/`, and should not grow adapter/runtime/entrypoint imports.
- `ports/` define typed boundaries only where the domain needs isolation from external mechanisms.
- `adapters/` own GitHub, SQLite, worktree, subprocess, and transport mechanics.
- `board_consumer.py`, `board_automation.py`, and `board_control_plane.py` are entry shells. They should delegate to application use cases, not accumulate new policy.
- New internal code must import from canonical paths (`domain/`, `ports/`, `adapters/`). Do not add new internal dependencies on `board_io.py`, `consumer_db.py`, or `github_http.py` except where explicitly documented as transitional.
- Do not bypass a port by calling an adapter directly from orchestration. If the port is missing capability, extend the port.
- Do not create new mega-modules. If you touch a large coordinator, prefer extracting one focused policy module instead of adding more branches.
- Before moving behavior, add characterization tests for the current behavior. Pure policy modules need direct unit tests.
- Prefer names that describe controller concepts: review, rescue, launch, repair, escalation, admission. Avoid `helpers`, `misc`, or generic wrappers that hide responsibility.
- Leave the code cleaner than you found it: each change should improve boundaries, not just relocate complexity.

## Directory Structure
```
src/startupai_controller/   # 126 Python modules across shells/application/domain/ports/adapters/runtime
config/                      # Config and schema files
├── board-automation-config.json
├── critical-paths.json
├── project-field-sync-config.json
└── codex_session_result.schema.json
tests/                       # 28 test files (854+ tests)
docs/adr/                    # Architecture decisions
docs/runbooks/               # Operational runbooks
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

## Maintenance Constitution

Five principles that govern all changes to this repo:

1. **Shells delegate** — `board_automation.py`, `board_consumer.py`, and `board_control_plane.py` are entry shells. They parse CLI args and delegate to `application/` use cases. They must not accumulate new policy.
2. **Policy lives in domain/** — Business rules, decision logic, and state transitions belong in `domain/`. No I/O, no shims, no adapter imports.
3. **Application uses ports** — `application/` coordinates use cases through `ports/` protocols. It must not import adapters, runtime, entrypoints, or shims directly.
4. **Adapters own mechanism** — GitHub API calls, SQLite queries, subprocess invocations, and transport details live in `adapters/`. They implement port protocols.
5. **Shims shrink monotonically** — `board_io.py`, `consumer_db.py`, and `github_http.py` are compatibility shims. They may not grow. When touched, they must shrink. No new callers may be added.

## Hard Rules

- **No new shim imports**: Code outside the shim files themselves must not add new `import` statements referencing `board_io`, `consumer_db`, or `github_http`. The frozen allowlist in `tests/test_architecture_boundaries.py` enforces this.
- **No service-locator patterns**: Do not use `sys.modules[__name__]`, `_shell_module()`, or similar dynamic dispatch to locate dependencies. Use explicit port injection.
- **No adapter bypass**: Application and domain code must not call adapters directly. If a port is missing capability, extend the port protocol.
- **No new mega-modules**: If touching a large coordinator, extract focused policy modules instead of adding branches.

## CLI Public API Contract

The three entry modules are the public surface of this repo:

| Module | Purpose |
|--------|---------|
| `startupai_controller.board_automation` | CLI for GitHub Actions workflows (18 subcommands) |
| `startupai_controller.board_consumer` | Daemon for continuous board management (8 subcommands) |
| `startupai_controller.board_control_plane` | Overnight sweep / recovery (tick, run) |

Breaking changes to subcommand names, arguments, or exit codes require coordinated
updates to the 16 GitHub Actions workflows in `startupai-crew`.

## No Self-Dispatch Policy

This repo has no `WORKFLOW.md` and does not self-dispatch via the board consumer.
The consumer manages work in `startupai-crew`, `app.startupai.site`, and
`startupai.site` — not in this repo. This is a deliberate architectural constraint,
not a gap. Controller changes are made via normal PRs and CI, not board automation.

## Agent Ownership

| Change Type | Owning Agent | Notes |
|-------------|--------------|-------|
| Domain policy | `system-architect` | Pure logic in `domain/` |
| Port protocol changes | `system-architect` | Typed boundaries |
| Adapter implementation | `platform-engineer` | GitHub, SQLite, transport |
| Application use cases | `backend-developer` | Coordination logic |
| Entry shell changes | `platform-engineer` | CLI parsing, wiring |
| CI/workflow changes | `platform-engineer` | `.github/workflows/` |
| Architecture decisions | `system-architect` | `docs/adr/` |
| Operational runbooks | `platform-engineer` | `docs/runbooks/` |
| Boundary tests | `qa-engineer` | `tests/test_architecture_boundaries.py` |

## Compatibility Shim Policy

Three compatibility shims exist as transitional artifacts from the extraction:

| Shim | Lines | Status |
|------|-------|--------|
| `board_io.py` | 1463 | Transitional — shrink on touch |
| `consumer_db.py` | 1309 | Transitional — shrink on touch |
| `github_http.py` | 1659 | Transitional — shrink on touch |

**Rules:**
- Shims may not grow in line count (ratchet test enforces this)
- No new callers may import shims (frozen allowlist enforces this)
- When a PR touches a shim, it must reduce its size or leave it unchanged
- Target: eventual elimination once all callers migrate to ports/adapters

---
**Last Updated**: 2026-03-12
