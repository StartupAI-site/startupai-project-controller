# StartupAI Project Board Controller

GitHub Project board automation for the
[StartupAI Alpha → Launch](https://github.com/orgs/StartupAI-site/projects/1)
project board. Manages issue lifecycle, agent dispatch, review gates, and
operational health across three repos (crew, app, site).

Extracted from `startupai-crew/scripts/` —
see [`docs/adr/001-architecture.md`](docs/adr/001-architecture.md).

## Public CLI Surface

```bash
# Board automation — used by GitHub Actions workflows in startupai-crew
uv run python -m startupai_controller.board_automation <subcommand>

# Board consumer — long-running daemon for continuous board management
uv run python -m startupai_controller.board_consumer <subcommand>

# Control plane — overnight sweep / recovery
uv run python -m startupai_controller.board_control_plane <subcommand>
```

## Quick Start

```bash
uv sync --group dev          # Install dependencies
uv run pytest tests/ -v      # Run tests
uv run black --check .       # Check formatting
uv run mypy src/startupai_controller/domain/ src/startupai_controller/ports/
```

## Architecture

```
src/startupai_controller/
├── board_automation.py        # CLI/parser entry shell (18 subcommands)
├── board_consumer.py          # Daemon entry shell (8 subcommands)
├── board_control_plane.py     # Sync/recovery entry shell (tick, run)
├── application/               # Use-case coordination
│   ├── automation/            # Automation use cases
│   ├── consumer/              # Consumer use cases
│   └── control_plane/         # Control plane use cases
├── domain/                    # Pure policy (no I/O, no shims)
├── ports/                     # Typed protocol boundaries
├── adapters/                  # GitHub, SQLite, worktree, transport
└── runtime/                   # Wiring and dependency assembly
```

Three entry shells delegate to application use cases. Policy lives in `domain/`.
External mechanism lives in `adapters/`. `ports/` define the boundaries between them.

See [`CLAUDE.md`](CLAUDE.md) for full architecture details, coding standards, and
maintenance rules.

## Relationship to startupai-crew

The 16 GitHub Actions board-automation workflows live in `startupai-crew`.
Each workflow checks out this repo and runs controller commands via
`uv sync && uv run python -m startupai_controller.<module> <subcmd>`.

This repo has **no dependency on CrewAI, Modal, or Supabase** — pure Python + PyYAML.

## License

Proprietary — StartupAI, Inc.
