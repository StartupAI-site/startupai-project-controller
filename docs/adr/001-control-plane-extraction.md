# ADR-001: Control-Plane Extraction from startupai-crew

**Status**: Accepted
**Date**: 2026-03-09

## Context

The local board consumer/control-plane code (~19k lines of Python across 13 modules) lived in `startupai-crew` but had **zero coupling** to the CrewAI/Modal application code. It managed GitHub Project board automation across 3 repos (crew, app, site) and was a separate system co-located for historical convenience.

## Decision

Extract all control-plane code into `StartupAI-site/startupai-project-controller`:
- 13 Python modules → `src/startupai_controller/`
- 11 test files → `tests/`
- 4 config/schema files → `config/`
- systemd unit → `systemd/`

GitHub Actions workflows remain in `startupai-crew` (triggered by events in that repo) but are repointed to checkout and run code from this repo.

## Rationale

1. **Clean separation of concerns** — board automation vs AI validation engine
2. **Independent CI** — no heavy crewai/supabase deps for control-plane tests
3. **Lighter startupai-crew** — focused on its actual purpose
4. **Explicit dependency boundaries** — instead of implicit co-location

## Consequences

- Workflows in startupai-crew checkout this repo via `actions/checkout` (public repo, no token needed)
- ~30s overhead per workflow run for checkout + `uv sync`
- Consumer systemd unit references new repo paths
- `startupai-crew` ADR-018 (local board consumer) is historical; ADR-019 records the extraction

## What Moved

| Component | From (startupai-crew) | To (this repo) |
|-----------|----------------------|----------------|
| 13 Python modules | `scripts/board_*.py`, `scripts/consumer_*.py`, etc. | `src/startupai_controller/` |
| 11 test files | `tests/contracts/test_board_*.py`, etc. | `tests/` |
| Config files | `docs/master-architecture/reference/*.json` | `config/` |
| Schema | `scripts/schemas/codex_session_result.schema.json` | `config/` |
| systemd unit | `systemd/startupai-consumer.service` | `systemd/` |

## What Remains in startupai-crew

- 16 GitHub Actions workflows (repointed to this repo)
- 7 self-contained workflows (tag-contract, diagnostic gates/locks)
- All CrewAI/Modal application code
- ADR-018 (historical) and ADR-019 (extraction record)
- `systemd/startupai-consumer.service` stub pointing to this repo
