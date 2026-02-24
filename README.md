# QDArchive Seeding

Config-driven ETL framework and TUI to seed QDArchive from open or authenticated data sources.

## Quickstart

- Install deps (uv): `uv sync --dev`
- Run tests: `uv run pytest`
- Lint: `uv run ruff check .`

## Structure

- `src/qdarchive_seeding/`: package code
- `configs/`: YAML configs (examples in `configs/examples/`)
- `runs/`: run manifests
- `metadata/`: sqlite db
- `downloads/`: downloaded files
- `logs/`: log files

See `project-plan.md` for the full architecture and roadmap.
