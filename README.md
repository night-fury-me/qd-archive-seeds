# QDArchive Seeding

Config-driven ETL framework to seed QDArchive from open or authenticated data sources.

## What this project does

The CLI runs a configurable pipeline:

1. Extract dataset records from a source (REST API, HTML, or static list)
2. Apply transform chain(s) to normalize and validate records
3. Download dataset assets
4. Store metadata in a sink (SQLite/MySQL/MongoDB/CSV/Excel)
5. Write run progress and summaries

---

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) for environment and dependency management

Install dependencies:

```bash
uv sync --dev
```

Sanity checks:

```bash
uv run pytest
uv run ruff check .
```

---

## CLI entrypoint

The installed command is:

```bash
qdarchive
```

You can run it from source with `uv` as well:

```bash
uv run qdarchive --help
uv run qdarchive seed --help
```

---

## Pipeline configuration

Pipelines are defined in YAML files under `configs/`.

Examples:

- `configs/examples/zenodo.yaml`
- `configs/examples/auth_api.yaml`

### Minimal required top-level sections

- `pipeline`
- `source`
- `auth`
- `extractor`
- `transforms`
- `storage`
- `sink`
- `logging`

### Typical first run

Copy and adapt an example:

```bash
cp configs/examples/zenodo.yaml configs/my_zenodo.yaml
```

If your source requires credentials, set environment variables referenced by `auth.env`.

Example:

```bash
export EXAMPLE_API_KEY="<your-key>"
```

---

## Run a pipeline from CLI

### 1) Validate config

```bash
uv run qdarchive seed validate-config --config configs/my_zenodo.yaml
```

Expected output:

- `Valid` if schema passes
- `Invalid: ...` with details if not

### 2) Dry run (safe preflight)

Runs extraction/transforms/sink logic without downloading files.

```bash
uv run qdarchive seed run --config configs/my_zenodo.yaml --dry-run
```

### 3) Full run

```bash
uv run qdarchive seed run --config configs/my_zenodo.yaml
```

### Common runtime options

- `--max-items N` override `pipeline.max_items` for this run only
- `--force` ignore incremental skip policy and re-download assets
- `--retry-failed` prefer retrying failed/resumable assets

Example:

```bash
uv run qdarchive seed run \
	--config configs/my_zenodo.yaml \
	--max-items 50 \
	--retry-failed
```

---

## Monitor run outcome

### Check metadata DB status

Default DB path is `./metadata/qdarchive.sqlite`.

```bash
uv run qdarchive seed status
```

Custom DB path:

```bash
uv run qdarchive seed status --db ./metadata/qdarchive.sqlite
```

### Export results

CSV export:

```bash
uv run qdarchive seed export --format csv --out ./exports/run_001
```

Excel export:

```bash
uv run qdarchive seed export --format excel --out ./exports/run_001.xlsx
```

---

## Output locations

Based on your YAML settings, generated artifacts are typically under:

- `downloads/` downloaded files
- `metadata/` SQLite database (if sink is SQLite)
- `logs/` log file(s)

---

## Troubleshooting

### `Config error` or `Invalid`

- Re-run `validate-config`
- Compare with example YAMLs in `configs/examples/`
- Ensure section names and value types match expected schema

### Authentication failures

- Verify required environment variables exist
- Confirm `auth.type`, `placement`, and `header_name` match your API requirements

### Database not found in `seed status` or `seed export`

- Confirm sink type is `sqlite`
- Confirm sink path under `sink.options.path`
- Pass explicit `--db` path when querying status/export

---

## Repository layout

- `src/qdarchive_seeding/` package code
- `configs/` YAML configs
- `tests/` unit/integration tests
- `docs/` architecture and planning docs

See `docs/project-plan.md` for the architecture roadmap.
