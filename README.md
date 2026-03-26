# Seeding QDArchive

A **config-driven ETL framework** for discovering, downloading, and cataloguing qualitative research datasets from open data repositories. Built to seed **QDArchive** — a platform for sharing and preserving qualitative data analysis (QDA) projects.

## What Is This Project About?

Qualitative researchers use tools like **MAXQDA**, **NVivo**, **ATLAS.ti**, and **QDA Miner** to analyze interview transcripts, documents, images, and other non-numeric data. Their work produces three categories of files:

| Category | Description | Example Extensions |
|---|---|---|
| **Analysis Data** | QDA project files containing coded annotations and the structured output of qualitative analysis | `.qdpx`, `.mx24`, `.mx22`, `.nvp`, `.nvpx`, `.atlasproj`, `.hpr7`, `.qdc` |
| **Primary Data** | The raw research inputs that were analyzed (interviews, transcripts, documents) | `.pdf`, `.docx`, `.txt`, `.rtf`, `.mp3`, `.mp4`, `.csv`, `.xlsx` |
| **Additional Data** | Supporting files (licenses, readmes, codebooks, supplementary materials) | `.zip`, `.md`, `.ods`, `.xml`, `.json` |

Researchers publish these datasets on open repositories like Zenodo, Syracuse QDR, and others. This framework **automatically discovers these datasets via repository APIs**, downloads all associated files, classifies them by type, and stores structured metadata in a database — making them available for import into QDArchive.

---

## Supported Data Sources

### Datasets by Source

| Source | Datasets | Status |
|---|---|---|
| Zenodo | Multi-query pipeline (extension + NL queries, auto date-splitting) | Active |
| Harvard Dataverse | Multi-query pipeline (146 queries: extension + NL) | Active |
| Syracuse QDR | Multi-query pipeline (extension + NL queries) | Active |
| Uni Hannover (CKAN) | Single dataset with MAXQDA `.mx24` file | Static |

*Note: Exact counts change as pipelines run incrementally. Use `seed status` to see current counts.*

### Asset Classification

| Type | Count | Percentage | Description |
|---|---|---|---|
| Primary Data | 608 | 77.2% | Research inputs (PDFs, transcripts, interviews) |
| Additional Data | 96 | 12.2% | Supporting files (archives, licenses, codebooks) |
| Analysis Data | 84 | 10.7% | QDA project files (the core data we seek) |

### File Extension Breakdown

#### QDA Analysis Files Found (84 total)

| Extension | Tool | Count | % of All Assets |
|---|---|---|---|
| `.nvp` | NVivo (legacy) | 24 | 3.0% |
| `.qdpx` | REFI-QDA Standard | 19 | 2.4% |
| `.nvpx` | NVivo | 9 | 1.1% |
| `.mx24` | MAXQDA 2024 | 6 | 0.8% |
| `.mx22` | MAXQDA 2022 | 5 | 0.6% |
| `.hpr7` | ATLAS.ti 7 | 3 | 0.4% |
| `.qdc` | QDA Miner | 3 | 0.4% |
| `.atlproj` | ATLAS.ti | 2 | 0.3% |

#### Top File Extensions Overall (788 total)

| Extension | Count | Percentage |
|---|---|---|
| `.pdf` | 216 | 27.4% |
| `.docx` | 201 | 25.5% |
| `.rtf` | 90 | 11.4% |
| `.txt` | 50 | 6.3% |
| `.xlsx` | 32 | 4.1% |
| `.nvp` | 24 | 3.0% |
| `.qdpx` | 19 | 2.4% |
| `.tab` | 14 | 1.8% |
| `.csv` | 12 | 1.5% |
| `.nvpx` | 9 | 1.1% |
| `.zip` | 8 | 1.0% |
| Other | 113 | 14.3% |

---

## Data Source Repository Assessment

We investigated multiple qualitative data repositories. Here is the status of each:

| Repository | URL | Status | Notes |
|---|---|---|---|
| **Zenodo** | https://zenodo.org/ | Working | REST API; multi-query with auto date-splitting for large result sets; extension + NL queries |
| **Harvard Dataverse** | https://dataverse.harvard.edu/ | Working | Dataverse API; 146 queries (extension + NL); per-dataset file fetching |
| **Syracuse QDR** | https://qdr.syr.edu/ | Working | Dataverse API; specialized QDA repository; extension + NL queries |
| **Uni Hannover (CKAN)** | https://data.uni-hannover.de/ | Working | CKAN API; 1 dataset with MAXQDA `.mx24` file found |
| **Dryad** | http://datadryad.org/ | Not Useful | Datasets *mention* QDA software in descriptions but contain only CSV/Excel/images — no actual QDA files |
| **DataverseNO** | https://dataverse.no/ | Not Working | API consistently times out (>90 seconds), preventing search completion |
| **DANS** | https://dans.knaw.nl/en/ | Not Working | OAI-PMH protocol provides metadata only; lacks direct file download capability |
| **UK Data Service** | https://ukdataservice.ac.uk/ | Not Working | scraping failed; no actual QDA files |
| **Qualidata Network** | https://www.qualidatanet.com/ | Not Working | no actual QDA files |
| **Qualiservice** | (part of Qualidata Network) | Not Working | no actual QDA files |
| **QualiBi** | (part of Qualidata Network) | Not Working | no actual QDA files |
| Individual uploads | Search engines | Manual | Requires manual Google/Bing searches for QDA files on generic file shares (Drive, Dropbox, etc.) |

### Dataset Search Methodology

Finding QDA project files is challenging because:

1. **Keyword mismatch**: Many datasets *mention* QDA tools (e.g., "analyzed with NVivo") in their descriptions but only contain generic data files (CSV, Excel, PDF). The actual QDA project files are not included.
2. **No standardized tagging**: Repositories don't have a dedicated "qualitative data analysis" file type category.
3. **Proprietary formats**: Each QDA tool has its own format (MAXQDA uses `.mx24`, NVivo uses `.nvp`/`.nvpx`, ATLAS.ti uses `.atlproj`/`.hpr7`), so searches must cover many extensions.

The search strategy combines:
- **Tool-name Boolean searches**: `"MaxQDA OR NVivo OR ATLAS.ti OR QDPX OR qualitative data analysis"`
- **Extension-specific searches**: Searching for specific file extensions (`.qdpx`, `.mx24`, `.nvp`, etc.)
- **Repository-specific APIs**: Each repository has different API capabilities and requires custom extractors

---

## Architecture

```mermaid
flowchart LR
  subgraph Input[Data Sources]
    Z[Zenodo API]
    HD[Harvard Dataverse API]
    Q[Syracuse QDR / Dataverse API]
    C[CKAN / Generic REST API]
    H[HTML pages / static lists]
  end

  subgraph App[Application Orchestration]
    CFG[PipelineConfig<br/>YAML + Pydantic]
    REG[InMemoryRegistry]
    RUN[ETLRunner]
    POL[Policies<br/>incremental / retry]
    PROG[ProgressBus]
  end

  subgraph Infra[Execution Pipeline]
    EXT[Extractors]
    PRE[Pre-Transforms<br/>validate, normalize, deduplicate, slugify]
    DL[Downloader + PathStrategy<br/>atomic write, resume, checksum]
    POST[Post-Transforms<br/>classify_qda_files]
    SINK[Sinks<br/>SQLite / CSV / Excel / MySQL / MongoDB]
  end

  subgraph Output[Persistent Outputs]
    DB[(metadata/*.sqlite)]
    FILES[(downloads/)]
    RUNS[(runs/*.json)]
    LOGS[(logs/)]
  end

  Z --> EXT
  HD --> EXT
  Q --> EXT
  C --> EXT
  H --> EXT

  CFG --> RUN
  REG --> RUN
  POL --> RUN
  PROG --> RUN

  RUN --> EXT --> PRE --> DL --> POST --> SINK
  SINK --> DB
  DL --> FILES
  RUN --> RUNS
  RUN --> LOGS
```

### Clean Architecture Layers

```
core/         Pure domain: entities, protocols, exceptions (no I/O)
  ├── entities.py        DatasetRecord, AssetRecord, RunInfo
  ├── interfaces.py      Protocols: Extractor, Transform, Sink, Downloader, ...
  ├── exceptions.py      QDArchiveError, ConfigError, RegistryError
  └── constants.py       Status codes, run modes, defaults

app/          Orchestration and configuration
  ├── config_models.py   Pydantic v2 models (PipelineConfig + sub-models)
  ├── config_loader.py   YAML → PipelineConfig, config_hash()
  ├── container.py       DI composition root
  ├── runner.py          ETLRunner (the main pipeline loop)
  ├── registry.py        InMemoryRegistry for component lookup
  ├── progress.py        Event bus for pipeline progress
  ├── policies.py        Incremental/retry skip logic
  └── manifests.py       Run manifest writer (JSON per run)

infra/        Concrete implementations
  ├── extractors/        GenericREST, Zenodo, Harvard Dataverse, Syracuse QDR, HTML scraper, static list
  ├── transforms/        validate, normalize, deduplicate, slugify, classify_qda_files
  ├── sinks/             SQLite, MySQL, MongoDB, CSV, Excel
  ├── storage/           Downloader (streaming, atomic, resume, checksum), PathStrategy
  ├── http/              HttpClient (retries/backoff), Auth providers, RateLimiter, Pagination
  └── logging/           Rich console, rotating file, JSON formatter, TUI queue handler

cli/          Typer CLI entry point
  └── commands/seed.py   seed run, validate-config, status, export

Import order (no circular imports):  core  ←  infra  ←  app  ←  cli
```

### Stored Metadata (What We Persist)

The pipeline persists metadata in three durable places:

1. **Dataset + asset metadata** in sink backends (default: SQLite)
2. **Run-level metadata** in run manifests (`runs/<run_id>.json`)
3. **Downloaded file metadata** on disk paths in `downloads/` (plus checksums/status in sink rows)

#### 1) Project metadata (`projects` table in SQLite)

Unique key: `(repository_id, download_project_folder, version)` (idempotent upsert)

| Field | Type | Meaning |
|---|---|---|
| `id` | INTEGER | Auto-increment primary key |
| `query_string` | TEXT | Search query that found this dataset |
| `repository_id` | INTEGER | Source repository identifier (1=Zenodo, 10=Harvard, 20=QDR) |
| `repository_url` | TEXT | Source repository URL |
| `project_url` | TEXT | Dataset landing page URL |
| `version` | TEXT | Dataset version |
| `title` | TEXT | Dataset title |
| `description` | TEXT | Dataset description/abstract |
| `language` | TEXT | Dataset language |
| `doi` | TEXT | DOI if available |
| `upload_date` | TEXT | Publication date |
| `download_date` | TEXT | Download timestamp |
| `download_repository_folder` | TEXT | Source name for local storage |
| `download_project_folder` | TEXT | Dataset identifier for local storage |
| `download_version_folder` | TEXT | Version folder for local storage |
| `download_method` | TEXT | `SCRAPING` or `API-CALL` |

Related tables: `keywords` (project_id, keyword), `person_role` (project_id, name, role), `licenses` (project_id, license)

#### 2) File metadata (`files` table in SQLite)

Unique key: `(project_id, file_name)` (idempotent upsert)

| Field | Type | Meaning |
|---|---|---|
| `id` | INTEGER | Auto-increment primary key |
| `project_id` | INTEGER | Parent project id (FK) |
| `file_name` | TEXT | Filename |
| `file_type` | TEXT | File extension |
| `asset_url` | TEXT | Source download URL |
| `size_bytes` | INTEGER | File size in bytes |
| `status` | TEXT | `UNKNOWN`, `SUCCESS`, `FAILED`, `SKIPPED`, `RESUMABLE` |

#### 3) Run manifest metadata (`runs/*.json`)

Each pipeline run writes one manifest file with:

| Field | Meaning |
|---|---|
| `run_id` | UUID for the run |
| `pipeline_id` | Pipeline identifier from config |
| `started_at`, `ended_at` | Run timing (ISO 8601) |
| `config_hash` | Hash of the effective config used |
| `counts` | Processed datasets/assets counters |
| `failures` | Structured error records |
| `environment` | Runtime environment snapshot |

### Project Structure

```
qdarchive-seeding/
├── pyproject.toml              Project config (uv, ruff, mypy, pytest)
├── README.md                   This file
├── configs/
│   ├── examples/
│   │   ├── zenodo.yaml              Example Zenodo pipeline config
│   │   ├── harvard_dataverse.yaml   Example Harvard Dataverse config
│   │   ├── syracuse_qdr.yaml        Example Syracuse QDR config
│   │   └── auth_api.yaml            Example authenticated API config
│   ├── my_zenodo.yaml               Production Zenodo config
│   ├── harvard_dataverse.yaml       Production Harvard Dataverse config
│   ├── syracuse_qdr.yaml            Production Syracuse QDR config
│   ├── hannover_transens.yaml       Uni Hannover CKAN config
│   └── logging.yaml                 Logging configuration
├── src/qdarchive_seeding/      Source code (see Architecture above)
├── tests/
│   ├── unit/                   17 test modules
│   └── integration/            Pipeline integration tests
├── metadata/                   SQLite databases
├── downloads/                  Downloaded dataset files
├── logs/                       Pipeline run logs
└── runs/                       Run manifest JSONs
```

---

## Getting Started

### Prerequisites

- **Python 3.11+**
- **[uv](https://docs.astral.sh/uv/)** package manager

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd qdarchive-seeding

# Install all dependencies (runtime + dev)
uv sync --dev
```

### Configuration

Everything is driven by YAML config files. Secrets are **never stored in YAML** — only environment variable names are referenced.

```yaml
# configs/examples/zenodo.yaml (simplified)
pipeline:
  id: "zenodo_seed_v2"
  run_mode: "incremental"
  phases: ["metadata", "download"]

source:
  name: "zenodo"
  type: "rest_api"
  base_url: "https://zenodo.org/api"
  repository_id: 1
  repository_url: "https://zenodo.org"
  endpoints:
    search: "/records"
  params:
    size: 100
  search_strategy:
    base_query_prefix: "resource_type.type:dataset AND"
    extension_queries:
      - "qdpx"
      - "nvp"
      - "mx24"
      # ... more QDA file extensions
    natural_language_queries:
      - "interview study"
      - "qualitative research dataset"
      # ... more NL queries

auth:
  type: "bearer"
  env:
    token: "ZENODO_TOKEN"

extractor:
  name: "zenodo_extractor"
  options:
    include_files: true
    auto_date_split: true   # Split large queries by date range

pre_transforms:
  - name: "validate_required_fields"
    options:
      required_fields: ["source_url"]
  - name: "normalize_fields"
  - name: "deduplicate_assets"
  - name: "slugify_dataset"

post_transforms:
  - name: "classify_qda_files"

storage:
  downloads_root: "./downloads"
  layout: "{source_name}/{dataset_slug}/{version}/"
  checksum: "sha256"

sink:
  type: "sqlite"
  options:
    path: "./metadata/qdarchive.sqlite"

http:
  timeout_seconds: 15
  max_retries: 5
  rate_limit_per_second: 0.5
```

### Running a Pipeline

```bash
# Dry run (no downloads, just extract + transform)
uv run python -m qdarchive_seeding.cli.main seed run \
  --config configs/examples/zenodo.yaml --dry-run

# Metadata only (collect metadata, skip downloads)
uv run python -m qdarchive_seeding.cli.main seed run \
  --config configs/my_zenodo.yaml --metadata-only

# Full run with download limit
uv run python -m qdarchive_seeding.cli.main seed run \
  --config configs/examples/zenodo.yaml --max-items 10

# Production runs (each source)
uv run python -m qdarchive_seeding.cli.main seed run --config configs/my_zenodo.yaml
uv run python -m qdarchive_seeding.cli.main seed run --config configs/harvard_dataverse.yaml
uv run python -m qdarchive_seeding.cli.main seed run --config configs/syracuse_qdr.yaml

# Re-extract everything from scratch (clears checkpoint)
uv run python -m qdarchive_seeding.cli.main seed run \
  --config configs/my_zenodo.yaml --fresh-extract

# Re-download all files (ignores prior SUCCESS status)
uv run python -m qdarchive_seeding.cli.main seed run \
  --config configs/my_zenodo.yaml --fresh-download

# Retry only failed downloads
uv run python -m qdarchive_seeding.cli.main seed run \
  --config configs/my_zenodo.yaml --retry-failed

# Validate a config file
uv run python -m qdarchive_seeding.cli.main seed validate-config \
  --config configs/examples/zenodo.yaml

# Check database status
uv run python -m qdarchive_seeding.cli.main seed status

# Export to CSV or Excel
uv run python -m qdarchive_seeding.cli.main seed export --format csv --out export.csv
```

#### CLI Flags Reference

| Flag | Description |
|---|---|
| `--config PATH` | Path to YAML config file (required) |
| `--dry-run` | Extract and transform only, no downloads |
| `--metadata-only` | Collect metadata only, skip download phase |
| `--fresh-extract` | Clear checkpoint, re-extract all queries from scratch |
| `--fresh-download` | Re-download all files, ignoring prior SUCCESS status |
| `--retry-failed` | Retry only previously failed downloads |
| `--max-items N` | Override max items from config |
| `--no-confirm` | Skip the interactive download confirmation prompt |

---

## Development

### Commands

```bash
# Install dependencies
uv sync --dev

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/unit/test_transforms.py

# Lint
uv run ruff check .

# Format check / auto-fix
uv run ruff format --check .
uv run ruff format .

# Type checking
uv run mypy src

# Tests with coverage
uv run pytest --cov=qdarchive_seeding --cov-report=term-missing
```

### Code Conventions

- `from __future__ import annotations` at the top of every module
- `@dataclass(slots=True)` for all dataclasses
- Strict typing; mypy runs in strict mode
- Ruff: line-length 100, rules `E, F, I, B, UP, C4, SIM`
- Python 3.11+
- Secrets read from `os.environ` only — never hardcoded

### Adding a New Data Source

1. **Create an extractor** in `src/qdarchive_seeding/infra/extractors/` implementing the `Extractor` protocol (async `extract(ctx) -> AsyncIterator[DatasetRecord]`)
2. **Add a factory function** in `app/registry.py` and register it in `create_default_registries()`
3. **Create a YAML config** in `configs/` with a unique `pipeline.id` and `source.repository_id`
4. Include checkpoint/resume support, progress bus reporting, and `search_strategy` for consistency with existing extractors
5. Run the pipeline — no core code changes needed

### Test Suite

25 unit test modules + 2 integration tests covering (latest run: 183 passed, 1933 stmts, 0 missed, 100%):

| Module | What It Tests | Coverage |
|---|---|---|
| `test_config_loader.py` | YAML parsing, validation, config hash stability | 100% |
| `test_config_models.py` | Transform migration and config defaults | 100% |
| `test_registry.py` | Component registration, factories, and lookup | 100% |
| `test_auth.py` | NoAuth, ApiKey, Bearer, OAuth2 auth | 100% |
| `test_pagination.py` | Page/offset/cursor pagination | 100% |
| `test_rate_limit.py` | Rate limiter timing and zero-rate behavior | 100% |
| `test_checksums.py` | SHA-256 and "none" checksum paths | 100% |
| `test_filesystem.py` | FileSystem directory, write, exists helpers | 100% |
| `test_http_client.py` | HttpxClient retries, errors, and settings | 100% |
| `test_transforms.py` | All transforms incl. classification + filters | 100% |
| `test_extractors.py` | Generic REST, Zenodo, HTML, Syracuse extractors | 100% |
| `test_downloader.py` | Streaming, resume, checksums, errors | 100% |
| `test_sinks_base.py` | Base sink interface behaviors | 100% |
| `test_sinks_sqlite.py` | SQLite upsert idempotency | 100% |
| `test_sinks_csv.py` | CSV header creation and upserts | 100% |
| `test_sinks_excel.py` | Excel sink read/write and updates | 100% |
| `test_sinks_mysql_mongo.py` | MySQL/MongoDB sink behavior (mocked) | 100% |
| `test_runner.py` | Full pipeline with fake components, dry-run, cancellation | 100% |
| `test_cli.py` | CLI commands via Typer CliRunner | 100% |
| `test_logging.py` | Logger configuration, queue handler, context filter | 100% |
| `test_progress.py` | Publish/subscribe, exception isolation | 100% |
| `test_policies.py` | Incremental/retry skip logic | 100% |
| `test_manifests.py` | Write/load roundtrip, run listing | 100% |
| `test_container.py` | DI container building and dotenv parsing | 100% |
| `test_paths.py` | Path strategy, safe filename generation | 100% |
| `test_pipeline_sqlite.py` | End-to-end: static list → transforms → SQLite | 100% |
| `test_pipeline_csv.py` | End-to-end: static list → transforms → CSV | 100% |

---

## Known QDA File Extensions

The following file extensions are recognized as QDA analysis data by the `classify_qda_files` transform:

| Tool | Extensions |
|---|---|
| **REFI-QDA Standard** | `.qdpx` |
| **MAXQDA** | `.mx24`, `.mx22`, `.mx20`, `.mx18`, `.mx12`, `.mx11`, `.mx5`, `.mx4`, `.mx3`, `.mx2`, `.m2k`, `.mqbac`, `.mqtc`, `.mqex`, `.mqmtr`, `.mex24`, `.mc24`, `.mex22` |
| **NVivo** | `.nvp`, `.nvpx` |
| **ATLAS.ti** | `.atlasproj`, `.atlproj`, `.hpr7` |
| **QDA Miner / Provalis** | `.qdc`, `.qpd` |
| **f4analyse** | `.f4p` |
| **Quirkos** | `.qlt` |
| **Transana** | `.loa`, `.sea`, `.mtr`, `.mod` |
| **Dedoose** | `.ppj`, `.pprj` |
| **MQDA** | `.mqda` |

---

## Implementation Status

| Milestone | Status | Description |
|---|---|---|
| M0 | Done | Bootstrap: repo skeleton, CI, configs |
| M1 | Done | Core domain + config models (entities, interfaces, Pydantic) |
| M2 | Done | Logging module (Rich console, rotating file, UILogQueueHandler) |
| M3 | Done | HTTP infrastructure (retries, auth, rate limiting, pagination) |
| M4 | Done | Extractors (Zenodo, Harvard Dataverse, Syracuse QDR, GenericREST, HTML scraper, static list) |
| M5 | Done | Transform pipeline (chain-of-responsibility, 7 built-in transforms incl. QDA classification) |
| M6 | Done | Storage + async downloader (atomic writes, resume, checksums) |
| M7 | Done | Sinks (SQLite, CSV, Excel; MySQL/MongoDB stubs) |
| M8 | Done | App orchestration: async runner, DI container, checkpoint/resume, progress bus, policies, manifests |
| M9 | Done | CLI (Typer: `seed run`, `validate-config`, `status`, `export` + Rich progress display) |
| M10 | Pending | Complete stub implementations (MySQL, MongoDB, OAuth2) |

---

## Issues and Challenges

### Download Failures

- **Zenodo rate limiting**: Some Zenodo datasets with many files (e.g., "Mapping 'the constructive turn'" with 81 files) experienced 60 failures due to rate limiting or intermittent server errors.
- **Access-restricted files**: A small number of Zenodo records advertise files in metadata but return 403/404 when downloading.
- **Syracuse QDR API file URLs**: QDR serves files via `/api/access/datafile/{id}` — filenames must be extracted from HTTP `Content-Disposition` headers rather than the URL.

### Search Quality

The biggest challenge is **precision vs. recall**: broad keyword searches return many datasets that merely *mention* qualitative research tools without actually containing QDA project files. The `classify_qda_files` post-transform helps by categorizing downloaded files, but filtering at the search level remains difficult without repository support for file-type filtering.

---

## Dependencies

### Runtime
`pydantic>=2.6` | `pyyaml>=6` | `httpx>=0.27` | `tenacity>=8.2` | `rich>=13.7` | `typer[all]>=0.12` | `anyio>=4.3` | `orjson>=3.9` | `beautifulsoup4>=4.12` | `pandas>=2.2` | `openpyxl>=3.1` | `pymysql>=1.1` | `pymongo>=4.7`

### Development
`pytest>=8.1` | `pytest-cov>=5.0` | `ruff>=0.5` | `mypy>=1.10` | `respx>=0.21` | `pytest-asyncio>=0.23`

---

## License

This project is licensed under the MIT License. For details, please refer to the `LICENSE` file in the repository root.
