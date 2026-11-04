# Technical Steering — Dynamic Databricks Ingestion Framework

## Technology Stack

| Layer | Technology | Version / Notes |
|---|---|---|
| Processing Engine | Apache Spark (PySpark) | Via Databricks Runtime |
| Platform | Databricks | DBR 13.x LTS or higher |
| Storage Format | Delta Lake | Unity Catalog managed tables |
| Source Storage | AWS S3 | IAM role-based access (no hardcoded keys) |
| Governance | Unity Catalog | 3-level namespace: catalog.schema.table |
| Orchestration | Databricks Workflows / Jobs | Defined via DAB resource YAMLs |
| Deployment | Databricks Asset Bundles (DAB) | `databricks.yml` + `resources/` |
| Language | Python 3.10+ | PEP 8, type hints required |
| Configuration | YAML | PyYAML for loading |
| Testing | pytest | Unit tests for all reusable modules |
| Secret Management | Databricks Secrets / AWS Secrets Manager | Never hardcode credentials |
| Logging | Python `logging` module | Structured logs, avoid `print()` |

## Architecture Principles

### 1. Metadata-Driven Design
All ingestion behaviour (source path, format, schema, DQ rules, load type) is stored in
Unity Catalog metadata tables, not in code. Code reads configuration at runtime.

### 2. Idempotency
- Full loads use `overwrite` mode — safe to re-run.
- Incremental loads use Auto Loader with checkpoints — no duplicate processing.
- Metadata MERGE operations use SHA-256 hash comparison to avoid unnecessary updates.

### 3. Modular Layering
```
ingestion_framework/     ← reusable engine components (Auto Loader, schema registry, metadata service)
src/                     ← layer-specific orchestration (bronze, silver, gold)
notebooks/               ← Databricks notebook entry points
configs/                 ← YAML-based configuration per domain/environment
```

### 4. Unity Catalog Namespace Convention
```
{catalog}.{domain}_bronze.{dataset_name}     ← raw ingested data
{catalog}.{domain}_silver.{dataset_name}     ← cleansed, validated data
{catalog}.{domain}_gold.{dataset_name}       ← aggregated, business-ready data
{catalog}.metadata.{metadata_table}          ← framework metadata tables
```

### 5. Volumes for Schema & Checkpoint Storage
```
/Volumes/{catalog}/metadata/schemas/         ← Auto Loader inferred schemas (JSON)
/Volumes/{catalog}/metadata/checkpoints/     ← Auto Loader streaming checkpoints
```

## Coding Standards

### Python
- All public functions must have type hints and docstrings.
- Use `logging.getLogger(__name__)` — never `print()`.
- Raise specific exceptions with meaningful messages.
- Use `dataclasses` or typed dicts for structured config objects where beneficial.
- Avoid mutable default arguments.
- Maximum function length: 50 lines. Refactor beyond that.

### PySpark
- Use `spark.read` / `spark.readStream` — not RDDs.
- Prefer DataFrame API over SQL strings for programmatic use.
- Always specify `mergeSchema` and `cloudFiles.schemaLocation` for Auto Loader.
- Use `MERGE INTO` (Delta) for upsert operations.
- Avoid `collect()` on large DataFrames.
- Partition Delta tables by a meaningful column (e.g., ingestion date).

### Configuration
- All environment-specific values go in `configs/environments/{env}.yml`.
- Never commit secrets, tokens, passwords, or cloud credentials to Git.
- Use `${env_var}` syntax for dynamic values in YAML where supported.

### Error Handling
- Wrap ingestion loops in try/except — a single dataset failure must not abort the entire job.
- Log full tracebacks at ERROR level.
- Update `ingestion_runtime_state` with FAILED status and error message on failure.

### Testing
- Unit test every function in `ingestion_framework/` that contains business logic.
- Mock Spark session and external dependencies in unit tests.
- Integration tests are separate and require a Databricks environment.

## Security Requirements

- AWS S3 access via IAM instance profile or Unity Catalog external location — no access keys in code.
- Databricks secrets scope for any required credentials.
- PII datasets must be tagged in `dataset_tags` and may require column masking (future).
- `.env` files must never be committed; they are in `.gitignore`.

## CI/CD Approach

- Linting: `flake8` or `ruff`
- Formatting: `black`
- Type checking: `mypy`
- Testing: `pytest`
- Deployment: `databricks bundle deploy --target {env}`
