# Structure Steering — Dynamic Databricks Ingestion Framework

## Project Root
```
dynamic-databricks-ingestion/
```

## Directory Responsibilities

### `.kiro/`
Kiro IDE configuration. Contains steering files (always-included context for the AI assistant)
and spec definitions. Do not store business logic or application code here.

```
.kiro/
├── steering/           ← Always-included context: product goals, tech standards, structure rules
│   ├── product.md      ← Business vision, objectives, personas, success criteria
│   ├── tech.md         ← Technology choices, coding standards, security requirements
│   └── structure.md    ← This file: folder map and responsibility rules
└── specs/              ← Kiro spec definitions (requirements → design → tasks)
```

### `docs/`
Human-readable documentation. Written in Markdown. All architecture decisions, design rationale,
workflow descriptions, and deployment procedures live here.

```
docs/
├── architecture.md         ← System architecture with Mermaid diagram
├── project_structure.md    ← Annotated folder tree with file responsibilities
├── ingestion_workflow.md   ← Step-by-step Bronze → Silver → Gold flow
├── metadata_design.md      ← Metadata table designs and Unity Catalog volume usage
├── deployment_guide.md     ← How to deploy with Databricks Asset Bundles
└── open_questions.md       ← Unresolved design questions and assumptions
```

### `configs/`
All YAML-based configuration. Separated by concern and environment. No Spark code here.

```
configs/
├── domain_config.yml       ← Dataset-level ingestion configuration (illustrative example)
├── api_config.yml          ← API source configuration (illustrative example)
└── environments/
    ├── dev.yml             ← Development environment overrides
    ├── uat.yml             ← UAT environment overrides
    └── prod.yml            ← Production environment overrides
```

### `ingestion_framework/`
The **reusable core engine**. These modules are used by all layers (Bronze, Silver, Gold).
No business logic specific to a single dataset or domain belongs here.

```
ingestion_framework/
├── __init__.py
├── ingestion_engine/
│   ├── __init__.py
│   ├── autoloader_engine.py    ← Auto Loader + batch reader; format-aware file ingestion
│   └── schema_registry.py      ← Schema versioning, registration, and evolution tracking
└── metadata_service/
    ├── __init__.py
    └── metadata_loader.py      ← YAML → Unity Catalog metadata table MERGE; hash-based deduplication
```

### `src/`
Layer-specific orchestration logic. Calls `ingestion_framework/` modules but adds
layer-level concerns (DQ, transformation rules, aggregation).

```
src/
├── __init__.py
├── bronze/
│   ├── __init__.py
│   ├── bronze_ingestion.py     ← Entry point: reads job params, filters datasets, runs ingestion loop
│   ├── bronze_loader.py        ← Delegates to autoloader_engine; handles write to Delta Bronze
│   └── data_extraction.py      ← Source file discovery and extraction helpers
├── silver/
│   ├── __init__.py
│   ├── silver_transformations.py   ← Cleansing, deduplication, type casting
│   └── data_quality.py             ← DQ rule evaluation and result persistence
├── gold/
│   ├── __init__.py
│   └── gold_transformations.py     ← Business aggregations and joins
└── api/
    ├── __init__.py
    └── api_fetcher.py              ← REST API ingestion; pagination; auth handling
```

### `sql/`
DDL and DML SQL scripts. Primarily for metadata table creation.

```
sql/
└── metadata_tables.sql     ← CREATE TABLE statements for all 7 metadata tables
```

### `scripts/`
Operational scripts organised by concern. Each subdirectory has a README explaining
what scripts belong there and how to use them.

```
scripts/
├── auth/               ← Authentication helpers (IAM, service principals, secret scope setup)
├── transformations/    ← Reusable transformation SQL or Python snippets
├── silver/             ← Silver-layer specific operational scripts
├── gold/               ← Gold-layer specific operational scripts
└── unity_catalog/      ← UC catalog/schema/volume provisioning scripts
```

### `resources/`
Databricks Asset Bundle resource definitions. Job and cluster configurations in YAML.

```
resources/
└── databricks_job.yml  ← DAB job resource: Bronze ingestion job definition
```

### `tests/`
All automated tests. Unit tests mock external dependencies; integration tests require
a live Databricks environment and are tagged separately.

```
tests/
├── __init__.py
├── unit/               ← Pure Python unit tests (no Spark session required)
├── integration/        ← Databricks-connected tests (require cluster)
└── test_metadata_loader.py   ← Starter test for metadata_loader module
```

### `notebooks/`
Databricks notebook entry points (`.py` files in `# COMMAND ----------` format).
These are thin wrappers that import from `src/` and accept widget parameters.

```
notebooks/
├── bronze_ingestion_notebook.py        ← Calls src/bronze/bronze_ingestion.py
├── silver_transformation_notebook.py   ← Calls src/silver/silver_transformations.py
└── gold_transformation_notebook.py     ← Calls src/gold/gold_transformations.py
```

## Naming Conventions

| Item | Convention | Example |
|---|---|---|
| Python files | snake_case | `autoloader_engine.py` |
| Python classes | PascalCase | `AutoLoaderEngine` |
| Python functions | snake_case | `load_metadata_config()` |
| Python constants | UPPER_SNAKE | `DEFAULT_CHECKPOINT_PATH` |
| YAML config keys | snake_case | `dataset_name`, `load_type` |
| Delta table names | snake_case | `orders_raw`, `customers_cleansed` |
| Unity Catalog schemas | `{domain}_{tier}` | `ecommerce_bronze` |
| Databricks job names | kebab-case | `bronze-ingestion-job` |

## Rules

1. **Never** put credentials, tokens, or passwords in any file in this project.
2. **Never** import from `src/` into `ingestion_framework/` — the framework is the base layer.
3. **Always** add a module-level docstring to every Python file.
4. **Always** update `docs/open_questions.md` when a design decision is deferred.
5. Environment-specific values belong in `configs/environments/`, not in Python code.
6. Notebooks are entry points only — all logic belongs in `src/` or `ingestion_framework/`.
