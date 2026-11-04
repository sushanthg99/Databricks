# Project Structure — Dynamic Databricks Ingestion Framework

## Annotated Folder Tree

```
dynamic-databricks-ingestion/
│
├── .kiro/                                  ← Kiro IDE configuration (not deployed)
│   ├── steering/
│   │   ├── product.md                      ← Business vision, objectives, and personas
│   │   ├── tech.md                         ← Technology stack, coding standards, security rules
│   │   └── structure.md                    ← Folder map, naming conventions, structural rules
│   └── specs/                              ← Kiro spec definitions (requirements → tasks)
│
├── .gitignore                              ← Excludes secrets, venvs, cache, Databricks local state
├── README.md                               ← Project entry point: overview, setup, usage
├── requirements.txt                        ← Pinned Python dependencies
├── databricks.yml                          ← Databricks Asset Bundle root config (dev/uat/prod)
│
├── docs/                                   ← Human-readable documentation (Markdown)
│   ├── architecture.md                     ← System design with Mermaid diagram
│   ├── project_structure.md                ← This file: annotated folder tree
│   ├── ingestion_workflow.md               ← Step-by-step Bronze → Silver → Gold flow
│   ├── metadata_design.md                  ← Metadata table schemas and volume design
│   ├── deployment_guide.md                 ← DAB deploy instructions per environment
│   └── open_questions.md                   ← Unresolved design questions and assumptions
│
├── configs/                                ← YAML-based dataset and environment configuration
│   ├── domain_config.yml                   ← Example dataset config (illustrative, not production)
│   ├── api_config.yml                      ← Example REST API source config
│   └── environments/
│       ├── dev.yml                         ← Dev environment variable overrides
│       ├── uat.yml                         ← UAT environment variable overrides
│       └── prod.yml                        ← Production environment variable overrides
│
├── ingestion_framework/                    ← Reusable core engine — no business logic
│   ├── __init__.py
│   ├── ingestion_engine/
│   │   ├── __init__.py
│   │   ├── autoloader_engine.py            ← Auto Loader + batch file reader (format-aware)
│   │   └── schema_registry.py              ← Schema versioning and evolution tracking
│   └── metadata_service/
│       ├── __init__.py
│       └── metadata_loader.py              ← YAML → metadata table MERGE with hash deduplication
│
├── src/                                    ← Layer-specific orchestration (calls ingestion_framework)
│   ├── __init__.py
│   ├── bronze/
│   │   ├── __init__.py
│   │   ├── bronze_ingestion.py             ← Entry point: params → filter datasets → run loop
│   │   ├── bronze_loader.py                ← Delegates to autoloader_engine; writes Bronze Delta
│   │   └── data_extraction.py             ← S3 file discovery, path helpers, extraction utilities
│   ├── silver/
│   │   ├── __init__.py
│   │   ├── silver_transformations.py       ← Cleansing, deduplication, type casting
│   │   └── data_quality.py                 ← DQ rule evaluation and result persistence
│   ├── gold/
│   │   ├── __init__.py
│   │   └── gold_transformations.py         ← Aggregations, joins, business-level transforms
│   └── api/
│       ├── __init__.py
│       └── api_fetcher.py                  ← REST API ingestion: pagination, auth, retry
│
├── sql/
│   └── metadata_tables.sql                 ← DDL for all 7 Unity Catalog metadata tables
│
├── scripts/                                ← Operational scripts (not part of ingestion pipeline)
│   ├── auth/
│   │   └── README.md                       ← IAM, service principal, secret scope setup
│   ├── transformations/
│   │   └── README.md                       ← Reusable SQL/Python transformation snippets
│   ├── silver/
│   │   └── README.md                       ← Silver-specific operational scripts
│   ├── gold/
│   │   └── README.md                       ← Gold-specific operational scripts
│   └── unity_catalog/
│       └── README.md                       ← Catalog/schema/volume provisioning scripts
│
├── resources/
│   └── databricks_job.yml                  ← DAB job: Bronze ingestion job resource definition
│
├── tests/
│   ├── __init__.py
│   ├── unit/                               ← Pure Python unit tests (no cluster needed)
│   ├── integration/                        ← Tests requiring a live Databricks environment
│   └── test_metadata_loader.py             ← Starter unit tests for metadata_loader.py
│
└── notebooks/                              ← Thin Databricks notebook entry points
    ├── bronze_ingestion_notebook.py        ← Calls src/bronze/bronze_ingestion.py
    ├── silver_transformation_notebook.py   ← Calls src/silver/silver_transformations.py
    └── gold_transformation_notebook.py     ← Calls src/gold/gold_transformations.py
```

---

## File Classification

### Files with Starter Content (non-trivial)

These files have meaningful code, configuration, or documentation already in place:

| File | Content Type |
|---|---|
| `README.md` | Full project overview, setup guide, folder description |
| `databricks.yml` | DAB bundle config with all three targets |
| `requirements.txt` | Pinned dependencies with rationale comments |
| `.gitignore` | Comprehensive exclusion rules |
| `.kiro/steering/product.md` | Business objectives, personas, success criteria |
| `.kiro/steering/tech.md` | Full tech stack, coding standards, security rules |
| `.kiro/steering/structure.md` | Folder responsibilities and naming conventions |
| `docs/architecture.md` | Mermaid diagram + component descriptions |
| `docs/project_structure.md` | This file |
| `docs/ingestion_workflow.md` | Step-by-step ingestion flow |
| `docs/metadata_design.md` | Metadata table purpose, known columns, open questions |
| `docs/deployment_guide.md` | DAB deploy instructions |
| `docs/open_questions.md` | Assumptions and unresolved decisions |
| `configs/domain_config.yml` | Illustrative dataset configuration |
| `configs/api_config.yml` | Illustrative API configuration |
| `configs/environments/*.yml` | Environment-specific variable stubs |
| `sql/metadata_tables.sql` | Metadata table DDL with TODO markers |
| `resources/databricks_job.yml` | Bronze job resource YAML |
| `ingestion_framework/**/*.py` | Module docstrings + function signatures + TODOs |
| `src/**/*.py` | Module docstrings + function signatures + TODOs |
| `notebooks/*.py` | Notebook structure with widget definitions |
| `tests/test_metadata_loader.py` | Starter test structure |

### Placeholder Files (structure only, content to be added)

| File | Reason |
|---|---|
| `tests/unit/` | Directory only — tests written per module in Phase 2 |
| `tests/integration/` | Directory only — requires live cluster |
| `scripts/*/README.md` | Topic headings only — scripts added per need |

---

## Dependency Graph

```
notebooks/              ← calls
  └── src/              ← calls
        └── ingestion_framework/    ← calls
              ├── PySpark / Databricks APIs
              ├── Unity Catalog (SQL)
              └── AWS S3 (via UC External Location)
```

**Rule:** `ingestion_framework/` must never import from `src/`. Dependency flows one way only.

---

## Module Mapping to Developer Guide Concepts

> Note: No developer guide was found in the workspace at project creation time.
> This mapping is based on the task specification. Update this table once the guide is available.

| Guide Concept | Implemented In |
|---|---|
| Metadata-driven configuration | `ingestion_framework/metadata_service/metadata_loader.py` |
| Auto Loader ingestion | `ingestion_framework/ingestion_engine/autoloader_engine.py` |
| Schema Registry | `ingestion_framework/ingestion_engine/schema_registry.py` |
| Bronze ingestion orchestration | `src/bronze/bronze_ingestion.py` + `bronze_loader.py` |
| Silver transformations | `src/silver/silver_transformations.py` |
| Data Quality | `src/silver/data_quality.py` |
| Gold aggregations | `src/gold/gold_transformations.py` |
| API ingestion | `src/api/api_fetcher.py` |
| Metadata DDL | `sql/metadata_tables.sql` |
| DAB deployment | `databricks.yml` + `resources/databricks_job.yml` |
