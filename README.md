# Dynamic Databricks Ingestion Framework

A **metadata-driven, reusable data ingestion framework** built on Databricks, Delta Lake,
AWS S3, and Unity Catalog. New datasets are onboarded by editing a YAML configuration file —
no Spark code changes required.

---

## Business Objective

Modern data platforms accumulate dozens of source systems and hundreds of datasets. Writing a
custom ingestion pipeline for each dataset is slow, error-prone, and hard to maintain. This
framework solves that by centralising all ingestion configuration in metadata tables and
providing a single, parameterised ingestion engine that handles any source.

Key outcomes:
- Reduce dataset onboarding time from days to hours.
- Enforce consistent data quality and governance across all ingestion paths.
- Enable safe, reproducible deployments across dev / UAT / prod environments.

---

## Architecture Overview

```
Source Systems (CSV, JSON, Parquet, Delta, REST APIs)
        │
        ▼
   AWS S3 Buckets  ──────────────────────────────────────┐
        │                                                 │
        ▼                                                 │
 Metadata Service                                         │
 (YAML → Unity Catalog metadata tables)                  │
        │                                                 │
        ▼                                                 │
 Ingestion Engine                                         │
 (Auto Loader / Batch Reader)                             │
        │                                                 │
        ▼                                                 ▼
 ┌─────────────┐    ┌─────────────────┐    ┌────────────────────┐
 │   Bronze    │───▶│     Silver      │───▶│       Gold         │
 │ (Raw Delta) │    │ (Cleansed/DQ'd) │    │ (Business-Ready)   │
 └─────────────┘    └─────────────────┘    └────────────────────┘
        │                   │
        └──────┬────────────┘
               ▼
        Unity Catalog
    (Governance + Lineage)
```

See [`docs/architecture.md`](docs/architecture.md) for the full Mermaid diagram and component
descriptions.

---

## Technology Stack

| Component | Technology |
|---|---|
| Processing | Apache Spark (PySpark) via Databricks |
| Platform | Databricks (DBR 13.x LTS+) |
| Storage Format | Delta Lake |
| Source Storage | AWS S3 |
| Governance | Unity Catalog |
| Incremental Ingestion | Databricks Auto Loader |
| Orchestration | Databricks Workflows |
| Deployment | Databricks Asset Bundles (DAB) |
| Language | Python 3.10+ |
| Configuration | YAML |

---

## Folder Structure

```
dynamic-databricks-ingestion/
├── .kiro/                          ← Kiro IDE steering & specs
├── docs/                           ← Architecture, design, and operational docs
├── configs/                        ← YAML configuration per domain and environment
├── ingestion_framework/            ← Reusable core engine (Auto Loader, schema registry, metadata)
├── src/                            ← Layer-specific orchestration (Bronze / Silver / Gold / API)
├── sql/                            ← Metadata table DDL
├── scripts/                        ← Operational scripts (auth, UC setup, etc.)
├── resources/                      ← Databricks Asset Bundle job definitions
├── tests/                          ← Unit and integration tests
├── notebooks/                      ← Databricks notebook entry points
├── databricks.yml                  ← DAB bundle configuration
├── requirements.txt                ← Python dependencies
└── .gitignore                      ← Excludes secrets, venvs, cache
```

---

## Supported Source Formats

| Format | Ingestion Mode | Notes |
|---|---|---|
| CSV | Auto Loader / Batch | Configurable delimiter, encoding |
| JSON | Auto Loader / Batch | Single-line and multi-line |
| Parquet | Auto Loader / Batch | Schema inferred or registered |
| Delta | Batch | Full or incremental merge |
| REST API | HTTP pull | Pagination, auth via secrets |

---

## Medallion Architecture

### Bronze Layer — Raw Ingestion
Exact copy of source data written to Delta tables. No transformations applied.
Schema is inferred or enforced from the schema registry. Supports full overwrite
and incremental Auto Loader modes.

### Silver Layer — Cleansed & Validated
Data is type-cast, deduplicated, null-checked, and validated against DQ rules
defined in `dq_sla_config`. Results are written to Delta Silver tables.

### Gold Layer — Business-Ready
Aggregations, joins, and business-level transformations applied. Serves as the
source for BI tools, dashboards, and downstream ML pipelines.

---

## Initial Setup

### Prerequisites
- Python 3.10+
- Databricks CLI v0.18+ with a configured profile
- AWS credentials configured via IAM role (not access keys)
- Unity Catalog enabled on your Databricks workspace

### 1. Clone the repository
```bash
git clone <repository-url>
cd dynamic-databricks-ingestion
```

### 2. Create a virtual environment
```bash
python -m venv .venv
source .venv/bin/activate      # macOS/Linux
.venv\Scripts\activate         # Windows
pip install -r requirements.txt
```

### 3. Configure your environment
```bash
# Copy and fill in the environment-specific config
cp configs/environments/dev.yml configs/environments/dev.local.yml
# Edit dev.local.yml with your catalog name, S3 paths, etc.
# dev.local.yml is gitignored — safe for local values
```

### 4. Create metadata tables
```sql
-- Run in Databricks SQL or a notebook:
-- sql/metadata_tables.sql
```

### 5. Load dataset configuration
```bash
# Run the metadata loader notebook or job to register your YAML configs
# into the Unity Catalog metadata tables
```

### 6. Deploy with Databricks Asset Bundles
```bash
# Validate the bundle
databricks bundle validate

# Deploy to dev
databricks bundle deploy --target dev

# Run the Bronze ingestion job
databricks bundle run bronze_ingestion_job --target dev
```

---

## Future Implementation Phases

| Phase | Scope |
|---|---|
| **Phase 1** (current) | Project structure, metadata tables, Bronze ingestion (CSV + JSON) |
| **Phase 2** | Silver transformations, DQ rule engine, schema registry |
| **Phase 3** | Gold aggregations, dataset dependency graph |
| **Phase 4** | REST API ingestion, SLA monitoring, alerting |
| **Phase 5** | Column-level lineage, PII masking, data classification enforcement |
| **Phase 6** | CI/CD pipeline, automated testing against Databricks Connect |

---

## Key Documents

- [Project Flow & Ingestion Framework](docs/project_flow_and_ingestion_framework.md)
- [Architecture](docs/architecture.md)
- [Metadata Design](docs/metadata_design.md)
- [Ingestion Workflow](docs/ingestion_workflow.md)
- [Deployment Guide](docs/deployment_guide.md)
- [Open Questions](docs/open_questions.md)
- [ABC Planning (TM1) P&L Pipeline + Security](docs/tm1_pipeline.md)

---

## Contributing

1. Create a feature branch: `git checkout -b feature/your-feature-name`
2. Follow the coding standards in [`.kiro/steering/tech.md`](.kiro/steering/tech.md)
3. Add unit tests for any new logic in `ingestion_framework/`
4. Run `pytest tests/unit/` before opening a pull request
5. Never commit secrets, credentials, or tokens

---

## License

Internal use only. See your organisation's data engineering platform team for access and
contribution guidelines.
