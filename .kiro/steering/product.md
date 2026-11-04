# Product Steering — Dynamic Databricks Ingestion Framework

## Product Vision

Build a **metadata-driven, reusable ingestion framework** on Databricks that enables scalable,
governed, and observable data ingestion from multiple source systems into a Delta Lake-based
medallion architecture (Bronze → Silver → Gold).

## Business Objectives

- Eliminate per-dataset custom ingestion pipelines by centralising configuration in metadata tables.
- Accelerate onboarding of new data sources through YAML-driven configuration — no code changes required.
- Enforce data governance via Unity Catalog, column-level lineage, and data quality checks.
- Support both incremental (Auto Loader) and full-load (batch overwrite) ingestion patterns.
- Provide end-to-end observability through runtime state tracking and SLA monitoring.

## Target Users

| Persona | Need |
|---|---|
| Data Engineers | Onboard new datasets without writing ingestion code |
| Data Architects | Define and evolve schemas through schema registry |
| Data Stewards | Monitor ingestion health, DQ failures, and SLA breaches |
| Platform Engineers | Deploy and manage the framework using Databricks Asset Bundles |

## Core Capabilities

1. **Metadata-driven configuration** — all ingestion behaviour is controlled by metadata tables, not hardcoded logic.
2. **Auto Loader integration** — incremental file ingestion with schema inference, evolution, and checkpointing.
3. **Schema Registry** — versioned schema tracking in Unity Catalog volumes.
4. **Medallion architecture** — Bronze (raw), Silver (cleansed/validated), Gold (aggregated/business-ready).
5. **Data Quality** — configurable DQ rules per dataset with pass/fail tracking.
6. **Environment promotion** — dev → UAT → prod deployment via Databricks Asset Bundles.
7. **API ingestion** — REST API source support alongside file-based ingestion.

## Non-Goals (Current Phase)

- Real-time streaming beyond Auto Loader micro-batch.
- Cross-cloud source support (Azure, GCP) — AWS S3 only in this phase.
- Self-service UI for metadata management.
- ML feature store integration.

## Success Criteria

- A new dataset can be onboarded by editing a YAML config and running the metadata loader — no Spark code changes.
- Bronze ingestion runs end-to-end for at least one CSV and one JSON dataset.
- Schema changes are detected and logged in the schema registry.
- DQ rules execute and results are persisted to the DQ metadata table.
- All environments (dev/UAT/prod) deployable via `databricks bundle deploy`.
