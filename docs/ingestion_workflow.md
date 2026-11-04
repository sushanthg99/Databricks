# Ingestion Workflow — Dynamic Databricks Ingestion Framework

## Overview

The ingestion workflow follows the **medallion architecture**: Bronze (raw) → Silver (cleansed) → Gold (business-ready).
Each layer is triggered as a separate Databricks job task and reads its input from the previous layer's Delta table.
All behaviour is driven by the metadata tables registered in Unity Catalog.

---

## Phase 0: Configuration Registration (One-Time / On Change)

This phase runs whenever a new dataset is added or an existing configuration changes.
It does **not** need to run on every ingestion cycle.

```
Step 0.1 — Engineer edits YAML config
    configs/domain_config.yml
    └── Add or modify a dataset_config block

Step 0.2 — metadata_loader.py reads the YAML
    └── Parses all dataset_config entries

Step 0.3 — SHA-256 hash computed per dataset config
    └── If hash matches existing row in ingestion_config → SKIP
    └── If hash differs or row missing → proceed

Step 0.4 — MERGE INTO Unity Catalog metadata tables
    ├── dataset_registry      ← Register dataset identity
    ├── dataset_tags          ← Apply classification, PII, criticality tags
    ├── ingestion_config      ← Store ingestion parameters (S3 path, format, mode, etc.)
    └── schema_registry       ← Register initial schema version (if schema provided)
```

**Key module:** `ingestion_framework/metadata_service/metadata_loader.py`

---

## Phase 1: Bronze Ingestion

Raw data from S3 is written to Delta Bronze tables without transformation.

```
Step 1.1 — Job triggered (manual, scheduled, or event-based)
    Parameters: source_system, environment

Step 1.2 — Query metadata tables
    SELECT * FROM ingestion_config ic
    JOIN dataset_registry dr ON ic.dataset_id = dr.dataset_id
    WHERE dr.source_system = :source_system
      AND dr.active = true
      AND dr.data_tier = 'bronze'

Step 1.3 — For each active dataset:

    Step 1.3.1 — Check load_type
        ├── 'incremental' → use Auto Loader (cloudFiles)
        └── 'full'        → use batch overwrite

    Step 1.3.2 [Incremental — Auto Loader path]
        autoloader_engine.py:
        ├── Configure cloudFiles source:
        │   ├── format       = ic.format (csv, json, parquet)
        │   ├── source path  = ic.s3_bucket / ic.s3_path
        │   ├── schema loc   = /Volumes/{catalog}/metadata/schemas/{dataset_name}/
        │   └── checkpoint   = /Volumes/{catalog}/metadata/checkpoints/{dataset_name}/
        ├── Apply reader options (delimiter, encoding, header, etc.)
        ├── Set schema evolution mode (addNewColumns, rescue)
        └── Write stream to Delta:
            ├── target table = {catalog}.{domain}_bronze.{dataset_name}
            ├── mode         = append
            ├── trigger      = AvailableNow (process all backlog, then stop)
            └── add metadata columns:
                ├── _source_file        (input_file_name())
                ├── _ingestion_date     (current_date())
                └── _ingestion_timestamp (current_timestamp())

    Step 1.3.3 [Full load — Batch overwrite path]
        autoloader_engine.py:
        ├── spark.read with format and options
        ├── Read from s3_bucket / s3_path
        └── Write to Delta:
            ├── mode = overwrite
            └── replaceWhere or full overwrite (config-driven)

    Step 1.3.4 — schema_registry.py
        ├── Detect current schema from written table
        ├── Compare with latest registered schema version
        └── If changed → insert new schema version row into schema_registry

    Step 1.3.5 — Update ingestion_runtime_state
        ├── dataset_id
        ├── run_id (job run ID)
        ├── start_time / end_time
        ├── status: SUCCESS / FAILED
        ├── rows_ingested
        └── error_message (if failed)
```

**Key modules:**
- `src/bronze/bronze_ingestion.py` — orchestration loop
- `src/bronze/bronze_loader.py` — delegates to engine, writes Bronze
- `ingestion_framework/ingestion_engine/autoloader_engine.py` — core reader/writer
- `ingestion_framework/ingestion_engine/schema_registry.py` — schema versioning

---

## Phase 2: Silver Transformation

Bronze data is cleansed, validated, and written to Silver Delta tables.

```
Step 2.1 — Read Bronze Delta table
    spark.read.table("{catalog}.{domain}_bronze.{dataset_name}")
    └── Optionally filter by _ingestion_date (watermark-based processing)

Step 2.2 — Apply Silver transformations (silver_transformations.py)
    ├── Type casting (string → integer, timestamp, decimal, etc.)
    ├── Null handling (drop, fill, or flag based on rules)
    ├── Deduplication (by primary_keys + watermark from ingestion_config)
    ├── String normalisation (trim, lower, etc.)
    └── Derived column computation

Step 2.3 — Data Quality evaluation (data_quality.py)
    ├── Load DQ rules from dq_sla_config for this dataset
    ├── Evaluate each rule (not_null, unique, range, regex, referential)
    ├── Compute pass/fail counts per rule
    └── Write DQ results back to dq_sla_config or a DQ results table

Step 2.4 — Handle DQ failures
    ├── Configurable threshold: fail_on_dq_error (true/false per config)
    ├── Below threshold → quarantine failed rows to _quarantine table
    └── Above threshold → abort Silver write, mark run as FAILED

Step 2.5 — Write to Silver Delta
    ├── MERGE INTO {catalog}.{domain}_silver.{dataset_name}
    ├── Match on primary_keys from ingestion_config
    └── Update changed rows, insert new rows

Step 2.6 — Update ingestion_runtime_state (Silver run)
```

**Key modules:**
- `src/silver/silver_transformations.py`
- `src/silver/data_quality.py`

---

## Phase 3: Gold Transformation

Silver data is aggregated and joined to produce business-ready Gold tables.

```
Step 3.1 — Resolve dataset dependencies
    SELECT * FROM dataset_dependencies
    WHERE downstream_dataset_id = :gold_dataset_id
    └── Ensures all upstream Silver datasets are available

Step 3.2 — Read Silver tables (one or more)
    └── Joins defined in gold_transformations.py or via config

Step 3.3 — Apply Gold transformations (gold_transformations.py)
    ├── Aggregations (SUM, COUNT, AVG, etc.)
    ├── Business KPI calculations
    ├── Dimensional joins
    └── Final column selection and renaming

Step 3.4 — Write to Gold Delta
    ├── MERGE INTO {catalog}.{domain}_gold.{dataset_name}
    └── Or overwrite (for full aggregation rebuilds)

Step 3.5 — Update ingestion_runtime_state (Gold run)
```

**Key modules:**
- `src/gold/gold_transformations.py`

---

## Phase 4: API Ingestion (Parallel Path)

REST API sources bypass S3 and write directly to Bronze.

```
Step 4.1 — Load API config from api_config.yml
    ├── endpoint URL
    ├── authentication method (Bearer token, API key, OAuth)
    ├── pagination strategy (cursor, offset, page number)
    └── response schema

Step 4.2 — api_fetcher.py
    ├── Authenticate using Databricks secret scope (no hardcoded keys)
    ├── Fetch paginated responses
    ├── Parse JSON response into Spark DataFrame
    └── Write to Bronze Delta (same table as file-based, with source tag)

Step 4.3 — Continue to Silver / Gold as normal
```

**Key module:** `src/api/api_fetcher.py`

---

## Error Handling Strategy

| Scenario | Behaviour |
|---|---|
| One dataset fails in the loop | Log error, update `ingestion_runtime_state` to FAILED, continue other datasets |
| Auto Loader schema mismatch | Rescue unexpected columns into `_rescued_data` column |
| DQ failure above threshold | Abort Silver write for that dataset; quarantine rows |
| Missing source file | Log warning, skip dataset, update runtime state |
| Metadata table unreachable | Fail entire job — metadata is required for all operations |
| S3 access denied | Fail dataset, log full traceback, continue loop |

---

## Execution Modes

| Mode | Trigger | Use Case |
|---|---|---|
| `incremental` | Scheduled / event | Daily or hourly Auto Loader runs |
| `full` | Manual / scheduled | Initial load or full refresh |
| `backfill` | Manual | Re-process a date range (TODO — Phase 2) |
| `dry_run` | Manual | Validate config without writing data (TODO — Phase 2) |

---

## Orchestration Flow (Databricks Job)

```
bronze_ingestion_job
├── Task 1: run_metadata_loader
│   └── notebook: notebooks/bronze_ingestion_notebook.py
│       └── dbutils.widgets: source_system, environment, run_mode
├── Task 2: run_bronze_ingestion
│   └── depends_on: run_metadata_loader
│   └── notebook: notebooks/bronze_ingestion_notebook.py
└── (Future) Task 3: run_silver_transformation
    └── depends_on: run_bronze_ingestion
```
