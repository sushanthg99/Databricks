# Open Questions & Assumptions — Dynamic Databricks Ingestion Framework

## Context

This document tracks unresolved design questions, implementation gaps, and assumptions
made during the initial project scaffolding. All items here should be reviewed and
resolved before implementation of the corresponding component begins.

**Last updated:** Project initialisation (Phase 1 scaffold)
**Developer guide status:** Not found in workspace at time of scaffolding. Structure
was built from the task specification prompt. This document should be updated once
the guide is reviewed.

---

## Open Questions

### OQ-001 — Dataset ID Generation Strategy

**Component:** `dataset_registry`, `metadata_loader.py`
**Question:** How is `dataset_id` generated?
- Option A: Framework generates a UUID (deterministic, based on `dataset_name + source_system`)
- Option B: Framework generates a random UUID on first insert
- Option C: `dataset_id` is provided externally in the YAML config

**Impact:** Affects the MERGE key in `metadata_loader.py` and whether configs are portable across environments.
**Assumption made:** Option A (deterministic UUID from `dataset_name + source_system` hash) to ensure idempotent re-registration across environments.

---

### OQ-002 — Dataset Tags Structure

**Component:** `dataset_tags`, `metadata_loader.py`
**Question:** Are tags stored as flat key-value rows (one row per tag), or as a MAP column on `dataset_registry`?
**Impact:** Affects DDL in `sql/metadata_tables.sql` and how tags are loaded from YAML.
**Assumption made:** Flat key-value rows in a separate `dataset_tags` table — more flexible for querying.

---

### OQ-003 — Dataset Dependency Resolution

**Component:** `dataset_dependencies`, `gold_transformations.py`
**Question:** Is dependency resolution handled:
- A. In-framework (Gold job checks `dataset_dependencies` before running), or
- B. By Databricks Workflows task dependencies (job-level)?
**Impact:** If B, `dataset_dependencies` may only be informational metadata, not a runtime gate.
**Assumption made:** B (Databricks Workflows handles execution order); `dataset_dependencies` is informational for now.

---

### OQ-004 — Full Set of Auto Loader Reader Options

**Component:** `ingestion_config`, `autoloader_engine.py`
**Question:** Which reader options must be configurable per dataset?
Known options: `delimiter`, `encoding`, `header`, `multiline`, `dateFormat`, `timestampFormat`.
Are there others (e.g., `escape`, `quote`, `nullValue`, `nanValue`, `inferSchema`)?
**Impact:** Affects the `ingestion_config` DDL and the `autoloader_engine.py` option mapping.
**Assumption made:** Implemented the most common options; others can be added to the options dict without schema changes if stored as a `MAP<STRING,STRING>` column. See `ingestion_config` DDL — TODO marker added.

---

### OQ-005 — `ingestion_runtime_state` Write Mode

**Component:** `ingestion_runtime_state`, `bronze_ingestion.py`
**Question:** Is `ingestion_runtime_state` append-only (one row per run attempt) or does it
use MERGE to update an existing row for the same `run_id + dataset_id`?
**Impact:** Affects whether the write at end-of-run is INSERT or MERGE.
**Assumption made:** Append-only. Each run attempt creates a new row. Simplest model; supports full history.

---

### OQ-006 — DQ Results Storage: In-place Update vs History Table

**Component:** `dq_sla_config`, `data_quality.py`
**Question:** Does the DQ engine:
- A. Update the existing `dq_sla_config` row in-place (`last_run_pass_pct`, `last_run_at`)
- B. Write to a separate `dq_results` history table (one row per rule per run)
- C. Both (update in-place AND append to history table)
**Impact:** If B or C, a `dq_results` table needs to be added to `sql/metadata_tables.sql`.
**Assumption made:** C — update `dq_sla_config` for latest status AND append to a separate `dq_results` table. The `dq_results` table is marked as TODO in the SQL file.

---

### OQ-007 — Schema Registry Format

**Component:** `schema_registry`, `schema_registry.py`
**Question:** Is `schema_json` stored as:
- A. Spark `StructType.json()` format (native Spark schema JSON)
- B. A simplified custom format (column name + type pairs)
- C. DDL string (e.g., `col1 STRING, col2 INT`)
**Impact:** Affects how `schema_registry.py` serialises and deserialises schemas.
**Assumption made:** Option A (Spark `StructType.json()`) — most compatible with Auto Loader schema evolution.

---

### OQ-008 — Full Load Overwrite Strategy

**Component:** `autoloader_engine.py`, `bronze_loader.py`
**Question:** For full-load datasets, does the framework:
- A. Overwrite the entire table (`mode=overwrite`)
- B. Use `replaceWhere` to overwrite only a partition (e.g., current date)
- C. Truncate then insert
**Impact:** Affects data retention and whether historical partitions are preserved.
**Assumption made:** Option A for initial implementation. `replaceWhere` support noted as TODO.

---

### OQ-009 — Multi-Environment Catalog Strategy

**Component:** `databricks.yml`, `ingestion_config`
**Question:** Is the catalog name per environment (`dev_catalog`, `uat_catalog`, `prod_catalog`)
or is it a single catalog with environment as a schema prefix (e.g., `shared_catalog.dev_bronze`)?
**Impact:** Affects the `target_table` column format and DAB variable configuration.
**Assumption made:** Separate catalog per environment — cleaner isolation and access control.

---

### OQ-010 — API Ingestion Authentication Methods

**Component:** `api_config.yml`, `api_fetcher.py`
**Question:** What authentication methods must be supported for REST API sources?
- Bearer token
- API key (header or query param)
- OAuth 2.0 client credentials
- Basic auth
**Impact:** Affects `api_fetcher.py` design and what secrets are required.
**Assumption made:** Bearer token and API key (header) for Phase 1. OAuth 2.0 marked as TODO.

---

### OQ-011 — Backfill / Reprocessing Strategy

**Component:** `bronze_ingestion.py`, `autoloader_engine.py`
**Question:** How does the framework handle backfill (re-processing a historical date range)?
Does it:
- A. Delete the checkpoint and re-run (risky — processes all files)
- B. Use a dedicated `backfill` run mode with date range parameters
- C. Use batch reader with a date-filtered source path
**Impact:** Needs explicit design before implementation.
**Assumption made:** Deferred to Phase 2. Marked as TODO in `bronze_ingestion.py`.

---

### OQ-012 — PII Column Handling

**Component:** `dataset_registry`, `dataset_tags`, Silver layer
**Question:** When `pii = true` in `dataset_registry`, what action is taken?
- A. Columns are tagged in Unity Catalog (metadata only)
- B. Columns are masked in Silver (dynamic views or column masks)
- C. Columns are encrypted at Bronze and decrypted for authorised users
**Impact:** Significant — affects Silver transformation design.
**Assumption made:** Option A only (tagging) for Phase 1. Masking marked as Phase 5.

---

### OQ-013 — Developer Guide Location

**Question:** The task specified "read the developer guide available in the workspace."
No developer guide was found in the workspace directory or Downloads folder at the time of
scaffolding. The PDFs found (`Databricks - Generic.pdf`) are binary-compressed and their
content could not be extracted as text.

**Action required:** Please provide the developer guide by:
- Dropping it into the `dynamic-databricks-ingestion/` folder, or
- Sharing it as a text/markdown file, or
- Pasting the relevant sections into chat.

Once the guide is available, all open questions above should be reviewed against it, and
this document should be updated with confirmed answers.

---

## Resolved Assumptions

| ID | Assumption | Decision |
|---|---|---|
| A-001 | Project builds from task spec, not a guide | Confirmed: no guide found |
| A-002 | SHA-256 hash deduplication for metadata | Proceeding |
| A-003 | Unity Catalog Volumes for checkpoints/schemas | Proceeding |
| A-004 | Separate catalog per environment | Proceeding |
| A-005 | Append-only runtime state table | Proceeding |
| A-006 | Databricks Workflows handles job dependencies | Proceeding |
