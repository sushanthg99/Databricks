-- =============================================================================
-- Metadata Tables DDL — Dynamic Databricks Ingestion Framework
-- =============================================================================
--
-- PURPOSE:
--   Creates all seven Unity Catalog metadata tables that drive the ingestion
--   framework. Run this script against your target environment catalog before
--   deploying or running any ingestion jobs.
--
-- USAGE:
--   Replace {catalog} and {metadata_schema} with your environment values:
--     dev:  dev_catalog.ingestion_metadata
--     uat:  uat_catalog.ingestion_metadata
--     prod: prod_catalog.ingestion_metadata
--
--   Run in Databricks SQL Editor or a bootstrap notebook.
--
-- EXECUTION ORDER:
--   1. dataset_registry     (no foreign key dependencies)
--   2. dataset_tags         (depends on dataset_registry)
--   3. dataset_dependencies (depends on dataset_registry)
--   4. ingestion_config     (depends on dataset_registry)
--   5. ingestion_runtime_state (depends on dataset_registry)
--   6. dq_sla_config        (depends on dataset_registry)
--   7. schema_registry      (depends on dataset_registry)
--
-- NOTE:
--   Unity Catalog Delta tables do not enforce foreign key constraints at
--   the storage level. Referential integrity is maintained by the application.
--   TODO: Add constraint definitions once UC constraint support is confirmed.
--
-- See also: docs/metadata_design.md
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 0. Prerequisites: Catalog and Schema
-- -----------------------------------------------------------------------------

-- Create the catalog if it does not exist (run once per environment)
-- CREATE CATALOG IF NOT EXISTS {catalog};

-- Create the metadata schema
CREATE SCHEMA IF NOT EXISTS {catalog}.{metadata_schema}
  COMMENT 'Ingestion framework metadata tables — do not write directly';


-- =============================================================================
-- TABLE 1: dataset_registry
-- =============================================================================
-- PURPOSE:
--   Master registry of all datasets managed by the framework.
--   Each row represents one logical dataset with its identity,
--   classification, and governance attributes.
--   Primary source of truth for dataset existence and status.
--
-- LOADED BY: ingestion_framework/metadata_service/metadata_loader.py
-- READ BY:   src/bronze/bronze_ingestion.py (active dataset query)
-- =============================================================================

CREATE TABLE IF NOT EXISTS {catalog}.{metadata_schema}.dataset_registry (

  -- Identity
  dataset_id          STRING        NOT NULL  COMMENT 'Deterministic UUID v5: hash(source_system::dataset_name)',
  dataset_name        STRING        NOT NULL  COMMENT 'Logical dataset name (e.g. orders, customers)',
  version             STRING        NOT NULL  COMMENT 'Dataset schema version (e.g. 1.0, 2.1)',
  dataset_type        STRING                  COMMENT 'transactional | reference | event | aggregate',
  data_tier           STRING        NOT NULL  COMMENT 'bronze | silver | gold',
  source_system       STRING        NOT NULL  COMMENT 'Source system identifier (e.g. ecommerce, erp)',

  -- Description
  business_description  STRING                COMMENT 'Human-readable description of the dataset',

  -- Governance
  criticality         STRING                  COMMENT 'high | medium | low',
  classification      STRING                  COMMENT 'public | internal | confidential | restricted',
  pii                 BOOLEAN                 COMMENT 'True if dataset contains personally identifiable information',

  -- Status
  active              BOOLEAN       NOT NULL  COMMENT 'False = excluded from all ingestion runs',

  -- Audit
  created_at          TIMESTAMP     NOT NULL  COMMENT 'Row creation timestamp (UTC)',
  updated_at          TIMESTAMP               COMMENT 'Last update timestamp (UTC)',
  created_by          STRING                  COMMENT 'Service principal or user that created the row'

  -- TODO: Add CONSTRAINT pk_dataset_registry PRIMARY KEY (dataset_id) once UC PK support confirmed
  -- TODO: Add CHECK constraint on data_tier IN ('bronze','silver','gold')
  -- TODO: Add CHECK constraint on criticality IN ('high','medium','low')
)
USING DELTA
COMMENT 'Master registry of all datasets managed by the ingestion framework'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'framework.table' = 'dataset_registry',
  'framework.version' = '1.0'
);


-- =============================================================================
-- TABLE 2: dataset_tags
-- =============================================================================
-- PURPOSE:
--   Flexible key-value tag store for datasets. Supports data classification,
--   ownership, retention policies, and downstream governance enforcement.
--   Tags are loaded from the YAML config and refreshed on each config change.
--
-- LOADED BY: ingestion_framework/metadata_service/metadata_loader.py
-- READ BY:   Data governance tools, DQ monitoring dashboards
-- =============================================================================

CREATE TABLE IF NOT EXISTS {catalog}.{metadata_schema}.dataset_tags (

  dataset_id          STRING        NOT NULL  COMMENT 'FK → dataset_registry.dataset_id',
  tag_key             STRING        NOT NULL  COMMENT 'Tag name (e.g. domain, owner, retention_days, sensitivity)',
  tag_value           STRING                  COMMENT 'Tag value (e.g. ecommerce, data-team, 365)',

  -- Audit
  created_at          TIMESTAMP     NOT NULL  COMMENT 'Row creation timestamp (UTC)'

  -- TODO: Confirm whether a compound PK (dataset_id, tag_key) should be enforced
  -- TODO: Confirm tag schema — flat KV vs nested MAP<STRING,STRING> column on dataset_registry (OQ-002)
)
USING DELTA
COMMENT 'Key-value tags for datasets — used for governance, classification, and discovery'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'framework.table' = 'dataset_tags',
  'framework.version' = '1.0'
);


-- =============================================================================
-- TABLE 3: dataset_dependencies
-- =============================================================================
-- PURPOSE:
--   Captures upstream/downstream relationships between datasets.
--   Used by the Gold layer to verify Silver datasets are ready before
--   running Gold aggregations. Also supports lineage documentation.
--
-- LOADED BY: ingestion_framework/metadata_service/metadata_loader.py
-- READ BY:   src/gold/gold_transformations.py
-- NOTE:      See OQ-003 — dependency resolution may be handled by Databricks
--            Workflows task dependencies rather than in-framework checks.
-- =============================================================================

CREATE TABLE IF NOT EXISTS {catalog}.{metadata_schema}.dataset_dependencies (

  dependency_id           STRING    NOT NULL  COMMENT 'UUID — unique dependency identifier',
  upstream_dataset_id     STRING    NOT NULL  COMMENT 'FK → dataset_registry.dataset_id (source)',
  downstream_dataset_id   STRING    NOT NULL  COMMENT 'FK → dataset_registry.dataset_id (target)',
  dependency_type         STRING              COMMENT 'hard (must complete) | soft (best-effort)',

  -- Audit
  created_at              TIMESTAMP NOT NULL  COMMENT 'Row creation timestamp (UTC)'

  -- TODO: Confirm whether dependency_type should have a CHECK constraint
  -- TODO: Confirm in-framework dependency resolution vs Workflows task deps (OQ-003)
)
USING DELTA
COMMENT 'Upstream/downstream dataset dependency graph'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'framework.table' = 'dataset_dependencies',
  'framework.version' = '1.0'
);


-- =============================================================================
-- TABLE 4: ingestion_config
-- =============================================================================
-- PURPOSE:
--   Primary ingestion configuration per dataset per environment.
--   Contains all parameters needed to ingest a dataset: S3 source location,
--   file format, load mode, schema/checkpoint paths, primary keys, and
--   a SHA-256 hash for change detection.
--
-- LOADED BY: ingestion_framework/metadata_service/metadata_loader.py
-- READ BY:   src/bronze/bronze_ingestion.py
--            ingestion_framework/ingestion_engine/autoloader_engine.py
-- =============================================================================

CREATE TABLE IF NOT EXISTS {catalog}.{metadata_schema}.ingestion_config (

  -- Identity
  config_id             STRING      NOT NULL  COMMENT 'UUID — unique config identifier',
  dataset_id            STRING      NOT NULL  COMMENT 'FK → dataset_registry.dataset_id',
  environment           STRING      NOT NULL  COMMENT 'dev | uat | prod',

  -- Ingestion engine
  ingestion_mode        STRING                COMMENT 'auto_loader | batch',
  load_type             STRING                COMMENT 'incremental | full',

  -- AWS S3 source
  s3_bucket             STRING                COMMENT 'Source S3 bucket name (no s3:// prefix)',
  s3_path               STRING                COMMENT 'Key prefix within the bucket',
  file_pattern          STRING                COMMENT 'File glob pattern (e.g. orders_*.csv)',

  -- File format
  format                STRING      NOT NULL  COMMENT 'csv | json | parquet | avro | delta',
  delimiter             STRING                COMMENT 'CSV field delimiter (default ",")',
  encoding              STRING                COMMENT 'File encoding (default UTF-8)',
  header                BOOLEAN               COMMENT 'CSV: whether first row is a header',
  multiline             BOOLEAN               COMMENT 'JSON: whether records span multiple lines',

  -- TODO: Add reader_options MAP<STRING,STRING> for additional per-format options (OQ-004)
  -- reader_options     MAP<STRING, STRING>    COMMENT 'Additional Spark reader options',

  -- Auto Loader volume paths
  schema_location       STRING                COMMENT 'Unity Catalog Volume path for Auto Loader schema files',
  checkpoint_location   STRING                COMMENT 'Unity Catalog Volume path for Auto Loader checkpoints',

  -- Key columns
  primary_keys          ARRAY<STRING>         COMMENT 'Primary key column(s) for MERGE and deduplication',
  watermark_column      STRING                COMMENT 'Timestamp column for incremental Silver processing',
  watermark_delay       STRING                COMMENT 'Late data tolerance (e.g. "2 hours")',

  -- Schedule
  frequency             STRING                COMMENT 'daily | hourly | weekly | on_demand',

  -- Target
  target_table          STRING                COMMENT 'Fully qualified target Delta table name',

  -- Change detection
  config_hash           STRING                COMMENT 'SHA-256 hash of this config — used for deduplication',

  -- Audit
  created_at            TIMESTAMP   NOT NULL  COMMENT 'Row creation timestamp (UTC)',
  updated_at            TIMESTAMP             COMMENT 'Last update timestamp (UTC)'

  -- TODO: Confirm whether (dataset_id, environment) should be a unique constraint
  -- TODO: Confirm reader_options column approach for extensibility (OQ-004)
)
USING DELTA
COMMENT 'Per-dataset ingestion configuration — drives all ingestion engine behaviour'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'framework.table' = 'ingestion_config',
  'framework.version' = '1.0'
);


-- =============================================================================
-- TABLE 5: ingestion_runtime_state
-- =============================================================================
-- PURPOSE:
--   Append-only run history table. Every ingestion run (Bronze, Silver, Gold)
--   appends one row per dataset per run. Enables monitoring, alerting, SLA
--   tracking, and debugging failed runs.
--
-- WRITTEN BY: src/bronze/bronze_ingestion.py
--             src/silver/silver_transformations.py
--             src/gold/gold_transformations.py
-- READ BY:    Monitoring dashboards, SLA alerting (Phase 4)
-- NOTE:       See OQ-005 — confirmed append-only (no MERGE).
-- =============================================================================

CREATE TABLE IF NOT EXISTS {catalog}.{metadata_schema}.ingestion_runtime_state (

  run_id                STRING      NOT NULL  COMMENT 'Databricks job run ID',
  dataset_id            STRING      NOT NULL  COMMENT 'FK → dataset_registry.dataset_id',
  environment           STRING      NOT NULL  COMMENT 'dev | uat | prod',
  layer                 STRING      NOT NULL  COMMENT 'bronze | silver | gold',

  -- Timing
  start_time            TIMESTAMP             COMMENT 'Run start time (UTC)',
  end_time              TIMESTAMP             COMMENT 'Run end time (UTC)',
  duration_seconds      LONG                  COMMENT 'Elapsed seconds',

  -- Status
  status                STRING      NOT NULL  COMMENT 'RUNNING | SUCCESS | FAILED | SKIPPED',

  -- Metrics
  rows_ingested         LONG                  COMMENT 'Rows written in this run',
  rows_failed_dq        LONG                  COMMENT 'Rows that failed DQ checks (Silver/Gold only)',
  source_file_count     LONG                  COMMENT 'Number of source files processed (Bronze only)',

  -- Error details
  error_message         STRING                COMMENT 'Full error message if status = FAILED',

  -- Audit
  created_at            TIMESTAMP   NOT NULL  COMMENT 'Row insertion timestamp (UTC)'

  -- TODO: Consider partitioning by created_at date for query performance
  -- TODO: Add retention policy (VACUUM) recommendation in deployment_guide.md
)
USING DELTA
COMMENT 'Append-only ingestion run history — one row per dataset per run'
PARTITIONED BY (layer)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'framework.table' = 'ingestion_runtime_state',
  'framework.version' = '1.0'
);


-- =============================================================================
-- TABLE 6: dq_sla_config
-- =============================================================================
-- PURPOSE:
--   Data Quality rule definitions and SLA thresholds per dataset.
--   Rules are evaluated by the DataQualityEngine during Silver processing.
--   Last-run results (pass %, timestamp) are updated in-place after each run.
--
-- LOADED BY: ingestion_framework/metadata_service/metadata_loader.py (initial rules)
-- READ BY:   src/silver/data_quality.py
-- WRITTEN BY: src/silver/data_quality.py (last_run_* columns updated after each run)
-- NOTE:       See OQ-006 — separate dq_results history table may be added.
-- =============================================================================

CREATE TABLE IF NOT EXISTS {catalog}.{metadata_schema}.dq_sla_config (

  -- Identity
  dq_rule_id            STRING      NOT NULL  COMMENT 'UUID — unique DQ rule identifier',
  dataset_id            STRING      NOT NULL  COMMENT 'FK → dataset_registry.dataset_id',
  rule_name             STRING      NOT NULL  COMMENT 'Human-readable rule name',

  -- Rule definition
  rule_type             STRING      NOT NULL  COMMENT 'not_null | unique | range | regex | referential',
  column_name           STRING                COMMENT 'Column the rule applies to',
  rule_expression       STRING                COMMENT 'Rule logic (SQL expression, regex pattern, etc.)',

  -- Threshold
  threshold_pct         DECIMAL(5,2)          COMMENT 'Minimum % of rows that must pass (e.g. 99.5)',
  fail_on_breach        BOOLEAN               COMMENT 'If true, abort Silver write when threshold breached',

  -- SLA
  sla_type              STRING                COMMENT 'completeness | freshness | accuracy | timeliness',
  sla_deadline          STRING                COMMENT 'SLA time string (e.g. "08:00 UTC") — TODO: confirm format',

  -- Last run results (updated in-place after each evaluation)
  last_run_pass_pct     DECIMAL(5,2)          COMMENT 'Pass % from the last DQ evaluation',
  last_run_at           TIMESTAMP             COMMENT 'Timestamp of last DQ evaluation',

  -- Status
  active                BOOLEAN     NOT NULL  COMMENT 'False = rule is disabled',

  -- Audit
  created_at            TIMESTAMP   NOT NULL  COMMENT 'Row creation timestamp (UTC)'

  -- TODO: Add dq_results history table (OQ-006) — one row per rule per run
  -- TODO: Confirm sla_deadline format (string vs TIME vs STRUCT)
)
USING DELTA
COMMENT 'Data quality rule definitions and SLA thresholds per dataset'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'framework.table' = 'dq_sla_config',
  'framework.version' = '1.0'
);


-- =============================================================================
-- TABLE 7: schema_registry
-- =============================================================================
-- PURPOSE:
--   Versioned schema history for each dataset. A new row is inserted whenever
--   the inferred or declared schema changes, enabling schema evolution audit,
--   rollback planning, and compatibility checks.
--
-- WRITTEN BY: ingestion_framework/ingestion_engine/schema_registry.py
-- READ BY:    src/silver/silver_transformations.py (type cast guidance)
--             Monitoring and lineage tools
-- NOTE:       schema_json stored as Spark StructType.json() format (OQ-007).
-- =============================================================================

CREATE TABLE IF NOT EXISTS {catalog}.{metadata_schema}.schema_registry (

  -- Identity
  schema_version_id     STRING      NOT NULL  COMMENT 'UUID — unique schema version identifier',
  dataset_id            STRING      NOT NULL  COMMENT 'FK → dataset_registry.dataset_id',
  version_number        INT         NOT NULL  COMMENT 'Incrementing version number (1, 2, 3, ...)',

  -- Schema content
  schema_json           STRING      NOT NULL  COMMENT 'Full schema as Spark StructType.json() JSON string',
  change_type           STRING                COMMENT 'initial | add_column | drop_column | rename_column | type_change',
  changed_columns       ARRAY<STRING>         COMMENT 'Column names added/removed/modified in this version',

  -- Status
  is_current            BOOLEAN     NOT NULL  COMMENT 'True for the most recent version only',

  -- Audit
  registered_at         TIMESTAMP   NOT NULL  COMMENT 'When this version was registered (UTC)',
  registered_by         STRING                COMMENT 'Service principal or user that registered the version'

  -- TODO: Confirm schema_json format — StructType.json() vs custom (OQ-007)
  -- TODO: Add CHECK constraint on change_type values
  -- TODO: Consider storing schema as a VARIANT column when UC Variant type is available
)
USING DELTA
COMMENT 'Versioned schema history for all ingested datasets'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'framework.table' = 'schema_registry',
  'framework.version' = '1.0'
);


-- =============================================================================
-- Post-Creation: Recommended Grants
-- =============================================================================
-- Run these after creating the tables. Replace {service_principal} and
-- {read_group} with your actual values.
--
-- Grant WRITE to the ingestion service principal:
--   GRANT SELECT, INSERT, UPDATE, DELETE
--     ON SCHEMA {catalog}.{metadata_schema}
--     TO `{service_principal}`;
--
-- Grant READ to data engineers and stewards:
--   GRANT SELECT
--     ON SCHEMA {catalog}.{metadata_schema}
--     TO `{read_group}`;
--
-- TODO: Confirm access control groups and service principal names per environment.
-- =============================================================================


-- =============================================================================
-- Post-Creation: Unity Catalog Volume for Auto Loader
-- =============================================================================
-- Create the volume used for Auto Loader schema and checkpoint storage:
--
-- CREATE VOLUME IF NOT EXISTS {catalog}.{metadata_schema}.autoloader_volume
--   COMMENT 'Auto Loader schema inference files and streaming checkpoints';
--
-- Volume paths used by the framework:
--   /Volumes/{catalog}/{metadata_schema}/autoloader_volume/schemas/{dataset_name}/
--   /Volumes/{catalog}/{metadata_schema}/autoloader_volume/checkpoints/{dataset_name}/
-- =============================================================================
