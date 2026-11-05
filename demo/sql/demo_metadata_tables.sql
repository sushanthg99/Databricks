-- =============================================================================
-- Demo Metadata Tables DDL
-- Dynamic Databricks Ingestion Framework — Demo Mode (No S3)
-- =============================================================================
--
-- PURPOSE:
--   Creates all 7 metadata tables in the `workspace` Unity Catalog.
--   Use this script for your current Databricks workspace.
--   hive_metastore is DISABLED in this workspace — all tables use Unity Catalog.
--
-- CATALOG  : workspace  (your Unity Catalog)
-- SCHEMA   : ingestion_metadata
--
-- HOW TO RUN:
--   1. Open Databricks SQL Editor
--   2. Make sure catalog = workspace is selected
--   3. Run this entire script top to bottom
--   4. Verify: SHOW TABLES IN workspace.ingestion_metadata;
-- =============================================================================


-- =============================================================================
-- STEP 1: Create the metadata schema
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS workspace.ingestion_metadata
  COMMENT 'Demo ingestion framework metadata tables';


-- =============================================================================
-- TABLE 1: dataset_registry
-- Master registry of all datasets managed by the framework.
-- =============================================================================

CREATE TABLE IF NOT EXISTS workspace.ingestion_metadata.dataset_registry (
  dataset_id            STRING      COMMENT 'Deterministic UUID v5: hash(source_system::dataset_name)',
  dataset_name          STRING      COMMENT 'Logical dataset name (e.g. orders, customers)',
  version               STRING      COMMENT 'Dataset schema version (e.g. 1.0)',
  dataset_type          STRING      COMMENT 'transactional | reference | event | aggregate',
  data_tier             STRING      COMMENT 'bronze | silver | gold',
  source_system         STRING      COMMENT 'Source system identifier (e.g. ecommerce)',
  business_description  STRING      COMMENT 'Human-readable description',
  criticality           STRING      COMMENT 'high | medium | low',
  classification        STRING      COMMENT 'public | internal | confidential',
  pii                   BOOLEAN     COMMENT 'True if dataset contains PII',
  active                BOOLEAN     COMMENT 'False = excluded from ingestion runs',
  created_at            TIMESTAMP   COMMENT 'Row creation timestamp (UTC)',
  updated_at            TIMESTAMP   COMMENT 'Last update timestamp (UTC)',
  created_by            STRING      COMMENT 'Service or user that created the row'
)
USING DELTA
COMMENT 'Master registry of all datasets managed by the ingestion framework'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'framework.table' = 'dataset_registry'
);


-- =============================================================================
-- TABLE 2: dataset_tags
-- Key-value tag store per dataset.
-- =============================================================================

CREATE TABLE IF NOT EXISTS workspace.ingestion_metadata.dataset_tags (
  dataset_id   STRING     COMMENT 'FK → dataset_registry.dataset_id',
  tag_key      STRING     COMMENT 'Tag name (e.g. domain, owner, retention_days)',
  tag_value    STRING     COMMENT 'Tag value',
  created_at   TIMESTAMP  COMMENT 'Row creation timestamp (UTC)'
)
USING DELTA
COMMENT 'Key-value tags for datasets'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'framework.table' = 'dataset_tags'
);


-- =============================================================================
-- TABLE 3: dataset_dependencies
-- Upstream/downstream dataset relationships.
-- =============================================================================

CREATE TABLE IF NOT EXISTS workspace.ingestion_metadata.dataset_dependencies (
  dependency_id           STRING     COMMENT 'UUID — unique dependency identifier',
  upstream_dataset_id     STRING     COMMENT 'FK → dataset_registry.dataset_id (source)',
  downstream_dataset_id   STRING     COMMENT 'FK → dataset_registry.dataset_id (target)',
  dependency_type         STRING     COMMENT 'hard | soft',
  created_at              TIMESTAMP  COMMENT 'Row creation timestamp (UTC)'
)
USING DELTA
COMMENT 'Upstream/downstream dataset dependency graph'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'framework.table' = 'dataset_dependencies'
);


-- =============================================================================
-- TABLE 4: ingestion_config
-- Primary ingestion configuration per dataset per environment.
-- =============================================================================

CREATE TABLE IF NOT EXISTS workspace.ingestion_metadata.ingestion_config (
  config_id             STRING     COMMENT 'UUID — unique config identifier',
  dataset_id            STRING     COMMENT 'FK → dataset_registry.dataset_id',
  environment           STRING     COMMENT 'dev | uat | prod',
  ingestion_mode        STRING     COMMENT 'auto_loader | batch',
  load_type             STRING     COMMENT 'incremental | full',

  -- Source location (DBFS for demo, S3 for production)
  source_type           STRING     COMMENT 'dbfs | s3 | abfss',
  dbfs_path             STRING     COMMENT 'DBFS path (used when source_type=dbfs)',
  s3_bucket             STRING     COMMENT 'S3 bucket name (used when source_type=s3)',
  s3_path               STRING     COMMENT 'S3 key prefix (used when source_type=s3)',
  file_pattern          STRING     COMMENT 'File glob pattern (e.g. orders.tsv)',

  -- File format
  format                STRING     COMMENT 'csv | tsv | json | parquet | avro | delta',
  delimiter             STRING     COMMENT 'Field delimiter (tab for TSV, comma for CSV)',
  encoding              STRING     COMMENT 'File encoding (default UTF-8)',
  header                BOOLEAN    COMMENT 'Whether first row is header',
  multiline             BOOLEAN    COMMENT 'JSON: whether records span multiple lines',

  -- Auto Loader volume paths
  schema_location       STRING     COMMENT 'Volume path for Auto Loader schema files',
  checkpoint_location   STRING     COMMENT 'Volume path for Auto Loader checkpoints',

  -- Key columns
  primary_keys          STRING     COMMENT 'JSON array of primary key column names',
  watermark_column      STRING     COMMENT 'Timestamp column for incremental processing',
  watermark_delay       STRING     COMMENT 'Late data tolerance (e.g. 2 hours)',

  frequency             STRING     COMMENT 'daily | hourly | weekly | on_demand',
  target_table          STRING     COMMENT 'Fully qualified target Delta table name',
  config_hash           STRING     COMMENT 'SHA-256 hash for change detection',
  created_at            TIMESTAMP  COMMENT 'Row creation timestamp (UTC)',
  updated_at            TIMESTAMP  COMMENT 'Last update timestamp (UTC)'
)
USING DELTA
COMMENT 'Per-dataset ingestion configuration — drives all ingestion engine behaviour'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'framework.table' = 'ingestion_config'
);


-- =============================================================================
-- TABLE 5: ingestion_runtime_state
-- Append-only run history — one row per dataset per run.
-- =============================================================================

CREATE TABLE IF NOT EXISTS workspace.ingestion_metadata.ingestion_runtime_state (
  run_id                STRING     COMMENT 'Job run UUID',
  dataset_id            STRING     COMMENT 'FK → dataset_registry.dataset_id',
  environment           STRING     COMMENT 'dev | uat | prod',
  layer                 STRING     COMMENT 'bronze | silver | gold',
  start_time            TIMESTAMP  COMMENT 'Run start time (UTC)',
  end_time              TIMESTAMP  COMMENT 'Run end time (UTC)',
  duration_seconds      DOUBLE     COMMENT 'Elapsed seconds',
  status                STRING     COMMENT 'RUNNING | SUCCESS | FAILED | SKIPPED',
  rows_ingested         LONG       COMMENT 'Rows written in this run',
  rows_failed_dq        LONG       COMMENT 'Rows that failed DQ checks',
  source_file_count     LONG       COMMENT 'Number of source files processed',
  error_message         STRING     COMMENT 'Full error message if status=FAILED',
  created_at            TIMESTAMP  COMMENT 'Row insertion timestamp (UTC)'
)
USING DELTA
COMMENT 'Append-only ingestion run history'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'framework.table' = 'ingestion_runtime_state'
);


-- =============================================================================
-- TABLE 6: dq_sla_config
-- Data Quality rule definitions and SLA thresholds per dataset.
-- =============================================================================

CREATE TABLE IF NOT EXISTS workspace.ingestion_metadata.dq_sla_config (
  dq_rule_id            STRING       COMMENT 'UUID — unique DQ rule identifier',
  dataset_id            STRING       COMMENT 'FK → dataset_registry.dataset_id',
  rule_name             STRING       COMMENT 'Human-readable rule name',
  rule_type             STRING       COMMENT 'not_null | unique | range | regex | referential',
  column_name           STRING       COMMENT 'Column the rule applies to',
  rule_expression       STRING       COMMENT 'Rule logic (SQL expression, regex)',
  threshold_pct         DECIMAL(5,2) COMMENT 'Minimum % of rows that must pass',
  fail_on_breach        BOOLEAN      COMMENT 'If true, abort write when threshold breached',
  sla_type              STRING       COMMENT 'completeness | freshness | accuracy | timeliness',
  sla_deadline          STRING       COMMENT 'SLA time string (e.g. 08:00 UTC)',
  last_run_pass_pct     DECIMAL(5,2) COMMENT 'Pass % from last evaluation',
  last_run_at           TIMESTAMP    COMMENT 'Timestamp of last DQ evaluation',
  active                BOOLEAN      COMMENT 'False = rule is disabled',
  created_at            TIMESTAMP    COMMENT 'Row creation timestamp (UTC)'
)
USING DELTA
COMMENT 'Data quality rule definitions and SLA thresholds per dataset'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'framework.table' = 'dq_sla_config'
);


-- =============================================================================
-- TABLE 7: schema_registry
-- Versioned schema history per dataset.
-- =============================================================================

CREATE TABLE IF NOT EXISTS workspace.ingestion_metadata.schema_registry (
  schema_version_id   STRING     COMMENT 'UUID — unique schema version identifier',
  dataset_id          STRING     COMMENT 'FK → dataset_registry.dataset_id',
  version_number      INT        COMMENT 'Incrementing version (1, 2, 3, ...)',
  schema_json         STRING     COMMENT 'Full schema as Spark StructType.json() string',
  change_type         STRING     COMMENT 'initial | add_column | drop_column | type_change | mixed',
  changed_columns     ARRAY<STRING> COMMENT 'Column names added/changed in this version',
  is_current          BOOLEAN    COMMENT 'True for the most recent version only',
  registered_at       TIMESTAMP  COMMENT 'When this version was registered (UTC)',
  registered_by       STRING     COMMENT 'Service principal or user'
)
USING DELTA
COMMENT 'Versioned schema history for all ingested datasets'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'framework.table' = 'schema_registry'
);


-- =============================================================================
-- STEP 2: Verify all tables were created
-- =============================================================================

SHOW TABLES IN workspace.ingestion_metadata;


-- =============================================================================
-- STEP 3: Create Bronze schema for demo datasets
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS workspace.demo_bronze
  COMMENT 'Demo Bronze Delta tables — ecommerce raw data';

-- Verify
SHOW SCHEMAS IN workspace LIKE 'demo*';
