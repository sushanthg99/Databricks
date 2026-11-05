-- =============================================================================
-- DEMO: Metadata Tables DDL
-- Dynamic Databricks Ingestion Framework
-- =============================================================================
--
-- PURPOSE:
--   Creates all 7 framework metadata tables in hive_metastore.ingestion_metadata
--   for the end-to-end demo. Safe to re-run (IF NOT EXISTS).
--
-- HOW TO RUN:
--   1. Open Databricks SQL Editor
--   2. Select hive_metastore as your catalog (top-left dropdown)
--   3. Run this script section by section or all at once
--
-- AFTER RUNNING:
--   Run the demo notebook: demo/notebooks/demo_end_to_end.py
-- =============================================================================

-- Step 1: Create schemas
CREATE SCHEMA IF NOT EXISTS hive_metastore.ingestion_metadata
    COMMENT 'Framework metadata tables — do not write directly';

CREATE SCHEMA IF NOT EXISTS hive_metastore.ecommerce_bronze
    COMMENT 'Raw ecommerce Bronze layer — ingested by demo framework';

-- =============================================================================
-- TABLE 1: dataset_registry
-- =============================================================================
CREATE TABLE IF NOT EXISTS hive_metastore.ingestion_metadata.dataset_registry (
    dataset_id           STRING    NOT NULL COMMENT 'Deterministic UUID v5',
    dataset_name         STRING    NOT NULL COMMENT 'Logical dataset name',
    version              STRING    NOT NULL COMMENT 'Schema version',
    dataset_type         STRING             COMMENT 'transactional|reference|event',
    data_tier            STRING    NOT NULL COMMENT 'bronze|silver|gold',
    source_system        STRING    NOT NULL COMMENT 'ecommerce|erp|crm etc.',
    business_description STRING             COMMENT 'Human-readable description',
    criticality          STRING             COMMENT 'high|medium|low',
    classification       STRING             COMMENT 'public|internal|confidential',
    pii                  BOOLEAN            COMMENT 'Contains PII data?',
    active               BOOLEAN   NOT NULL COMMENT 'False = excluded from ingestion',
    created_at           TIMESTAMP NOT NULL,
    updated_at           TIMESTAMP,
    created_by           STRING
)
USING DELTA
COMMENT 'Master registry of all datasets in the framework'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- =============================================================================
-- TABLE 2: dataset_tags
-- =============================================================================
CREATE TABLE IF NOT EXISTS hive_metastore.ingestion_metadata.dataset_tags (
    dataset_id  STRING    NOT NULL COMMENT 'FK → dataset_registry',
    tag_key     STRING    NOT NULL COMMENT 'Tag name (domain, owner, sensitivity)',
    tag_value   STRING             COMMENT 'Tag value',
    created_at  TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Key-value tags for datasets';

-- =============================================================================
-- TABLE 3: dataset_dependencies
-- =============================================================================
CREATE TABLE IF NOT EXISTS hive_metastore.ingestion_metadata.dataset_dependencies (
    dependency_id         STRING    NOT NULL,
    upstream_dataset_id   STRING    NOT NULL COMMENT 'FK → dataset_registry (source)',
    downstream_dataset_id STRING    NOT NULL COMMENT 'FK → dataset_registry (target)',
    dependency_type       STRING             COMMENT 'hard|soft',
    created_at            TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Upstream/downstream dataset dependency graph';

-- =============================================================================
-- TABLE 4: ingestion_config
-- =============================================================================
CREATE TABLE IF NOT EXISTS hive_metastore.ingestion_metadata.ingestion_config (
    config_id            STRING    NOT NULL,
    dataset_id           STRING    NOT NULL COMMENT 'FK → dataset_registry',
    environment          STRING    NOT NULL COMMENT 'demo|dev|uat|prod',
    ingestion_mode       STRING             COMMENT 'auto_loader|batch',
    load_type            STRING             COMMENT 'incremental|full',
    s3_bucket            STRING             COMMENT 'Source S3 bucket (prod)',
    s3_path              STRING             COMMENT 'S3 key prefix (prod)',
    file_pattern         STRING             COMMENT 'File glob pattern',
    format               STRING    NOT NULL COMMENT 'csv|json|parquet|avro|delta',
    delimiter            STRING             COMMENT 'Field delimiter (CSV/TSV)',
    encoding             STRING             COMMENT 'File encoding',
    header               BOOLEAN            COMMENT 'CSV has header row?',
    multiline            BOOLEAN            COMMENT 'JSON multi-line?',
    schema_location      STRING             COMMENT 'Auto Loader schema path',
    checkpoint_location  STRING             COMMENT 'Auto Loader checkpoint path',
    primary_keys         STRING             COMMENT 'JSON array of PK columns',
    watermark_column     STRING             COMMENT 'Timestamp column for incremental',
    watermark_delay      STRING             COMMENT 'Late data tolerance',
    frequency            STRING             COMMENT 'daily|hourly|weekly',
    target_table         STRING             COMMENT 'Fully qualified Delta table',
    config_hash          STRING             COMMENT 'SHA-256 for change detection',
    created_at           TIMESTAMP NOT NULL,
    updated_at           TIMESTAMP
)
USING DELTA
COMMENT 'Per-dataset ingestion configuration'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- =============================================================================
-- TABLE 5: ingestion_runtime_state
-- =============================================================================
CREATE TABLE IF NOT EXISTS hive_metastore.ingestion_metadata.ingestion_runtime_state (
    run_id             STRING    NOT NULL COMMENT 'UUID per job run',
    dataset_id         STRING    NOT NULL COMMENT 'FK → dataset_registry',
    environment        STRING    NOT NULL,
    layer              STRING    NOT NULL COMMENT 'bronze|silver|gold',
    start_time         TIMESTAMP,
    end_time           TIMESTAMP,
    duration_seconds   DOUBLE,
    status             STRING    NOT NULL COMMENT 'SUCCESS|FAILED|SKIPPED|RUNNING',
    rows_ingested      LONG      COMMENT 'Rows written this run',
    rows_failed_dq     LONG      COMMENT 'Rows quarantined by DQ',
    source_file_count  LONG      COMMENT 'Source files processed',
    error_message      STRING    COMMENT 'Error details if FAILED',
    created_at         TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (layer)
COMMENT 'Append-only ingestion run history';

-- =============================================================================
-- TABLE 6: dq_sla_config
-- =============================================================================
CREATE TABLE IF NOT EXISTS hive_metastore.ingestion_metadata.dq_sla_config (
    dq_rule_id        STRING         NOT NULL,
    dataset_id        STRING         NOT NULL COMMENT 'FK → dataset_registry',
    rule_name         STRING         NOT NULL,
    rule_type         STRING         NOT NULL COMMENT 'not_null|unique|range|regex',
    column_name       STRING         COMMENT 'Target column',
    rule_expression   STRING         COMMENT 'SQL expression or regex',
    threshold_pct     DECIMAL(5,2)   COMMENT 'Min % rows that must pass',
    fail_on_breach    BOOLEAN        COMMENT 'Abort write if threshold breached?',
    sla_type          STRING         COMMENT 'completeness|accuracy|timeliness',
    sla_deadline      STRING,
    last_run_pass_pct DECIMAL(5,2)   COMMENT 'Pass % from last run',
    last_run_at       TIMESTAMP,
    active            BOOLEAN        NOT NULL,
    created_at        TIMESTAMP      NOT NULL
)
USING DELTA
COMMENT 'Data quality rule definitions and SLA thresholds';

-- =============================================================================
-- TABLE 7: schema_registry
-- =============================================================================
CREATE TABLE IF NOT EXISTS hive_metastore.ingestion_metadata.schema_registry (
    schema_version_id  STRING         NOT NULL,
    dataset_id         STRING         NOT NULL COMMENT 'FK → dataset_registry',
    version_number     INT            NOT NULL COMMENT 'Incrementing: 1, 2, 3...',
    schema_json        STRING         NOT NULL COMMENT 'Spark StructType.json()',
    change_type        STRING         COMMENT 'initial|add_column|drop_column|type_change',
    changed_columns    ARRAY<STRING>  COMMENT 'Columns that changed',
    is_current         BOOLEAN        NOT NULL COMMENT 'True for latest version only',
    registered_at      TIMESTAMP      NOT NULL,
    registered_by      STRING
)
USING DELTA
COMMENT 'Versioned schema history for all ingested datasets';

-- =============================================================================
-- Verify all tables created
-- =============================================================================
SHOW TABLES IN hive_metastore.ingestion_metadata;
