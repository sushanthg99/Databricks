# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Dynamic Databricks Ingestion Framework — End-to-End Demo
# MAGIC
# MAGIC **No AWS S3 required** — uses DBFS (Databricks File System) as the source.
# MAGIC TSV files are uploaded to DBFS in Step 1, then the full framework pipeline runs.
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC | Step | Action |
# MAGIC |---|---|
# MAGIC | **Step 1** | Setup — create metadata tables + Bronze schema in hive_metastore |
# MAGIC | **Step 2** | Upload TSV files to DBFS |
# MAGIC | **Step 3** | Register dataset configs into metadata tables (metadata loader) |
# MAGIC | **Step 4** | Run Bronze ingestion for all 3 datasets |
# MAGIC | **Step 5** | Verify — query Bronze tables and show results |
# MAGIC | **Step 6** | View metadata tables — registry, config, runtime state, schema |
# MAGIC
# MAGIC ## Datasets
# MAGIC | Dataset | Rows | Format | Load type |
# MAGIC |---|---|---|---|
# MAGIC | orders | 30 | TSV | Full batch overwrite |
# MAGIC | customers | 10 | TSV | Full batch overwrite |
# MAGIC | products | 10 | TSV | Full batch overwrite |
# MAGIC
# MAGIC ## After this demo
# MAGIC When you get AWS S3, change `source_type: s3` in `configs/domain_config.yml`
# MAGIC and add your `s3_bucket` and `s3_path` — no other code changes needed.

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Configuration

# COMMAND ----------

# ── Configuration ──────────────────────────────────────────────────────────────
CATALOG          = "hive_metastore"       # Default Databricks catalog — no Unity Catalog needed
METADATA_SCHEMA  = "ingestion_metadata"   # Schema for framework metadata tables
BRONZE_SCHEMA    = "demo_bronze"          # Schema for Bronze Delta tables
ENVIRONMENT      = "dev"
SOURCE_SYSTEM    = "ecommerce"

# DBFS paths — where TSV files are uploaded
DBFS_BASE        = "dbfs:/FileStore/demo/ecommerce"
DBFS_ORDERS      = f"{DBFS_BASE}/orders"
DBFS_CUSTOMERS   = f"{DBFS_BASE}/customers"
DBFS_PRODUCTS    = f"{DBFS_BASE}/products"

# Bundle files location — where deployed code lives
BUNDLE_PATH = (
    "/Workspace/Users/sushanth.gangadhara@blumetra.com"
    "/.bundle/dynamic-databricks-ingestion/dev/files"
)

print("=" * 60)
print("  DEMO CONFIGURATION")
print("=" * 60)
print(f"  Catalog          : {CATALOG}")
print(f"  Metadata Schema  : {METADATA_SCHEMA}")
print(f"  Bronze Schema    : {BRONZE_SCHEMA}")
print(f"  Environment      : {ENVIRONMENT}")
print(f"  Source System    : {SOURCE_SYSTEM}")
print(f"  DBFS Base Path   : {DBFS_BASE}")
print(f"  Bundle Path      : {BUNDLE_PATH}")
print("=" * 60)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1 — Setup: Create Metadata Tables and Bronze Schema
# MAGIC
# MAGIC Creates all 7 metadata tables in `hive_metastore.ingestion_metadata`
# MAGIC and the `hive_metastore.demo_bronze` schema for Bronze Delta tables.

# COMMAND ----------

import logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("demo_notebook")

# ── Create metadata schema ─────────────────────────────────────────────────────
print("Creating metadata schema...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA} COMMENT 'Demo ingestion metadata'")
print(f"  ✓ {CATALOG}.{METADATA_SCHEMA}")

# ── Create Bronze schema ───────────────────────────────────────────────────────
print("Creating Bronze schema...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA} COMMENT 'Demo Bronze Delta tables'")
print(f"  ✓ {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# ── Create dataset_registry ────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}.dataset_registry (
  dataset_id            STRING,
  dataset_name          STRING,
  version               STRING,
  dataset_type          STRING,
  data_tier             STRING,
  source_system         STRING,
  business_description  STRING,
  criticality           STRING,
  classification        STRING,
  pii                   BOOLEAN,
  active                BOOLEAN,
  created_at            TIMESTAMP,
  updated_at            TIMESTAMP,
  created_by            STRING
) USING DELTA
COMMENT 'Master registry of all datasets'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

# ── Create dataset_tags ────────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}.dataset_tags (
  dataset_id   STRING,
  tag_key      STRING,
  tag_value    STRING,
  created_at   TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

# ── Create ingestion_config ────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}.ingestion_config (
  config_id             STRING,
  dataset_id            STRING,
  environment           STRING,
  ingestion_mode        STRING,
  load_type             STRING,
  source_type           STRING,
  dbfs_path             STRING,
  s3_bucket             STRING,
  s3_path               STRING,
  file_pattern          STRING,
  format                STRING,
  delimiter             STRING,
  encoding              STRING,
  header                BOOLEAN,
  multiline             BOOLEAN,
  schema_location       STRING,
  checkpoint_location   STRING,
  primary_keys          STRING,
  watermark_column      STRING,
  watermark_delay       STRING,
  frequency             STRING,
  target_table          STRING,
  config_hash           STRING,
  created_at            TIMESTAMP,
  updated_at            TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

# ── Create ingestion_runtime_state ─────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}.ingestion_runtime_state (
  run_id                STRING,
  dataset_id            STRING,
  environment           STRING,
  layer                 STRING,
  start_time            TIMESTAMP,
  end_time              TIMESTAMP,
  duration_seconds      DOUBLE,
  status                STRING,
  rows_ingested         LONG,
  rows_failed_dq        LONG,
  source_file_count     LONG,
  error_message         STRING,
  created_at            TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

# ── Create schema_registry ─────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}.schema_registry (
  schema_version_id   STRING,
  dataset_id          STRING,
  version_number      INT,
  schema_json         STRING,
  change_type         STRING,
  changed_columns     ARRAY<STRING>,
  is_current          BOOLEAN,
  registered_at       TIMESTAMP,
  registered_by       STRING
) USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

# ── Create dq_sla_config ───────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}.dq_sla_config (
  dq_rule_id            STRING,
  dataset_id            STRING,
  rule_name             STRING,
  rule_type             STRING,
  column_name           STRING,
  rule_expression       STRING,
  threshold_pct         DECIMAL(5,2),
  fail_on_breach        BOOLEAN,
  sla_type              STRING,
  sla_deadline          STRING,
  last_run_pass_pct     DECIMAL(5,2),
  last_run_at           TIMESTAMP,
  active                BOOLEAN,
  created_at            TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

print("\n✓ All metadata tables created successfully")
spark.sql(f"SHOW TABLES IN {CATALOG}.{METADATA_SCHEMA}").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2 — Upload TSV Files to DBFS
# MAGIC
# MAGIC Reads TSV files from the bundle path and uploads them to DBFS
# MAGIC so the ingestion engine can read them as if they were in S3.

# COMMAND ----------

import os

def upload_tsv_to_dbfs(local_bundle_path: str, dbfs_target_dir: str, filename: str):
    """
    Copy a TSV file from the bundle workspace path to a DBFS directory.
    Uses dbutils.fs.cp which works within Databricks.
    """
    source = f"{local_bundle_path}/demo/data/{filename}"
    target = f"{dbfs_target_dir}/{filename}"

    # Create target directory
    dbutils.fs.mkdirs(dbfs_target_dir)

    # Copy file
    dbutils.fs.cp(source, target)
    print(f"  ✓ Uploaded: {filename} → {dbfs_target_dir}")
    return target

print("Uploading TSV files to DBFS...")
print("-" * 50)

# Upload all three TSV files
upload_tsv_to_dbfs(BUNDLE_PATH, DBFS_ORDERS,    "orders.tsv")
upload_tsv_to_dbfs(BUNDLE_PATH, DBFS_CUSTOMERS,  "customers.tsv")
upload_tsv_to_dbfs(BUNDLE_PATH, DBFS_PRODUCTS,   "products.tsv")

print("-" * 50)
print("\n✓ All TSV files uploaded to DBFS")

# Verify files exist
print("\nVerifying uploaded files:")
for path in [DBFS_ORDERS, DBFS_CUSTOMERS, DBFS_PRODUCTS]:
    files = dbutils.fs.ls(path)
    for f in files:
        print(f"  {f.path}  ({f.size:,} bytes)")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3 — Register Dataset Configs into Metadata Tables
# MAGIC
# MAGIC The metadata loader reads `demo/demo_config.yml`, computes SHA-256 hashes,
# MAGIC and MERGEs each dataset into the 4 metadata tables.

# COMMAND ----------

import sys
if BUNDLE_PATH not in sys.path:
    sys.path.insert(0, BUNDLE_PATH)

from pathlib import Path
from ingestion_framework.metadata_service.metadata_loader import MetadataLoader

# Point to demo_config.yml
demo_config_path = Path(f"{BUNDLE_PATH}/demo/demo_config.yml")

print(f"Loading metadata from: {demo_config_path}")
print(f"Target catalog.schema: {CATALOG}.{METADATA_SCHEMA}")
print("-" * 50)

loader = MetadataLoader(
    spark=spark,
    catalog=CATALOG,
    metadata_schema=METADATA_SCHEMA,
    environment=ENVIRONMENT,
)

result = loader.load_from_file(demo_config_path)

print("\n✓ Metadata registration complete")
print(f"  Total    : {result['total']}")
print(f"  Registered: {result['registered']}")
print(f"  Skipped  : {result['skipped']} (hash unchanged)")
print(f"  Failed   : {result['failed']}")

# COMMAND ----------

# Verify dataset_registry
print("\n── dataset_registry ──────────────────────────────────")
spark.sql(f"""
    SELECT dataset_id, dataset_name, data_tier, source_system,
           criticality, pii, active
    FROM {CATALOG}.{METADATA_SCHEMA}.dataset_registry
    ORDER BY dataset_name
""").show(truncate=False)

# Verify ingestion_config
print("── ingestion_config ───────────────────────────────────")
spark.sql(f"""
    SELECT dataset_id, environment, source_type, load_type,
           format, dbfs_path, target_table
    FROM {CATALOG}.{METADATA_SCHEMA}.ingestion_config
    ORDER BY dataset_id
""").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4 — Run Bronze Ingestion
# MAGIC
# MAGIC The BronzeIngestion orchestrator:
# MAGIC 1. Queries `ingestion_config` + `dataset_registry` for active datasets
# MAGIC 2. Calls `AutoLoaderEngine.ingest()` per dataset (full batch load from DBFS)
# MAGIC 3. Writes raw data to `hive_metastore.demo_bronze.{dataset_name}`
# MAGIC 4. Registers schema version in `schema_registry`
# MAGIC 5. Writes run metrics to `ingestion_runtime_state`

# COMMAND ----------

import time
from src.bronze.bronze_ingestion import BronzeIngestion

# Build environment config dict (matches configs/environments/dev.yml structure)
env_config = {
    "environment": ENVIRONMENT,
    "unity_catalog": {
        "catalog_name":    CATALOG,
        "metadata_schema": METADATA_SCHEMA,
    },
    "ingestion": {
        "schema_evolution_mode": "addNewColumns",
        "max_files_per_trigger": 1000,
        "log_level": "DEBUG",
    },
}

print("=" * 60)
print("  BRONZE INGESTION — STARTING")
print("=" * 60)
print(f"  Source system : {SOURCE_SYSTEM}")
print(f"  Environment   : {ENVIRONMENT}")
print(f"  Catalog       : {CATALOG}")
print(f"  Run mode      : full")
print("=" * 60)

start = time.time()

ingestion = BronzeIngestion(spark=spark, dbutils=dbutils)

summary = ingestion.run(
    source_system=SOURCE_SYSTEM,
    environment=ENVIRONMENT,
    run_mode="full",
    catalog_name=CATALOG,
    metadata_schema=METADATA_SCHEMA,
    env_config=env_config,
)

duration = round(time.time() - start, 2)

print("\n" + "=" * 60)
print("  BRONZE INGESTION — COMPLETE")
print("=" * 60)
print(f"  Total datasets   : {summary.get('total', 0)}")
print(f"  Succeeded        : {summary.get('succeeded', 0)}")
print(f"  Failed           : {summary.get('failed', 0)}")
print(f"  Skipped          : {summary.get('skipped', 0)}")
print(f"  Duration (sec)   : {summary.get('duration_seconds', duration)}")
print("=" * 60)

if summary.get("failed", 0) > 0:
    raise RuntimeError(
        f"{summary['failed']} dataset(s) failed ingestion. "
        f"Check ingestion_runtime_state for details."
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5 — Verify Bronze Tables
# MAGIC
# MAGIC Query the three Bronze Delta tables to confirm data was ingested correctly.

# COMMAND ----------

print("=" * 60)
print("  BRONZE TABLE VERIFICATION")
print("=" * 60)

# ── Orders Bronze ──────────────────────────────────────────────────────────────
print("\n── hive_metastore.demo_bronze.orders ─────────────────")
orders_df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.orders")
print(f"  Row count : {orders_df.count()}")
print(f"  Columns   : {len(orders_df.columns)}")
print("\n  Sample rows:")
orders_df.select(
    "order_id", "customer_id", "product_id",
    "order_date", "order_status", "order_amount",
    "_source_file", "_ingestion_date"
).show(5, truncate=True)

# ── Customers Bronze ───────────────────────────────────────────────────────────
print("\n── hive_metastore.demo_bronze.customers ──────────────")
customers_df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.customers")
print(f"  Row count : {customers_df.count()}")
print(f"  Columns   : {len(customers_df.columns)}")
print("\n  Sample rows:")
customers_df.select(
    "customer_id", "first_name", "last_name",
    "city", "state", "loyalty_tier",
    "_ingestion_date"
).show(5, truncate=True)

# ── Products Bronze ────────────────────────────────────────────────────────────
print("\n── hive_metastore.demo_bronze.products ───────────────")
products_df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.products")
print(f"  Row count : {products_df.count()}")
print(f"  Columns   : {len(products_df.columns)}")
print("\n  Sample rows:")
products_df.select(
    "product_id", "product_name", "category",
    "unit_price", "stock_quantity", "is_active",
    "_ingestion_date"
).show(5, truncate=True)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Quick Analytics on Bronze Data

# COMMAND ----------

# Orders by status
print("── Orders by Status ──────────────────────────────────")
spark.sql(f"""
    SELECT order_status,
           COUNT(*)               AS order_count,
           ROUND(SUM(order_amount), 2) AS total_amount
    FROM {CATALOG}.{BRONZE_SCHEMA}.orders
    GROUP BY order_status
    ORDER BY order_count DESC
""").show()

# Revenue by customer
print("── Top 5 Customers by Order Value ────────────────────")
spark.sql(f"""
    SELECT o.customer_id,
           c.first_name,
           c.last_name,
           c.loyalty_tier,
           COUNT(o.order_id)           AS total_orders,
           ROUND(SUM(o.order_amount), 2) AS total_spent
    FROM {CATALOG}.{BRONZE_SCHEMA}.orders    o
    JOIN {CATALOG}.{BRONZE_SCHEMA}.customers c
      ON o.customer_id = c.customer_id
    GROUP BY o.customer_id, c.first_name, c.last_name, c.loyalty_tier
    ORDER BY total_spent DESC
    LIMIT 5
""").show()

# Orders by product category
print("── Orders by Product Category ────────────────────────")
spark.sql(f"""
    SELECT p.category,
           COUNT(o.order_id)                AS order_count,
           ROUND(SUM(o.order_amount), 2)    AS total_revenue
    FROM {CATALOG}.{BRONZE_SCHEMA}.orders   o
    JOIN {CATALOG}.{BRONZE_SCHEMA}.products p
      ON o.product_id = p.product_id
    GROUP BY p.category
    ORDER BY total_revenue DESC
""").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6 — View Metadata Tables
# MAGIC
# MAGIC Inspect the metadata tables to see how the framework tracked this run.

# COMMAND ----------

# ── ingestion_runtime_state ────────────────────────────────────────────────────
print("── ingestion_runtime_state ───────────────────────────")
spark.sql(f"""
    SELECT
        r.run_id,
        d.dataset_name,
        r.status,
        r.rows_ingested,
        r.duration_seconds,
        r.error_message,
        r.created_at
    FROM {CATALOG}.{METADATA_SCHEMA}.ingestion_runtime_state r
    JOIN {CATALOG}.{METADATA_SCHEMA}.dataset_registry d
      ON r.dataset_id = d.dataset_id
    ORDER BY r.created_at DESC
""").show(truncate=False)

# COMMAND ----------

# ── schema_registry ────────────────────────────────────────────────────────────
print("── schema_registry ───────────────────────────────────")
spark.sql(f"""
    SELECT
        d.dataset_name,
        s.version_number,
        s.change_type,
        s.is_current,
        s.registered_at
    FROM {CATALOG}.{METADATA_SCHEMA}.schema_registry s
    JOIN {CATALOG}.{METADATA_SCHEMA}.dataset_registry d
      ON s.dataset_id = d.dataset_id
    ORDER BY d.dataset_name, s.version_number
""").show(truncate=False)

# COMMAND ----------

# ── dq_sla_config ──────────────────────────────────────────────────────────────
print("── dq_sla_config (registered DQ rules) ──────────────")
spark.sql(f"""
    SELECT
        d.dataset_name,
        q.rule_name,
        q.rule_type,
        q.column_name,
        q.threshold_pct,
        q.fail_on_breach,
        q.active
    FROM {CATALOG}.{METADATA_SCHEMA}.dq_sla_config q
    JOIN {CATALOG}.{METADATA_SCHEMA}.dataset_registry d
      ON q.dataset_id = d.dataset_id
    ORDER BY d.dataset_name, q.rule_name
""").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC The Bronze ingestion pipeline ran successfully end-to-end:
# MAGIC
# MAGIC | Dataset | Bronze Table | Rows |
# MAGIC |---|---|---|
# MAGIC | orders | `hive_metastore.demo_bronze.orders` | 30 |
# MAGIC | customers | `hive_metastore.demo_bronze.customers` | 10 |
# MAGIC | products | `hive_metastore.demo_bronze.products` | 10 |
# MAGIC
# MAGIC Each Bronze table has the raw source columns **plus** these metadata columns:
# MAGIC - `_source_file` — originating DBFS file path
# MAGIC - `_ingestion_date` — date the data was ingested
# MAGIC - `_ingestion_timestamp` — exact timestamp
# MAGIC
# MAGIC ### What was tracked automatically
# MAGIC - ✅ `dataset_registry` — dataset identity and classification
# MAGIC - ✅ `ingestion_config` — source path, format, load type
# MAGIC - ✅ `ingestion_runtime_state` — rows ingested, duration, status per dataset
# MAGIC - ✅ `schema_registry` — schema version recorded for each dataset
# MAGIC - ✅ `dq_sla_config` — DQ rules registered from YAML config
# MAGIC - ✅ `dataset_tags` — domain, owner, sensitivity tags
# MAGIC
# MAGIC ### Next steps
# MAGIC - **Silver layer** — run cleansing, deduplication, type casting (Phase 2)
# MAGIC - **When you get S3** — change `source_type: s3` in `configs/domain_config.yml`

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC Run this cell only if you want to reset the demo and start fresh.

# COMMAND ----------

# Uncomment to clean up demo tables and DBFS files
# WARNING: This deletes all demo data

# print("Cleaning up demo tables...")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{BRONZE_SCHEMA}.orders")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{BRONZE_SCHEMA}.customers")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{BRONZE_SCHEMA}.products")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{METADATA_SCHEMA}.dataset_registry")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{METADATA_SCHEMA}.ingestion_config")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{METADATA_SCHEMA}.ingestion_runtime_state")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{METADATA_SCHEMA}.schema_registry")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{METADATA_SCHEMA}.dq_sla_config")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{METADATA_SCHEMA}.dataset_tags")
# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{BRONZE_SCHEMA}")
# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{METADATA_SCHEMA}")
# dbutils.fs.rm("dbfs:/FileStore/demo", recurse=True)
# print("✓ Cleanup complete")
