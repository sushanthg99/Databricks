# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Dynamic Databricks Ingestion Framework — End-to-End Demo
# MAGIC
# MAGIC **No AWS S3 required** — uses Unity Catalog Volumes as the source.
# MAGIC TSV files are uploaded to a UC Volume in Step 2, then the full framework pipeline runs.
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC | Step | Action |
# MAGIC |---|---|
# MAGIC | **Step 1** | Setup — create metadata tables + Bronze schema in Unity Catalog |
# MAGIC | **Step 2** | Upload TSV files to UC Volume |
# MAGIC | **Step 3** | Register dataset configs into metadata tables (metadata loader) |
# MAGIC | **Step 4** | Run Bronze ingestion for all 3 datasets |
# MAGIC | **Step 5** | Verify — query Bronze tables and show results |
# MAGIC | **Step 6** | View metadata tables — registry, config, runtime state, schema |
# MAGIC
# MAGIC ## Storage used
# MAGIC | Type | Path |
# MAGIC |---|---|
# MAGIC | Source files | `/Volumes/workspace/ingestion_metadata/demo_volume/data/` |
# MAGIC | Bronze tables | `workspace.demo_bronze.orders / customers / products` |
# MAGIC | Metadata tables | `workspace.ingestion_metadata.*` |
# MAGIC
# MAGIC ## Datasets
# MAGIC | Dataset | Rows | Format | Load type |
# MAGIC |---|---|---|---|
# MAGIC | orders | 30 | TSV | Full batch overwrite |
# MAGIC | customers | 10 | TSV | Full batch overwrite |
# MAGIC | products | 10 | TSV | Full batch overwrite |

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Configuration

# COMMAND ----------

# ── Configuration ──────────────────────────────────────────────────────────────
CATALOG          = "workspace"
METADATA_SCHEMA  = "ingestion_metadata"
BRONZE_SCHEMA    = "demo_bronze"
ENVIRONMENT      = "dev"
SOURCE_SYSTEM    = "ecommerce"

# UC Volume path — where TSV source files live
VOLUME_BASE      = f"/Volumes/{CATALOG}/{METADATA_SCHEMA}/demo_volume"
VOLUME_DATA      = f"{VOLUME_BASE}/data"
VOLUME_ORDERS    = f"{VOLUME_DATA}/orders"
VOLUME_CUSTOMERS = f"{VOLUME_DATA}/customers"
VOLUME_PRODUCTS  = f"{VOLUME_DATA}/products"

# Auto Loader schema + checkpoint locations inside the same volume
VOLUME_SCHEMAS      = f"{VOLUME_BASE}/schemas"
VOLUME_CHECKPOINTS  = f"{VOLUME_BASE}/checkpoints"

# Bundle files location
BUNDLE_PATH = (
    "/Workspace/Users/sushanth.gangadhara@blumetra.com"
    "/.bundle/dynamic-databricks-ingestion/dev/files"
)

print("=" * 65)
print("  DEMO CONFIGURATION")
print("=" * 65)
print(f"  Catalog           : {CATALOG}")
print(f"  Metadata Schema   : {METADATA_SCHEMA}")
print(f"  Bronze Schema     : {BRONZE_SCHEMA}")
print(f"  Environment       : {ENVIRONMENT}")
print(f"  Source System     : {SOURCE_SYSTEM}")
print(f"  Volume Data Path  : {VOLUME_DATA}")
print(f"  Bundle Path       : {BUNDLE_PATH}")
print("=" * 65)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1 — Setup: Create Metadata Tables and Bronze Schema

# COMMAND ----------

import logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("demo_notebook")

print("Creating schemas...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA} COMMENT 'Demo ingestion metadata'")
print(f"  ✓ {CATALOG}.{METADATA_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA} COMMENT 'Demo Bronze Delta tables'")
print(f"  ✓ {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# ── dataset_registry ───────────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}.dataset_registry (
  dataset_id STRING, dataset_name STRING, version STRING, dataset_type STRING,
  data_tier STRING, source_system STRING, business_description STRING,
  criticality STRING, classification STRING, pii BOOLEAN, active BOOLEAN,
  created_at TIMESTAMP, updated_at TIMESTAMP, created_by STRING
) USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

# ── dataset_tags ───────────────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}.dataset_tags (
  dataset_id STRING, tag_key STRING, tag_value STRING, created_at TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

# ── ingestion_config ───────────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}.ingestion_config (
  config_id STRING, dataset_id STRING, environment STRING,
  ingestion_mode STRING, load_type STRING,
  source_type STRING, dbfs_path STRING, volume_path STRING,
  s3_bucket STRING, s3_path STRING, file_pattern STRING,
  format STRING, delimiter STRING, encoding STRING,
  header BOOLEAN, multiline BOOLEAN,
  schema_location STRING, checkpoint_location STRING,
  primary_keys STRING, watermark_column STRING, watermark_delay STRING,
  frequency STRING, target_table STRING, config_hash STRING,
  created_at TIMESTAMP, updated_at TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

# ── ingestion_runtime_state ────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}.ingestion_runtime_state (
  run_id STRING, dataset_id STRING, environment STRING, layer STRING,
  start_time TIMESTAMP, end_time TIMESTAMP, duration_seconds DOUBLE,
  status STRING, rows_ingested LONG, rows_failed_dq LONG,
  source_file_count LONG, error_message STRING, created_at TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

# ── schema_registry ────────────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}.schema_registry (
  schema_version_id STRING, dataset_id STRING, version_number INT,
  schema_json STRING, change_type STRING, changed_columns ARRAY<STRING>,
  is_current BOOLEAN, registered_at TIMESTAMP, registered_by STRING
) USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

# ── dq_sla_config ──────────────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}.dq_sla_config (
  dq_rule_id STRING, dataset_id STRING, rule_name STRING, rule_type STRING,
  column_name STRING, rule_expression STRING, threshold_pct DECIMAL(5,2),
  fail_on_breach BOOLEAN, sla_type STRING, sla_deadline STRING,
  last_run_pass_pct DECIMAL(5,2), last_run_at TIMESTAMP,
  active BOOLEAN, created_at TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

print("\n✓ All 6 metadata tables created")
spark.sql(f"SHOW TABLES IN {CATALOG}.{METADATA_SCHEMA}").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2 — Upload TSV Files to Unity Catalog Volume
# MAGIC
# MAGIC Copies TSV files from the bundle workspace path into the UC Volume.
# MAGIC The volume path `/Volumes/workspace/ingestion_metadata/demo_volume/data/`
# MAGIC acts as our source "landing zone" — replacing DBFS and S3 for this demo.

# COMMAND ----------

import shutil, os

def upload_to_volume(bundle_path: str, volume_dir: str, filename: str):
    """Copy a TSV file from bundle workspace to UC Volume using Python file I/O."""
    source = f"{bundle_path}/demo/data/{filename}"
    os.makedirs(volume_dir, exist_ok=True)
    target = f"{volume_dir}/{filename}"
    shutil.copy2(source, target)
    size = os.path.getsize(target)
    print(f"  ✓ {filename}  →  {target}  ({size:,} bytes)")

print("Uploading TSV files to UC Volume...")
print("-" * 60)

upload_to_volume(BUNDLE_PATH, VOLUME_ORDERS,    "orders.tsv")
upload_to_volume(BUNDLE_PATH, VOLUME_CUSTOMERS,  "customers.tsv")
upload_to_volume(BUNDLE_PATH, VOLUME_PRODUCTS,   "products.tsv")

print("-" * 60)
print("\n✓ All TSV files uploaded to UC Volume")

# Verify
print("\nVerifying files in volume:")
for d, label in [(VOLUME_ORDERS, "orders"), (VOLUME_CUSTOMERS, "customers"), (VOLUME_PRODUCTS, "products")]:
    files = os.listdir(d)
    for f in files:
        size = os.path.getsize(f"{d}/{f}")
        print(f"  {label}/{f}  ({size:,} bytes)")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3 — Register Dataset Configs into Metadata Tables
# MAGIC
# MAGIC The metadata loader reads `demo/demo_config.yml`, computes SHA-256 hashes,
# MAGIC and MERGEs each dataset into the metadata tables.

# COMMAND ----------

import sys
if BUNDLE_PATH not in sys.path:
    sys.path.insert(0, BUNDLE_PATH)

from pathlib import Path
from ingestion_framework.metadata_service.metadata_loader import MetadataLoader

demo_config_path = Path(f"{BUNDLE_PATH}/demo/demo_config.yml")

print(f"Loading metadata from : {demo_config_path}")
print(f"Target catalog.schema : {CATALOG}.{METADATA_SCHEMA}")
print("-" * 60)

loader = MetadataLoader(
    spark=spark,
    catalog=CATALOG,
    metadata_schema=METADATA_SCHEMA,
    environment=ENVIRONMENT,
)

result = loader.load_from_file(demo_config_path)

print("\n✓ Metadata registration complete")
print(f"  Total     : {result['total']}")
print(f"  Registered: {result['registered']}")
print(f"  Skipped   : {result['skipped']}  (hash unchanged)")
print(f"  Failed    : {result['failed']}")

# COMMAND ----------

# Verify dataset_registry
print("\n── dataset_registry ──────────────────────────────────────────")
spark.sql(f"""
    SELECT dataset_id, dataset_name, data_tier, source_system, criticality, pii, active
    FROM {CATALOG}.{METADATA_SCHEMA}.dataset_registry
    ORDER BY dataset_name
""").show(truncate=False)

# Verify ingestion_config
print("── ingestion_config ───────────────────────────────────────────")
spark.sql(f"""
    SELECT dataset_id, environment, source_type, load_type,
           format, volume_path, target_table
    FROM {CATALOG}.{METADATA_SCHEMA}.ingestion_config
    ORDER BY dataset_id
""").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4 — Run Bronze Ingestion
# MAGIC
# MAGIC The `BronzeIngestion` orchestrator:
# MAGIC 1. Queries metadata tables for active datasets
# MAGIC 2. Calls `AutoLoaderEngine.ingest()` per dataset (full batch from UC Volume)
# MAGIC 3. Writes raw data to `workspace.demo_bronze.{dataset}`
# MAGIC 4. Registers schema version in `schema_registry`
# MAGIC 5. Writes run metrics to `ingestion_runtime_state`

# COMMAND ----------

import time
from src.bronze.bronze_ingestion import BronzeIngestion

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

print("=" * 65)
print("  BRONZE INGESTION — STARTING")
print("=" * 65)
print(f"  Source system : {SOURCE_SYSTEM}")
print(f"  Catalog       : {CATALOG}")
print(f"  Run mode      : full")
print("=" * 65)

ingestion = BronzeIngestion(spark=spark, dbutils=dbutils)

summary = ingestion.run(
    source_system   = SOURCE_SYSTEM,
    environment     = ENVIRONMENT,
    run_mode        = "full",
    catalog_name    = CATALOG,
    metadata_schema = METADATA_SCHEMA,
    env_config      = env_config,
)

print("\n" + "=" * 65)
print("  BRONZE INGESTION — COMPLETE")
print("=" * 65)
for k, v in summary.items():
    print(f"  {k:<22}: {v}")
print("=" * 65)

if summary.get("failed", 0) > 0:
    raise RuntimeError(
        f"{summary['failed']} dataset(s) failed. "
        f"Check {CATALOG}.{METADATA_SCHEMA}.ingestion_runtime_state"
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5 — Verify Bronze Tables

# COMMAND ----------

print("=" * 65)
print("  BRONZE TABLE VERIFICATION")
print("=" * 65)

# Orders
orders_df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.orders")
print(f"\n── {CATALOG}.{BRONZE_SCHEMA}.orders")
print(f"   Rows: {orders_df.count()}  |  Columns: {len(orders_df.columns)}")
orders_df.select(
    "order_id","customer_id","product_id",
    "order_date","order_status","order_amount",
    "_source_file","_ingestion_date"
).show(5, truncate=True)

# Customers
customers_df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.customers")
print(f"\n── {CATALOG}.{BRONZE_SCHEMA}.customers")
print(f"   Rows: {customers_df.count()}  |  Columns: {len(customers_df.columns)}")
customers_df.select(
    "customer_id","first_name","last_name",
    "city","loyalty_tier","_ingestion_date"
).show(5, truncate=True)

# Products
products_df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.products")
print(f"\n── {CATALOG}.{BRONZE_SCHEMA}.products")
print(f"   Rows: {products_df.count()}  |  Columns: {len(products_df.columns)}")
products_df.select(
    "product_id","product_name","category",
    "unit_price","stock_quantity","_ingestion_date"
).show(5, truncate=True)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Quick Analytics on Bronze Data

# COMMAND ----------

print("── Orders by Status ──────────────────────────────────────────")
spark.sql(f"""
    SELECT order_status,
           COUNT(*)                    AS order_count,
           ROUND(SUM(order_amount), 2) AS total_amount
    FROM {CATALOG}.{BRONZE_SCHEMA}.orders
    GROUP BY order_status
    ORDER BY order_count DESC
""").show()

print("── Top 5 Customers by Spend ──────────────────────────────────")
spark.sql(f"""
    SELECT o.customer_id, c.first_name, c.last_name, c.loyalty_tier,
           COUNT(o.order_id)             AS total_orders,
           ROUND(SUM(o.order_amount), 2) AS total_spent
    FROM {CATALOG}.{BRONZE_SCHEMA}.orders    o
    JOIN {CATALOG}.{BRONZE_SCHEMA}.customers c ON o.customer_id = c.customer_id
    GROUP BY o.customer_id, c.first_name, c.last_name, c.loyalty_tier
    ORDER BY total_spent DESC
    LIMIT 5
""").show()

print("── Revenue by Product Category ──────────────────────────────")
spark.sql(f"""
    SELECT p.category,
           COUNT(o.order_id)             AS order_count,
           ROUND(SUM(o.order_amount), 2) AS total_revenue
    FROM {CATALOG}.{BRONZE_SCHEMA}.orders   o
    JOIN {CATALOG}.{BRONZE_SCHEMA}.products p ON o.product_id = p.product_id
    GROUP BY p.category
    ORDER BY total_revenue DESC
""").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6 — View Metadata Tables

# COMMAND ----------

print("── ingestion_runtime_state ───────────────────────────────────")
spark.sql(f"""
    SELECT r.run_id, d.dataset_name, r.status,
           r.rows_ingested, r.duration_seconds, r.error_message, r.created_at
    FROM {CATALOG}.{METADATA_SCHEMA}.ingestion_runtime_state r
    JOIN {CATALOG}.{METADATA_SCHEMA}.dataset_registry d ON r.dataset_id = d.dataset_id
    ORDER BY r.created_at DESC
""").show(truncate=False)

print("── schema_registry ───────────────────────────────────────────")
spark.sql(f"""
    SELECT d.dataset_name, s.version_number, s.change_type,
           s.is_current, s.registered_at
    FROM {CATALOG}.{METADATA_SCHEMA}.schema_registry s
    JOIN {CATALOG}.{METADATA_SCHEMA}.dataset_registry d ON s.dataset_id = d.dataset_id
    ORDER BY d.dataset_name, s.version_number
""").show(truncate=False)

print("── dq_sla_config (registered rules) ─────────────────────────")
spark.sql(f"""
    SELECT d.dataset_name, q.rule_name, q.rule_type,
           q.column_name, q.threshold_pct, q.fail_on_breach, q.active
    FROM {CATALOG}.{METADATA_SCHEMA}.dq_sla_config q
    JOIN {CATALOG}.{METADATA_SCHEMA}.dataset_registry d ON q.dataset_id = d.dataset_id
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
# MAGIC | orders | `workspace.demo_bronze.orders` | 30 |
# MAGIC | customers | `workspace.demo_bronze.customers` | 10 |
# MAGIC | products | `workspace.demo_bronze.products` | 10 |
# MAGIC
# MAGIC Each Bronze table has raw source columns **plus** these metadata columns:
# MAGIC - `_source_file` — originating file path (UC Volume)
# MAGIC - `_ingestion_date` — date the data was ingested
# MAGIC - `_ingestion_timestamp` — exact timestamp
# MAGIC
# MAGIC ### Tracked automatically
# MAGIC - ✅ `dataset_registry` — dataset identity and classification
# MAGIC - ✅ `ingestion_config` — source path, format, load type
# MAGIC - ✅ `ingestion_runtime_state` — rows ingested, duration, status
# MAGIC - ✅ `schema_registry` — schema version per dataset
# MAGIC - ✅ `dq_sla_config` — DQ rules from YAML config
# MAGIC - ✅ `dataset_tags` — domain, owner, sensitivity
# MAGIC
# MAGIC ### When you get AWS S3
# MAGIC Change `source_type: s3` in `configs/domain_config.yml` — no code changes needed.

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup (Optional — uncomment to reset)

# COMMAND ----------

# for tbl in ["orders","customers","products"]:
#     spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{BRONZE_SCHEMA}.{tbl}")
# for tbl in ["dataset_registry","ingestion_config","ingestion_runtime_state",
#             "schema_registry","dq_sla_config","dataset_tags"]:
#     spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{METADATA_SCHEMA}.{tbl}")
# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{BRONZE_SCHEMA}")
# import shutil
# shutil.rmtree(VOLUME_DATA, ignore_errors=True)
# print("✓ Cleanup complete")
