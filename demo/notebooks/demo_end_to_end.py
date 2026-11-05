# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Dynamic Databricks Ingestion Framework — End-to-End Demo
# MAGIC
# MAGIC **What this demo shows:**
# MAGIC 1. Upload real TSV data files to DBFS (no S3 / AWS needed)
# MAGIC 2. Create metadata tables in Unity Catalog / Hive Metastore
# MAGIC 3. Register dataset configuration via the Metadata Loader (YAML → Delta)
# MAGIC 4. Run Bronze ingestion — raw TSV files → Delta Bronze tables
# MAGIC 5. Verify ingested data with row counts and sample queries
# MAGIC 6. Check schema registry and runtime state tracking
# MAGIC
# MAGIC **Datasets:**
# MAGIC | Dataset | Rows | Format | Load Type |
# MAGIC |---|---|---|---|
# MAGIC | orders | 20 | TSV | Incremental (Auto Loader) |
# MAGIC | customers | 16 | TSV | Full (batch overwrite) |
# MAGIC | products | 8 | TSV | Full (batch overwrite) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Step 0 — Configuration

# COMMAND ----------

# ── Demo settings ─────────────────────────────────────────────────────────────
CATALOG         = "hive_metastore"   # Change to your UC catalog if available
METADATA_SCHEMA = "ingestion_metadata"
BRONZE_SCHEMA   = "ecommerce_bronze"
ENVIRONMENT     = "demo"
SOURCE_SYSTEM   = "ecommerce"

# DBFS paths — files are uploaded here in Step 1
DBFS_BASE       = "dbfs:/FileStore/demo/ecommerce"
DBFS_ORDERS     = f"{DBFS_BASE}/orders"
DBFS_CUSTOMERS  = f"{DBFS_BASE}/customers"
DBFS_PRODUCTS   = f"{DBFS_BASE}/products"

# Bundle files path — adjust if your workspace path differs
BUNDLE_FILES_PATH = (
    f"/Workspace/Users/sushanth.gangadhara@blumetra.com"
    f"/.bundle/dynamic-databricks-ingestion/{ENVIRONMENT}/files"
)

print("=" * 60)
print("  DEMO CONFIGURATION")
print("=" * 60)
print(f"  catalog         : {CATALOG}")
print(f"  metadata_schema : {METADATA_SCHEMA}")
print(f"  bronze_schema   : {BRONZE_SCHEMA}")
print(f"  environment     : {ENVIRONMENT}")
print(f"  bundle_path     : {BUNDLE_FILES_PATH}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📁 Step 1 — Upload TSV files to DBFS
# MAGIC
# MAGIC Copies the sample TSV files from the bundle into DBFS so the
# MAGIC ingestion engine can read them as if they were landing files.

# COMMAND ----------

import os

# Create DBFS directories
for path in [DBFS_ORDERS, DBFS_CUSTOMERS, DBFS_PRODUCTS]:
    dbutils.fs.mkdirs(path)
    print(f"✅ Created DBFS directory: {path}")

# Source file paths in the bundle
data_dir = f"{BUNDLE_FILES_PATH}/demo/data"

# Copy TSV files to DBFS landing zones
file_copies = [
    (f"{data_dir}/orders.tsv",    f"{DBFS_ORDERS}/orders.tsv"),
    (f"{data_dir}/customers.tsv", f"{DBFS_CUSTOMERS}/customers.tsv"),
    (f"{data_dir}/products.tsv",  f"{DBFS_PRODUCTS}/products.tsv"),
]

for src, dst in file_copies:
    dbutils.fs.cp(src, dst, recurse=False)
    print(f"✅ Copied: {src.split('/')[-1]} → {dst}")

print("\n📂 DBFS landing zone contents:")
for path in [DBFS_ORDERS, DBFS_CUSTOMERS, DBFS_PRODUCTS]:
    files = dbutils.fs.ls(path)
    for f in files:
        print(f"   {f.path}  ({f.size:,} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗄️ Step 2 — Create Metadata Tables
# MAGIC
# MAGIC Creates all 7 framework metadata tables in `hive_metastore.ingestion_metadata`.
# MAGIC Safe to re-run — uses `CREATE TABLE IF NOT EXISTS`.

# COMMAND ----------

# Create the metadata schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}")
print(f"✅ Schema created: {CATALOG}.{METADATA_SCHEMA}")

# Create Bronze schema for demo output
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
print(f"✅ Schema created: {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# --- TABLE 1: dataset_registry ---
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}.dataset_registry (
    dataset_id           STRING    NOT NULL,
    dataset_name         STRING    NOT NULL,
    version              STRING    NOT NULL,
    dataset_type         STRING,
    data_tier            STRING    NOT NULL,
    source_system        STRING    NOT NULL,
    business_description STRING,
    criticality          STRING,
    classification       STRING,
    pii                  BOOLEAN,
    active               BOOLEAN   NOT NULL,
    created_at           TIMESTAMP NOT NULL,
    updated_at           TIMESTAMP,
    created_by           STRING
) USING DELTA
COMMENT 'Master registry of all datasets managed by the framework'
""")
print("✅ dataset_registry")

# --- TABLE 2: dataset_tags ---
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}.dataset_tags (
    dataset_id  STRING    NOT NULL,
    tag_key     STRING    NOT NULL,
    tag_value   STRING,
    created_at  TIMESTAMP NOT NULL
) USING DELTA
COMMENT 'Key-value tags per dataset'
""")
print("✅ dataset_tags")

# --- TABLE 3: dataset_dependencies ---
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}.dataset_dependencies (
    dependency_id         STRING    NOT NULL,
    upstream_dataset_id   STRING    NOT NULL,
    downstream_dataset_id STRING    NOT NULL,
    dependency_type       STRING,
    created_at            TIMESTAMP NOT NULL
) USING DELTA
COMMENT 'Dataset dependency graph'
""")
print("✅ dataset_dependencies")

# --- TABLE 4: ingestion_config ---
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}.ingestion_config (
    config_id            STRING    NOT NULL,
    dataset_id           STRING    NOT NULL,
    environment          STRING    NOT NULL,
    ingestion_mode       STRING,
    load_type            STRING,
    s3_bucket            STRING,
    s3_path              STRING,
    file_pattern         STRING,
    format               STRING    NOT NULL,
    delimiter            STRING,
    encoding             STRING,
    header               BOOLEAN,
    multiline            BOOLEAN,
    schema_location      STRING,
    checkpoint_location  STRING,
    primary_keys         STRING,
    watermark_column     STRING,
    watermark_delay      STRING,
    frequency            STRING,
    target_table         STRING,
    config_hash          STRING,
    created_at           TIMESTAMP NOT NULL,
    updated_at           TIMESTAMP
) USING DELTA
COMMENT 'Per-dataset ingestion configuration'
""")
print("✅ ingestion_config")

# --- TABLE 5: ingestion_runtime_state ---
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}.ingestion_runtime_state (
    run_id             STRING    NOT NULL,
    dataset_id         STRING    NOT NULL,
    environment        STRING    NOT NULL,
    layer              STRING    NOT NULL,
    start_time         TIMESTAMP,
    end_time           TIMESTAMP,
    duration_seconds   DOUBLE,
    status             STRING    NOT NULL,
    rows_ingested      LONG,
    rows_failed_dq     LONG,
    source_file_count  LONG,
    error_message      STRING,
    created_at         TIMESTAMP NOT NULL
) USING DELTA
PARTITIONED BY (layer)
COMMENT 'Ingestion run history — append only'
""")
print("✅ ingestion_runtime_state")

# --- TABLE 6: dq_sla_config ---
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}.dq_sla_config (
    dq_rule_id        STRING         NOT NULL,
    dataset_id        STRING         NOT NULL,
    rule_name         STRING         NOT NULL,
    rule_type         STRING         NOT NULL,
    column_name       STRING,
    rule_expression   STRING,
    threshold_pct     DECIMAL(5,2),
    fail_on_breach    BOOLEAN,
    sla_type          STRING,
    sla_deadline      STRING,
    last_run_pass_pct DECIMAL(5,2),
    last_run_at       TIMESTAMP,
    active            BOOLEAN        NOT NULL,
    created_at        TIMESTAMP      NOT NULL
) USING DELTA
COMMENT 'Data quality rule definitions'
""")
print("✅ dq_sla_config")

# --- TABLE 7: schema_registry ---
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{METADATA_SCHEMA}.schema_registry (
    schema_version_id  STRING    NOT NULL,
    dataset_id         STRING    NOT NULL,
    version_number     INT       NOT NULL,
    schema_json        STRING    NOT NULL,
    change_type        STRING,
    changed_columns    ARRAY<STRING>,
    is_current         BOOLEAN   NOT NULL,
    registered_at      TIMESTAMP NOT NULL,
    registered_by      STRING
) USING DELTA
COMMENT 'Versioned schema history per dataset'
""")
print("✅ schema_registry")

print("\n🎉 All 7 metadata tables created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🐍 Step 3 — Add framework to Python path

# COMMAND ----------

import sys

if BUNDLE_FILES_PATH not in sys.path:
    sys.path.insert(0, BUNDLE_FILES_PATH)
    print(f"✅ Added to sys.path: {BUNDLE_FILES_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Step 4 — Register Dataset Metadata (YAML → Delta)
# MAGIC
# MAGIC The MetadataLoader reads `demo_config.yml`, computes SHA-256 hashes,
# MAGIC and MERGEs dataset configurations into the metadata tables.

# COMMAND ----------

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")

from ingestion_framework.metadata_service.metadata_loader import MetadataLoader

loader = MetadataLoader(
    spark           = spark,
    catalog         = CATALOG,
    metadata_schema = METADATA_SCHEMA,
    environment     = ENVIRONMENT,
)

demo_config_path = f"{BUNDLE_FILES_PATH}/demo/demo_config.yml"
result = loader.load_from_file(demo_config_path)

print("\n" + "=" * 50)
print("  METADATA REGISTRATION RESULT")
print("=" * 50)
print(f"  Total datasets : {result['total']}")
print(f"  Registered     : {result['registered']}")
print(f"  Skipped        : {result['skipped']}  (unchanged configs)")
print(f"  Failed         : {result['failed']}")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Verify — dataset_registry

# COMMAND ----------

display(spark.sql(f"""
    SELECT dataset_name, version, data_tier, source_system,
           criticality, pii, active, created_at
    FROM {CATALOG}.{METADATA_SCHEMA}.dataset_registry
    ORDER BY dataset_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Verify — ingestion_config

# COMMAND ----------

display(spark.sql(f"""
    SELECT dataset_id, environment, ingestion_mode, load_type,
           format, delimiter, target_table, config_hash
    FROM {CATALOG}.{METADATA_SCHEMA}.ingestion_config
    ORDER BY target_table
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Verify — dq_sla_config

# COMMAND ----------

display(spark.sql(f"""
    SELECT dr.dataset_name, dq.rule_name, dq.rule_type,
           dq.column_name, dq.threshold_pct, dq.fail_on_breach
    FROM {CATALOG}.{METADATA_SCHEMA}.dq_sla_config dq
    JOIN {CATALOG}.{METADATA_SCHEMA}.dataset_registry dr
      ON dq.dataset_id = dr.dataset_id
    ORDER BY dr.dataset_name, dq.rule_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Step 5 — Run Bronze Ingestion
# MAGIC
# MAGIC The BronzeIngestion engine:
# MAGIC 1. Queries active datasets from the metadata tables
# MAGIC 2. For each dataset — reads TSV from DBFS and writes to Delta Bronze
# MAGIC 3. Records run metrics in ingestion_runtime_state

# COMMAND ----------

from src.bronze.bronze_ingestion import BronzeIngestion

# Build env_config manually for demo (no S3, no external environment YAML needed)
env_config = {
    "environment": ENVIRONMENT,
    "unity_catalog": {
        "catalog_name":    CATALOG,
        "metadata_schema": METADATA_SCHEMA,
    },
    "ingestion": {
        "schema_evolution_mode": "addNewColumns",
        "max_files_per_trigger": 1000,
        "rescue_data_column":    True,
    },
    "data_quality": {
        "fail_on_breach_override": False,  # Demo: never abort on DQ failures
    },
}

ingestion = BronzeIngestion(spark=spark, dbutils=dbutils)

print("🚀 Starting Bronze ingestion...")
summary = ingestion.run(
    source_system   = SOURCE_SYSTEM,
    environment     = ENVIRONMENT,
    run_mode        = "incremental",
    catalog_name    = CATALOG,
    metadata_schema = METADATA_SCHEMA,
    env_config      = env_config,
)

print("\n" + "=" * 50)
print("  BRONZE INGESTION SUMMARY")
print("=" * 50)
print(f"  Total datasets : {summary['total']}")
print(f"  Succeeded      : {summary['succeeded']}")
print(f"  Failed         : {summary['failed']}")
print(f"  Skipped        : {summary['skipped']}")
print(f"  Duration       : {summary['duration_seconds']}s")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Step 6 — Verify Bronze Tables
# MAGIC
# MAGIC Check row counts and sample data from each ingested Delta table.

# COMMAND ----------

print("📋 Bronze Table Row Counts:")
print("-" * 45)
for table in ["orders", "customers", "products"]:
    full_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table}"
    try:
        count = spark.table(full_table).count()
        print(f"  {full_table:<45} → {count:>4} rows")
    except Exception as e:
        print(f"  {full_table} → ERROR: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📦 Bronze Orders — Sample Data

# COMMAND ----------

display(spark.sql(f"""
    SELECT order_id, customer_id, product_id, order_date,
           order_status, order_amount, currency,
           _source_file, _ingestion_date
    FROM {CATALOG}.{BRONZE_SCHEMA}.orders
    ORDER BY order_id
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 👤 Bronze Customers — Sample Data

# COMMAND ----------

display(spark.sql(f"""
    SELECT customer_id, first_name, last_name, city, state,
           loyalty_tier, total_orders, total_spent,
           _ingestion_timestamp
    FROM {CATALOG}.{BRONZE_SCHEMA}.customers
    ORDER BY customer_id
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🛍️ Bronze Products — Sample Data

# COMMAND ----------

display(spark.sql(f"""
    SELECT product_id, product_name, category, unit_price,
           stock_quantity, brand, _ingestion_date
    FROM {CATALOG}.{BRONZE_SCHEMA}.products
    ORDER BY product_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Step 7 — Business Insights from Bronze Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revenue by Order Status

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        order_status,
        COUNT(*)                        AS order_count,
        ROUND(SUM(order_amount), 2)     AS total_revenue,
        ROUND(AVG(order_amount), 2)     AS avg_order_value
    FROM {CATALOG}.{BRONZE_SCHEMA}.orders
    GROUP BY order_status
    ORDER BY total_revenue DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top Customers by Spend

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        c.customer_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        c.loyalty_tier,
        c.city,
        COUNT(o.order_id)               AS orders_placed,
        ROUND(SUM(o.order_amount), 2)   AS total_spent
    FROM {CATALOG}.{BRONZE_SCHEMA}.customers c
    LEFT JOIN {CATALOG}.{BRONZE_SCHEMA}.orders o
           ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, customer_name, c.loyalty_tier, c.city
    ORDER BY total_spent DESC
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revenue by Product Category

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        p.category,
        COUNT(o.order_id)               AS orders,
        ROUND(SUM(o.order_amount), 2)   AS total_revenue,
        ROUND(AVG(o.order_amount), 2)   AS avg_order_value
    FROM {CATALOG}.{BRONZE_SCHEMA}.orders o
    JOIN {CATALOG}.{BRONZE_SCHEMA}.products p
      ON o.product_id = p.product_id
    GROUP BY p.category
    ORDER BY total_revenue DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Step 8 — Framework Observability
# MAGIC
# MAGIC Every ingestion run is tracked in the metadata tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestion Runtime State — Run History

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        r.run_id,
        d.dataset_name,
        r.layer,
        r.status,
        r.rows_ingested,
        r.duration_seconds,
        r.error_message,
        r.created_at
    FROM {CATALOG}.{METADATA_SCHEMA}.ingestion_runtime_state r
    JOIN {CATALOG}.{METADATA_SCHEMA}.dataset_registry d
      ON r.dataset_id = d.dataset_id
    ORDER BY r.created_at DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Registry — Registered Schema Versions

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        d.dataset_name,
        s.version_number,
        s.change_type,
        s.is_current,
        s.registered_at,
        s.registered_by
    FROM {CATALOG}.{METADATA_SCHEMA}.schema_registry s
    JOIN {CATALOG}.{METADATA_SCHEMA}.dataset_registry d
      ON s.dataset_id = d.dataset_id
    ORDER BY d.dataset_name, s.version_number
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔁 Step 9 — Simulate a Second Run (Incremental)
# MAGIC
# MAGIC Running ingestion again demonstrates:
# MAGIC - **Metadata deduplication**: unchanged YAML config is skipped (hash match)
# MAGIC - **Auto Loader**: only new files since last checkpoint are processed
# MAGIC - **Runtime state**: second run appended as a new row

# COMMAND ----------

print("🔁 Running ingestion again (simulating incremental run)...")

summary2 = ingestion.run(
    source_system   = SOURCE_SYSTEM,
    environment     = ENVIRONMENT,
    run_mode        = "incremental",
    catalog_name    = CATALOG,
    metadata_schema = METADATA_SCHEMA,
    env_config      = env_config,
)

print("\n" + "=" * 50)
print("  SECOND RUN SUMMARY")
print("=" * 50)
for k, v in summary2.items():
    print(f"  {k:<22}: {v}")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Confirm: Two run records in ingestion_runtime_state

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        d.dataset_name,
        r.status,
        r.rows_ingested,
        r.duration_seconds,
        r.created_at
    FROM {CATALOG}.{METADATA_SCHEMA}.ingestion_runtime_state r
    JOIN {CATALOG}.{METADATA_SCHEMA}.dataset_registry d
      ON r.dataset_id = d.dataset_id
    ORDER BY r.created_at DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Step 10 — Optional Cleanup
# MAGIC
# MAGIC Run this cell only if you want to reset the demo and start fresh.
# MAGIC **Skip this during the actual demo presentation.**

# COMMAND ----------

# UNCOMMENT to reset demo
# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{METADATA_SCHEMA} CASCADE")
# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{BRONZE_SCHEMA} CASCADE")
# dbutils.fs.rm("dbfs:/FileStore/demo", recurse=True)
# dbutils.fs.rm("dbfs:/tmp/demo", recurse=True)
# print("✅ Demo environment cleaned up.")

print("ℹ️  Cleanup skipped — comment out the lines above and re-run to reset.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Demo Complete!
# MAGIC
# MAGIC ### What we demonstrated:
# MAGIC
# MAGIC | Step | What happened |
# MAGIC |---|---|
# MAGIC | Upload TSV files | 3 TSV files landed in DBFS (simulating S3) |
# MAGIC | Create metadata tables | 7 Delta tables created in `ingestion_metadata` |
# MAGIC | Register config | YAML → SHA-256 hash → MERGE INTO metadata tables |
# MAGIC | Bronze ingestion | TSV → Delta Bronze via Auto Loader / batch |
# MAGIC | Business insights | Revenue, top customers, category analysis |
# MAGIC | Observability | Runtime state, schema registry, DQ rules |
# MAGIC | Second run | Incremental deduplication demonstrated |
# MAGIC
# MAGIC ### Key framework capabilities shown:
# MAGIC - ✅ **Metadata-driven** — no hardcoded dataset logic
# MAGIC - ✅ **Hash-based dedup** — unchanged configs never write
# MAGIC - ✅ **Schema registry** — every schema change versioned
# MAGIC - ✅ **Runtime tracking** — every run logged with metrics
# MAGIC - ✅ **Multi-format** — TSV/CSV/JSON/Parquet supported
# MAGIC - ✅ **Environment config** — dev/uat/prod separation
# MAGIC - ✅ **DAB deployment** — `databricks bundle deploy` ready
