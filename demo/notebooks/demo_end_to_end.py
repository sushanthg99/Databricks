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

# Repo path — use Repos path which is always readable in notebooks
BUNDLE_PATH = "/Repos/sushanth.gangadhara@blumetra.com/Databricks"

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
# MAGIC %load_ext autoreload

# COMMAND ----------
# MAGIC %autoreload 2

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

# Drop and recreate metadata tables to ensure schema is always up to date
print("\nRecreating metadata tables with latest schema...")
for tbl in ["ingestion_config", "dataset_registry", "dataset_tags",
            "ingestion_runtime_state", "schema_registry", "dq_sla_config"]:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{METADATA_SCHEMA}.{tbl}")
    print(f"  dropped: {tbl}")

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
# MAGIC ## Step 2 — Write TSV Data to Unity Catalog Volume
# MAGIC
# MAGIC Data is embedded directly in the notebook — no file copying needed.
# MAGIC Works on serverless compute (no sparkContext, no DBFS, no file I/O from Repos).

# COMMAND ----------

import os

# Inline TSV data — embedded directly so no file reading is needed
ORDERS_TSV = """order_id\tcustomer_id\tproduct_id\torder_date\torder_timestamp\torder_status\torder_amount\tquantity\tshipping_city\tshipping_country\tpayment_method\tdiscount_pct
1001\tCUST001\tPROD001\t2026-10-01\t2026-10-01 08:15:00\tDELIVERED\t1299.99\t1\tNew York\tUS\tCREDIT_CARD\t5.0
1002\tCUST002\tPROD002\t2026-10-01\t2026-10-01 09:30:00\tDELIVERED\t89.99\t2\tLos Angeles\tUS\tPAYPAL\t0.0
1003\tCUST003\tPROD003\t2026-10-01\t2026-10-01 10:45:00\tSHIPPED\t499.50\t1\tChicago\tUS\tDEBIT_CARD\t10.0
1004\tCUST001\tPROD004\t2026-10-02\t2026-10-02 08:00:00\tCONFIRMED\t24.99\t3\tNew York\tUS\tCREDIT_CARD\t0.0
1005\tCUST004\tPROD001\t2026-10-02\t2026-10-02 11:20:00\tPENDING\t1299.99\t1\tHouston\tUS\tCREDIT_CARD\t0.0
1006\tCUST005\tPROD005\t2026-10-02\t2026-10-02 13:10:00\tDELIVERED\t349.00\t1\tPhoenix\tUS\tPAYPAL\t15.0
1007\tCUST002\tPROD006\t2026-10-03\t2026-10-03 07:45:00\tCANCELLED\t59.99\t1\tLos Angeles\tUS\tDEBIT_CARD\t0.0
1008\tCUST006\tPROD002\t2026-10-03\t2026-10-03 09:00:00\tDELIVERED\t89.99\t4\tPhiladelphia\tUS\tCREDIT_CARD\t5.0
1009\tCUST007\tPROD003\t2026-10-03\t2026-10-03 14:30:00\tSHIPPED\t499.50\t2\tSan Antonio\tUS\tPAYPAL\t0.0
1010\tCUST003\tPROD007\t2026-10-04\t2026-10-04 08:20:00\tDELIVERED\t199.00\t1\tChicago\tUS\tCREDIT_CARD\t0.0
1011\tCUST008\tPROD001\t2026-10-04\t2026-10-04 10:00:00\tCONFIRMED\t1299.99\t1\tSan Diego\tUS\tDEBIT_CARD\t10.0
1012\tCUST009\tPROD004\t2026-10-04\t2026-10-04 11:45:00\tDELIVERED\t24.99\t5\tDallas\tUS\tCREDIT_CARD\t0.0
1013\tCUST010\tPROD005\t2026-10-05\t2026-10-05 09:15:00\tPENDING\t349.00\t1\tSan Jose\tUS\tPAYPAL\t5.0
1014\tCUST004\tPROD008\t2026-10-05\t2026-10-05 10:30:00\tDELIVERED\t74.99\t2\tHouston\tUS\tCREDIT_CARD\t0.0
1015\tCUST005\tPROD009\t2026-10-05\t2026-10-05 13:00:00\tSHIPPED\t899.00\t1\tPhoenix\tUS\tDEBIT_CARD\t0.0
1016\tCUST006\tPROD010\t2026-10-06\t2026-10-06 08:45:00\tDELIVERED\t149.99\t1\tPhiladelphia\tUS\tCREDIT_CARD\t20.0
1017\tCUST001\tPROD006\t2026-10-06\t2026-10-06 09:30:00\tDELIVERED\t59.99\t2\tNew York\tUS\tPAYPAL\t0.0
1018\tCUST007\tPROD001\t2026-10-06\t2026-10-06 14:15:00\tCONFIRMED\t1299.99\t1\tSan Antonio\tUS\tCREDIT_CARD\t5.0
1019\tCUST008\tPROD003\t2026-10-07\t2026-10-07 08:00:00\tDELIVERED\t499.50\t1\tSan Diego\tUS\tDEBIT_CARD\t0.0
1020\tCUST009\tPROD007\t2026-10-07\t2026-10-07 10:45:00\tSHIPPED\t199.00\t3\tDallas\tUS\tCREDIT_CARD\t10.0
1021\tCUST010\tPROD002\t2026-10-07\t2026-10-07 12:00:00\tDELIVERED\t89.99\t1\tSan Jose\tUS\tPAYPAL\t0.0
1022\tCUST002\tPROD009\t2026-10-08\t2026-10-08 09:00:00\tPENDING\t899.00\t1\tLos Angeles\tUS\tCREDIT_CARD\t0.0
1023\tCUST003\tPROD010\t2026-10-08\t2026-10-08 11:30:00\tDELIVERED\t149.99\t2\tChicago\tUS\tDEBIT_CARD\t5.0
1024\tCUST004\tPROD005\t2026-10-08\t2026-10-08 13:45:00\tCONFIRMED\t349.00\t1\tHouston\tUS\tCREDIT_CARD\t0.0
1025\tCUST005\tPROD008\t2026-10-09\t2026-10-09 08:30:00\tDELIVERED\t74.99\t3\tPhoenix\tUS\tPAYPAL\t15.0
1026\tCUST006\tPROD001\t2026-10-09\t2026-10-09 10:00:00\tSHIPPED\t1299.99\t1\tPhiladelphia\tUS\tCREDIT_CARD\t0.0
1027\tCUST007\tPROD004\t2026-10-09\t2026-10-09 14:00:00\tDELIVERED\t24.99\t10\tSan Antonio\tUS\tDEBIT_CARD\t0.0
1028\tCUST008\tPROD006\t2026-10-10\t2026-10-10 08:15:00\tDELIVERED\t59.99\t1\tSan Diego\tUS\tCREDIT_CARD\t5.0
1029\tCUST009\tPROD002\t2026-10-10\t2026-10-10 09:45:00\tCANCELLED\t89.99\t2\tDallas\tUS\tPAYPAL\t0.0
1030\tCUST010\tPROD003\t2026-10-10\t2026-10-10 11:30:00\tDELIVERED\t499.50\t1\tSan Jose\tUS\tCREDIT_CARD\t10.0"""

CUSTOMERS_TSV = """customer_id\tfirst_name\tlast_name\temail\tphone\tdate_of_birth\tgender\tcity\tstate\tcountry\tpostal_code\tregistration_date\tloyalty_tier\ttotal_lifetime_value\tis_active
CUST001\tJames\tAnderson\tjames.anderson@email.com\t+1-212-555-0101\t1985-03-15\tM\tNew York\tNY\tUS\t10001\t2023-01-10\tGOLD\t8750.50\ttrue
CUST002\tSarah\tMartinez\tsarah.martinez@email.com\t+1-213-555-0102\t1990-07-22\tF\tLos Angeles\tCA\tUS\t90001\t2023-02-14\tSILVER\t3200.75\ttrue
CUST003\tMichael\tJohnson\tmichael.johnson@email.com\t+1-312-555-0103\t1978-11-08\tM\tChicago\tIL\tUS\t60601\t2022-11-05\tGOLD\t12400.00\ttrue
CUST004\tEmily\tWilliams\temily.williams@email.com\t+1-713-555-0104\t1995-04-30\tF\tHouston\tTX\tUS\t77001\t2023-06-20\tBRONZE\t980.25\ttrue
CUST005\tDavid\tBrown\tdavid.brown@email.com\t+1-602-555-0105\t1982-09-12\tM\tPhoenix\tAZ\tUS\t85001\t2023-03-08\tSILVER\t4560.00\ttrue
CUST006\tLisa\tDavis\tlisa.davis@email.com\t+1-215-555-0106\t1988-12-25\tF\tPhiladelphia\tPA\tUS\t19101\t2022-09-17\tGOLD\t9100.80\ttrue
CUST007\tRobert\tWilson\trobert.wilson@email.com\t+1-210-555-0107\t1975-06-18\tM\tSan Antonio\tTX\tUS\t78201\t2023-01-28\tBRONZE\t1250.40\ttrue
CUST008\tJennifer\tMoore\tjennifer.moore@email.com\t+1-619-555-0108\t1992-02-14\tF\tSan Diego\tCA\tUS\t92101\t2023-04-12\tSILVER\t5670.90\ttrue
CUST009\tWilliam\tTaylor\twilliam.taylor@email.com\t+1-214-555-0109\t1980-08-05\tM\tDallas\tTX\tUS\t75201\t2022-12-01\tGOLD\t15800.25\ttrue
CUST010\tAmanda\tThomas\tamanda.thomas@email.com\t+1-408-555-0110\t1997-01-19\tF\tSan Jose\tCA\tUS\t95101\t2023-07-04\tBRONZE\t450.00\ttrue"""

PRODUCTS_TSV = """product_id\tproduct_name\tcategory\tsub_category\tbrand\tsku\tunit_price\tcost_price\tstock_quantity\treorder_level\tsupplier_id\tis_active\tcreated_date\tlast_updated
PROD001\tMacBook Pro 14\tElectronics\tLaptops\tApple\tAPL-MBP14-2026\t1299.99\t950.00\t45\t10\tSUP001\ttrue\t2025-01-01\t2026-10-01
PROD002\tWireless Headphones\tElectronics\tAudio\tSony\tSNY-WH1000XM6\t89.99\t45.00\t200\t50\tSUP002\ttrue\t2025-03-15\t2026-09-15
PROD003\tStanding Desk\tFurniture\tOffice\tFlexiDesk\tFD-SD-001\t499.50\t280.00\t30\t8\tSUP003\ttrue\t2025-02-10\t2026-08-20
PROD004\tMechanical Keyboard\tElectronics\tPeripherals\tLogitech\tLGT-MX-KEYS\t24.99\t12.00\t500\t100\tSUP002\ttrue\t2025-04-05\t2026-10-01
PROD005\tErgonomic Chair\tFurniture\tOffice\tHermanMiller\tHM-AERON-B\t349.00\t200.00\t25\t5\tSUP003\ttrue\t2025-01-20\t2026-07-10
PROD006\tUSB-C Hub\tElectronics\tAccessories\tAnker\tANK-A8352\t59.99\t25.00\t350\t80\tSUP004\ttrue\t2025-05-12\t2026-09-01
PROD007\tMonitor 27 4K\tElectronics\tDisplays\tLG\tLG-27UK850\t199.00\t120.00\t60\t15\tSUP002\ttrue\t2025-03-01\t2026-10-01
PROD008\tDesk Lamp LED\tHome\tLighting\tPhilips\tPHL-DL-LED\t74.99\t35.00\t150\t40\tSUP005\ttrue\t2025-06-08\t2026-09-20
PROD009\tNoise Cancelling Earbuds\tElectronics\tAudio\tBose\tBSE-QC-45\t899.00\t550.00\t80\t20\tSUP001\ttrue\t2025-02-28\t2026-10-01
PROD010\tWebcam HD 1080p\tElectronics\tPeripherals\tLogitech\tLGT-C920\t149.99\t70.00\t120\t30\tSUP002\ttrue\t2025-07-15\t2026-09-15"""

# Write inline data directly to UC Volume using Python open()
# UC Volumes support standard Python file I/O — no Spark/DBFS needed
print("Writing TSV data to UC Volume...")
print("-" * 60)

for volume_dir, filename, data in [
    (VOLUME_ORDERS,    "orders.tsv",    ORDERS_TSV),
    (VOLUME_CUSTOMERS, "customers.tsv", CUSTOMERS_TSV),
    (VOLUME_PRODUCTS,  "products.tsv",  PRODUCTS_TSV),
]:
    os.makedirs(volume_dir, exist_ok=True)
    target = f"{volume_dir}/{filename}"
    with open(target, "w", encoding="utf-8") as f:
        f.write(data.strip())
    size = os.path.getsize(target)
    print(f"  ✓ {filename}  →  {target}  ({size:,} bytes)")

print("-" * 60)
print("\n✓ All TSV data written to UC Volume")

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

import yaml
from ingestion_framework.metadata_service.metadata_loader import MetadataLoader

# Embed demo config inline — avoids Path.exists() issues on serverless Repos
DEMO_CONFIG_YAML = f"""
source_system: ecommerce
environment: dev

datasets:
  - dataset_name: orders
    version: "1.0"
    dataset_type: transactional
    data_tier: bronze
    business_description: Customer order transactions demo data.
    criticality: high
    classification: internal
    pii: false
    active: true
    source_type: volume
    volume_path: {VOLUME_ORDERS}
    file_pattern: orders.tsv
    format: tsv
    delimiter: "\\t"
    encoding: UTF-8
    header: true
    ingestion_mode: batch
    load_type: full
    target_table: {CATALOG}.{BRONZE_SCHEMA}.orders
    schema_location: {VOLUME_BASE}/schemas/orders/
    checkpoint_location: {VOLUME_BASE}/checkpoints/orders/
    primary_keys: [order_id]
    watermark:
      column: order_timestamp
      delay: "2 hours"
    frequency: daily
    tags:
      domain: ecommerce
      owner: data-engineering-team
      retention_days: "365"
      sensitivity: standard
      demo: "true"
    data_quality:
      - rule_name: order_id_not_null
        rule_type: not_null
        column_name: order_id
        threshold_pct: 100.0
        fail_on_breach: true
      - rule_name: order_amount_positive
        rule_type: range
        column_name: order_amount
        rule_expression: "order_amount > 0"
        threshold_pct: 100.0
        fail_on_breach: true

  - dataset_name: customers
    version: "1.0"
    dataset_type: reference
    data_tier: bronze
    business_description: Customer master data demo.
    criticality: high
    classification: confidential
    pii: true
    active: true
    source_type: volume
    volume_path: {VOLUME_CUSTOMERS}
    file_pattern: customers.tsv
    format: tsv
    delimiter: "\\t"
    encoding: UTF-8
    header: true
    ingestion_mode: batch
    load_type: full
    target_table: {CATALOG}.{BRONZE_SCHEMA}.customers
    schema_location: {VOLUME_BASE}/schemas/customers/
    checkpoint_location: {VOLUME_BASE}/checkpoints/customers/
    primary_keys: [customer_id]
    watermark:
      column: registration_date
      delay: "0 hours"
    frequency: daily
    tags:
      domain: ecommerce
      owner: data-engineering-team
      retention_days: "730"
      sensitivity: pii
      demo: "true"
    data_quality:
      - rule_name: customer_id_not_null
        rule_type: not_null
        column_name: customer_id
        threshold_pct: 100.0
        fail_on_breach: true

  - dataset_name: products
    version: "1.0"
    dataset_type: reference
    data_tier: bronze
    business_description: Product catalogue demo data.
    criticality: medium
    classification: internal
    pii: false
    active: true
    source_type: volume
    volume_path: {VOLUME_PRODUCTS}
    file_pattern: products.tsv
    format: tsv
    delimiter: "\\t"
    encoding: UTF-8
    header: true
    ingestion_mode: batch
    load_type: full
    target_table: {CATALOG}.{BRONZE_SCHEMA}.products
    schema_location: {VOLUME_BASE}/schemas/products/
    checkpoint_location: {VOLUME_BASE}/checkpoints/products/
    primary_keys: [product_id]
    watermark:
      column: last_updated
      delay: "1 hour"
    frequency: daily
    tags:
      domain: ecommerce
      owner: product-team
      retention_days: "365"
      sensitivity: standard
      demo: "true"
    data_quality:
      - rule_name: product_id_not_null
        rule_type: not_null
        column_name: product_id
        threshold_pct: 100.0
        fail_on_breach: true
"""

print(f"Registering metadata into : {CATALOG}.{METADATA_SCHEMA}")
print("-" * 60)

loader = MetadataLoader(
    spark=spark,
    catalog=CATALOG,
    metadata_schema=METADATA_SCHEMA,
    environment=ENVIRONMENT,
)

# Parse inline YAML and load directly — no file path needed
raw = yaml.safe_load(DEMO_CONFIG_YAML)
result = loader.load_from_list(
    raw["datasets"],
    source_system=raw["source_system"]
)

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

# Verify ingestion_config — confirm source_type=volume is stored correctly
print("── ingestion_config ───────────────────────────────────────────")
spark.sql(f"""
    SELECT dataset_id, environment, source_type, load_type,
           format, target_table, volume_path
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
# MAGIC 4. Writes run metrics to `ingestion_runtime_state`

# COMMAND ----------

import time, sys, uuid
from datetime import datetime, timezone

if BUNDLE_PATH not in sys.path:
    sys.path.insert(0, BUNDLE_PATH)

from ingestion_framework.ingestion_engine.autoloader_engine import AutoLoaderEngine

env_config = {
    "environment": ENVIRONMENT,
    "unity_catalog": {"catalog_name": CATALOG, "metadata_schema": METADATA_SCHEMA},
    "ingestion": {"schema_evolution_mode": "addNewColumns", "max_files_per_trigger": 1000},
}

# Hardcoded dataset configs — bypass metadata table lookup entirely
# This avoids the MERGE delimiter bug and ensures source_type=volume is always correct
DATASET_CONFIGS = [
    {
        "dataset_name":       "orders",
        "dataset_id":         "df2d49d7-8f84-5945-a618-02b79ff24375",
        "source_system":      "ecommerce",
        "data_tier":          "bronze",
        "format":             "tsv",
        "delimiter":          "\t",
        "header":             True,
        "encoding":           "UTF-8",
        "load_type":          "full",
        "source_type":        "volume",
        "volume_path":        VOLUME_ORDERS,
        "target_table":       f"{CATALOG}.{BRONZE_SCHEMA}.orders",
        "schema_location":    f"{VOLUME_BASE}/schemas/orders/",
        "checkpoint_location":f"{VOLUME_BASE}/checkpoints/orders/",
    },
    {
        "dataset_name":       "customers",
        "dataset_id":         "bcfd4686-4474-55c4-8687-6a17d9d632db",
        "source_system":      "ecommerce",
        "data_tier":          "bronze",
        "format":             "tsv",
        "delimiter":          "\t",
        "header":             True,
        "encoding":           "UTF-8",
        "load_type":          "full",
        "source_type":        "volume",
        "volume_path":        VOLUME_CUSTOMERS,
        "target_table":       f"{CATALOG}.{BRONZE_SCHEMA}.customers",
        "schema_location":    f"{VOLUME_BASE}/schemas/customers/",
        "checkpoint_location":f"{VOLUME_BASE}/checkpoints/customers/",
    },
    {
        "dataset_name":       "products",
        "dataset_id":         "58657fc6-5d89-58ba-aadf-4febd7cdf616",
        "source_system":      "ecommerce",
        "data_tier":          "bronze",
        "format":             "tsv",
        "delimiter":          "\t",
        "header":             True,
        "encoding":           "UTF-8",
        "load_type":          "full",
        "source_type":        "volume",
        "volume_path":        VOLUME_PRODUCTS,
        "target_table":       f"{CATALOG}.{BRONZE_SCHEMA}.products",
        "schema_location":    f"{VOLUME_BASE}/schemas/products/",
        "checkpoint_location":f"{VOLUME_BASE}/checkpoints/products/",
    },
]

print("=" * 65)
print("  BRONZE INGESTION — STARTING")
print("=" * 65)
engine = AutoLoaderEngine(spark=spark, env_config=env_config)
registry = SchemaRegistry(spark=spark, catalog=CATALOG, metadata_schema=METADATA_SCHEMA)

run_id   = str(uuid.uuid4())
succeeded = failed = 0

for cfg in DATASET_CONFIGS:
    dataset_name = cfg["dataset_name"]
    start = time.time()
    print(f"\n  Ingesting: {dataset_name} from {cfg['volume_path']}")
    result = engine.ingest(cfg)
    duration = round(time.time() - start, 2)
    print(f"  Status: {result['status']} | Rows: {result.get('rows_ingested',0)} | {duration}s")

    # Write runtime state
    now = datetime.now(timezone.utc).isoformat()
    rows = result.get("rows_ingested", 0)
    err  = (result.get("error_message") or "").replace("'","''")[:1000]
    try:
        spark.sql(f"""
            INSERT INTO {CATALOG}.{METADATA_SCHEMA}.ingestion_runtime_state
            VALUES ('{run_id}','{cfg["dataset_id"]}','{ENVIRONMENT}','bronze',
                    '{now}','{now}',{duration},'{result["status"]}',
                    {rows},0,0,'{err}','{now}')
        """)
    except Exception as e:
        print(f"  Warning: runtime state write failed: {e}")

    if result["status"] == "SUCCESS":
        succeeded += 1
    else:
        failed += 1
        print(f"  ERROR: {result.get('error_message','')}")

print(f"\n{'='*65}")
print(f"  DONE — Succeeded: {succeeded} | Failed: {failed}")
print(f"{'='*65}")

if failed > 0:
    raise RuntimeError(f"{failed} dataset(s) failed — see errors above")

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
