# Project Flow — Dynamic Databricks Ingestion Framework

## Overview

This document describes the complete end-to-end flow of the framework —
from a developer writing a YAML config file all the way to business-ready
Gold data available in Unity Catalog for analytics and reporting.

---

## 1. Big Picture Flow

```
Developer writes YAML config
         │
         ▼
  Metadata Loader registers config
  into Unity Catalog metadata tables
         │
         ▼
  Databricks Job triggered
  (scheduled / manual / event)
         │
         ▼
┌────────────────────────────────────────────────────┐
│                  BRONZE LAYER                      │
│  Auto Loader / Batch reads files from AWS S3       │
│  Writes raw data → Delta Bronze tables             │
│  Schema Registry records schema version            │
│  Runtime State updated (rows, status, duration)    │
└────────────────────────────────────────────────────┘
         │
         ▼
┌────────────────────────────────────────────────────┐
│                  SILVER LAYER                      │
│  Reads Bronze Delta tables                         │
│  Applies type casts, null handling, deduplication  │
│  Runs Data Quality checks                          │
│  Writes cleansed data → Delta Silver tables        │
│  Quarantines DQ-failing rows                       │
└────────────────────────────────────────────────────┘
         │
         ▼
┌────────────────────────────────────────────────────┐
│                   GOLD LAYER                       │
│  Reads Silver Delta tables                         │
│  Applies business aggregations and joins           │
│  Writes analytics-ready → Delta Gold tables        │
│  Available to BI tools / dashboards / ML           │
└────────────────────────────────────────────────────┘
```

---

## 2. Detailed Step-by-Step Flow

---

### PHASE 0 — Developer Configures a Dataset

**Who does this:** Data Engineer  
**When:** Once per new dataset, or when config changes  
**Files involved:** `configs/domain_config.yml`

```
Step 1: Developer adds a dataset block to domain_config.yml

        dataset_name: orders
        format: csv
        load_type: incremental
        s3_bucket: my-bucket
        s3_path: raw/ecommerce/orders/
        primary_keys: [order_id]
        data_quality:
          - rule_name: order_id_not_null
            rule_type: not_null
            ...

Step 2: Developer commits and pushes to GitHub
        git add configs/domain_config.yml
        git commit -m "feat: add orders dataset config"
        git push

Step 3: Developer deploys updated bundle
        databricks bundle deploy --target dev --profile dev
```

---

### PHASE 1 — Metadata Registration

**Who does this:** Automated (job step or manual run)  
**When:** Before every ingestion run  
**Files involved:**
- `ingestion_framework/metadata_service/metadata_loader.py`
- Unity Catalog metadata tables

```
Step 1: MetadataLoader reads domain_config.yml
        └── Parses all dataset blocks

Step 2: For each dataset:
        └── Generates deterministic dataset_id
            (UUID v5 from source_system + dataset_name)

Step 3: Computes SHA-256 hash of the config

Step 4: Compares hash against ingestion_config table
        ├── Hash MATCHES  → SKIP (no write needed)
        └── Hash DIFFERS  → proceed to Step 5

Step 5: MERGE INTO Unity Catalog metadata tables:
        ├── dataset_registry   ← dataset identity & classification
        ├── dataset_tags       ← key-value tags (domain, owner, PII, etc.)
        ├── ingestion_config   ← S3 path, format, load_type, keys, etc.
        └── dq_sla_config      ← initial DQ rules (if defined in YAML)

Result: Metadata tables are always in sync with YAML config.
        Unchanged datasets cost zero writes.
```

**Unity Catalog tables written:**

| Table | What gets written |
|---|---|
| `dataset_registry` | Dataset name, version, criticality, PII flag, active status |
| `dataset_tags` | All key-value tags from the YAML `tags:` block |
| `ingestion_config` | S3 path, format, load type, checkpoint/schema locations, primary keys |
| `dq_sla_config` | DQ rule definitions and thresholds |

---

### PHASE 2 — Bronze Ingestion

**Who does this:** Databricks Job — Task: `run_bronze_ingestion`  
**When:** On schedule (e.g. daily 06:00 UTC) or triggered manually  
**Files involved:**
- `src/bronze/bronze_ingestion.py`
- `src/bronze/bronze_loader.py`
- `ingestion_framework/ingestion_engine/autoloader_engine.py`
- `ingestion_framework/ingestion_engine/schema_registry.py`

```
Step 1: Job starts with parameters
        source_system = "ecommerce"
        environment   = "dev"
        run_mode      = "incremental"

Step 2: BronzeIngestion queries metadata tables
        SELECT dr.*, ic.*
        FROM dataset_registry dr
        JOIN ingestion_config ic ON dr.dataset_id = ic.dataset_id
        WHERE dr.source_system = 'ecommerce'
          AND dr.active = true
          AND dr.data_tier = 'bronze'
          AND ic.environment = 'dev'

        → Returns list of active datasets (e.g. orders, customers, products)

Step 3: For each active dataset — loop begins
        (failure of one dataset does NOT stop the loop)

        ┌─────────────────────────────────────────────┐
        │  Dataset: orders (CSV, incremental)         │
        │                                             │
        │  load_type = incremental                    │
        │  ↓                                          │
        │  AutoLoaderEngine._ingest_incremental()     │
        │                                             │
        │  spark.readStream                           │
        │    .format("cloudFiles")                    │
        │    .option("cloudFiles.format", "csv")      │
        │    .option("cloudFiles.schemaLocation",     │
        │       /Volumes/.../schemas/orders/)         │
        │    .load("s3://my-bucket/raw/.../orders/")  │
        │                                             │
        │  Add metadata columns:                      │
        │    _source_file      = input_file_name()    │
        │    _ingestion_date   = current_date()       │
        │    _ingestion_timestamp = current_timestamp │
        │                                             │
        │  writeStream                                │
        │    .format("delta")                         │
        │    .option("checkpointLocation",            │
        │       /Volumes/.../checkpoints/orders/)     │
        │    .trigger(availableNow=True)              │
        │    .toTable("dev_catalog.ecommerce_bronze   │
        │              .orders")                      │
        └─────────────────────────────────────────────┘

        ┌─────────────────────────────────────────────┐
        │  Dataset: customers (JSON, full load)       │
        │                                             │
        │  load_type = full                           │
        │  ↓                                          │
        │  AutoLoaderEngine._ingest_full()            │
        │                                             │
        │  spark.read                                 │
        │    .format("json")                          │
        │    .load("s3://my-bucket/raw/.../customers")│
        │                                             │
        │  df.write.format("delta")                   │
        │    .mode("overwrite")                       │
        │    .saveAsTable("dev_catalog               │
        │      .ecommerce_bronze.customers")          │
        └─────────────────────────────────────────────┘

Step 4: Schema Registry check (after each write)
        SchemaRegistry.check_and_register()
        ├── Get current schema from written Delta table
        ├── Compare with latest schema in schema_registry
        ├── If SAME    → no action
        └── If CHANGED → insert new version row:
              change_type    = "add_column"
              changed_columns = ["new_col_1"]
              version_number  = 2
              is_current      = true

Step 5: Update ingestion_runtime_state per dataset
        INSERT INTO ingestion_runtime_state:
          run_id          = job_run_id
          dataset_id      = orders_uuid
          layer           = "bronze"
          status          = "SUCCESS"
          rows_ingested   = 15420
          duration_seconds = 47
          start_time / end_time
```

**Delta tables written:**

| Table | Content |
|---|---|
| `dev_catalog.ecommerce_bronze.orders` | Raw CSV orders + metadata columns |
| `dev_catalog.ecommerce_bronze.customers` | Raw JSON customers (full overwrite) |
| `dev_catalog.ecommerce_bronze.products` | Raw Parquet products |

**Unity Catalog tables updated:**

| Table | What gets updated |
|---|---|
| `schema_registry` | New version row if schema changed |
| `ingestion_runtime_state` | Run metrics per dataset |

---

### PHASE 3 — Silver Transformation

**Who does this:** Databricks Job — Task: `run_silver_transformation` (Phase 2)  
**When:** After Bronze ingestion completes  
**Files involved:**
- `src/silver/silver_transformations.py`
- `src/silver/data_quality.py`

```
Step 1: Read Bronze Delta table
        spark.read.table("dev_catalog.ecommerce_bronze.orders")
        Optional: filter by _ingestion_date (only process today's new rows)

Step 2: Apply transformations
        ├── Type casting
        │     order_id     → IntegerType
        │     order_amount → DecimalType(18,2)
        │     order_ts     → TimestampType
        │
        ├── Null handling
        │     DROP rows where order_id is null
        │     FILL order_amount nulls with 0
        │
        ├── Deduplication
        │     Window.partitionBy(order_id)
        │           .orderBy(order_timestamp DESC)
        │     Keep rank = 1 (latest record per order)
        │
        └── String normalisation
              TRIM whitespace from all string columns

Step 3: Data Quality evaluation
        Load rules from dq_sla_config for this dataset
        For each rule:
          ├── not_null  → count nulls in column
          ├── range     → count rows failing expression
          ├── regex     → count rows not matching pattern
          └── unique    → count duplicate values

        Compute pass % per rule
        ├── Pass % >= threshold → OK
        └── Pass % <  threshold AND fail_on_breach=true → ABORT

Step 4: Split passing / failing rows
        passing_df = rows that passed all rules
        failing_df = rows that failed any rule

Step 5: Write to Silver
        MERGE INTO dev_catalog.ecommerce_silver.orders
        ON target.order_id = source.order_id
        WHEN MATCHED AND source.order_ts > target.order_ts
          THEN UPDATE SET *
        WHEN NOT MATCHED
          THEN INSERT *

Step 6: Write failing rows to quarantine
        failing_df.write → dev_catalog.ingestion_metadata.orders_quarantine

Step 7: Update DQ results and runtime state
```

---

### PHASE 4 — Gold Transformation

**Who does this:** Databricks Job — Task: `run_gold_transformation` (Phase 3)  
**When:** After Silver completes  
**Files involved:**
- `src/gold/gold_transformations.py`

```
Step 1: Resolve dataset dependencies
        SELECT upstream_dataset_id
        FROM dataset_dependencies
        WHERE downstream_dataset_id = gold_dataset_id

        → Confirms silver.orders and silver.products are ready

Step 2: Read Silver tables
        orders_df   = spark.table("dev_catalog.ecommerce_silver.orders")
        products_df = spark.table("dev_catalog.ecommerce_silver.products")

Step 3: Apply Gold transformations
        ├── Join orders + products on product_id
        ├── Filter order_status = 'DELIVERED'
        ├── GROUP BY order_date, product_category
        │   AGG: SUM(order_amount) AS daily_revenue
        │        COUNT(order_id)   AS order_count
        │        AVG(order_amount) AS avg_order_value
        └── Select final output columns

Step 4: Write to Gold
        MERGE INTO dev_catalog.ecommerce_gold.daily_order_revenue
        ON target.order_date = source.order_date
        AND target.product_category = source.product_category
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *

Step 5: Update ingestion_runtime_state (Gold run)
```

---

### PHASE 5 — API Ingestion (Parallel path, Phase 4)

**Files involved:** `src/api/api_fetcher.py`, `configs/api_config.yml`

```
Step 1: Load API config (endpoint, auth, pagination)

Step 2: Authenticate
        token = dbutils.secrets.get(scope="api-secrets-dev",
                                     key="pricing-api-token")
        headers = {"Authorization": f"Bearer {token}"}

Step 3: Fetch all pages
        page 1 → records 1-100
        page 2 → records 101-200
        ...until no next_cursor

Step 4: Convert to Spark DataFrame
        spark.createDataFrame(all_records)

Step 5: Write to Bronze Delta table
        df.write.format("delta").mode("overwrite")
          .saveAsTable("dev_catalog.ecommerce_bronze.market_pricing")
```

---

## 3. Metadata Tables — Who Reads and Writes Each

| Table | Written By | Read By |
|---|---|---|
| `dataset_registry` | `metadata_loader.py` | `bronze_ingestion.py` |
| `dataset_tags` | `metadata_loader.py` | Governance tools |
| `dataset_dependencies` | `metadata_loader.py` | `gold_transformations.py` |
| `ingestion_config` | `metadata_loader.py` | `bronze_ingestion.py`, `autoloader_engine.py` |
| `ingestion_runtime_state` | `bronze_ingestion.py`, `silver_transformations.py`, `gold_transformations.py` | Monitoring dashboards |
| `dq_sla_config` | `metadata_loader.py` (rules), `data_quality.py` (results) | `data_quality.py` |
| `schema_registry` | `schema_registry.py` | `silver_transformations.py` |

---

## 4. Unity Catalog Volumes — Purpose

| Volume Path | Used By | Purpose |
|---|---|---|
| `/Volumes/{catalog}/ingestion_metadata/autoloader_volume/schemas/{dataset}/` | Auto Loader | Stores inferred schema JSON — prevents re-inference on every run |
| `/Volumes/{catalog}/ingestion_metadata/autoloader_volume/checkpoints/{dataset}/` | Auto Loader | Tracks which files have been processed — enables exactly-once ingestion |

---

## 5. Code → Databricks Deployment Flow

```
Local Machine                GitHub                  Databricks
─────────────                ──────                  ──────────
Edit code
    │
git commit
git push ──────────────────► sushanthg99/Databricks
                                    │
databricks bundle deploy ───────────┼──────────────► Workspace files synced
                                    │                Job updated in Workflows
databricks repos update ────────────┴──────────────► Repo synced in /Repos/
```

**Commands:**
```powershell
# After any code change:
git add .
git commit -m "your message"
git push

# Redeploy to Databricks:
databricks bundle deploy --target dev --profile dev

# Sync repo in Databricks:
databricks repos update 1565896458886419 --branch main --profile dev

# Run the job:
databricks bundle run bronze_ingestion_job --target dev --profile dev
```

---

## 6. Environment Promotion Flow

```
dev  ──► uat  ──► prod

databricks bundle deploy --target dev  --profile dev
         ↓  (test + validate)
databricks bundle deploy --target uat  --profile uat
         ↓  (sign-off)
databricks bundle deploy --target prod --profile prod
```

Each environment has:
- Its own Databricks workspace
- Its own Unity Catalog (`dev_catalog` / `uat_catalog` / `prod_catalog`)
- Its own S3 bucket
- Its own secret scope

---

## 7. Implementation Phases

| Phase | What gets built | Status |
|---|---|---|
| **Phase 1** | Metadata loader MERGE SQL + Auto Loader Bronze ingestion | 🔲 Next |
| **Phase 2** | Silver transformations + Data Quality engine | 🔲 Pending |
| **Phase 3** | Gold aggregations + dependency resolution | 🔲 Pending |
| **Phase 4** | REST API ingestion + SLA monitoring + alerts | 🔲 Pending |
| **Phase 5** | PII column masking + data classification enforcement | 🔲 Pending |

---

## 8. File Responsibility Quick Reference

| File | What it does |
|---|---|
| `configs/domain_config.yml` | Define datasets (source, format, DQ rules, tags) |
| `configs/api_config.yml` | Define REST API sources |
| `configs/environments/dev.yml` | Dev-specific values (catalog, S3 bucket, log level) |
| `databricks.yml` | DAB bundle: workspace URL, targets, sync paths |
| `resources/databricks_job.yml` | Job definition: tasks, parameters, schedule |
| `sql/metadata_tables.sql` | Create all 7 metadata tables in Unity Catalog |
| `ingestion_framework/metadata_service/metadata_loader.py` | YAML → Unity Catalog MERGE |
| `ingestion_framework/ingestion_engine/autoloader_engine.py` | S3 → Delta Bronze (Auto Loader + batch) |
| `ingestion_framework/ingestion_engine/schema_registry.py` | Schema version tracking |
| `src/bronze/bronze_ingestion.py` | Job entry point: loop through active datasets |
| `src/bronze/bronze_loader.py` | Calls engine + schema registry per dataset |
| `src/silver/silver_transformations.py` | Bronze → Silver: cleanse + deduplicate + MERGE |
| `src/silver/data_quality.py` | Evaluate DQ rules, quarantine failing rows |
| `src/gold/gold_transformations.py` | Silver → Gold: aggregations + joins |
| `src/api/api_fetcher.py` | REST API → Bronze: fetch + paginate + write |
| `notebooks/bronze_ingestion_notebook.py` | Databricks entry point: reads widgets, calls bronze_ingestion |
| `notebooks/silver_transformation_notebook.py` | Databricks entry point for Silver |
| `notebooks/gold_transformation_notebook.py` | Databricks entry point for Gold |
| `tests/test_metadata_loader.py` | Unit tests: hash, UUID, validation, YAML loading |
