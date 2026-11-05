# Dynamic Databricks Ingestion Framework — Complete Explanation

## What is this framework?

This is a **metadata-driven data ingestion platform** built on Databricks.

The idea is simple: instead of writing custom ingestion code for every new dataset,
you write a YAML configuration file describing the dataset — where it is, what format
it is, what rules it must pass — and the framework handles everything else automatically.

**One framework. Any number of datasets. No new code per dataset.**

---

## The Problem it Solves

### Without this framework
```
Dataset 1 (Orders CSV)      → custom Python script #1
Dataset 2 (Customers JSON)  → custom Python script #2
Dataset 3 (Products TSV)    → custom Python script #3
Dataset 4 (Inventory API)   → custom Python script #4
...
Dataset N                   → custom Python script #N
```

Each script is different. If you need to add a new column, change a rule,
or track which runs succeeded — you update N scripts.

### With this framework
```
Dataset 1 (Orders CSV)      ─┐
Dataset 2 (Customers JSON)   ├─→  ONE ingestion engine  →  Delta Bronze tables
Dataset 3 (Products TSV)     │         driven by
Dataset 4 (Inventory API)   ─┘      YAML config files
```

Add a new dataset? Write 30 lines of YAML. Done.

---

## Architecture — The Big Picture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    SOURCE SYSTEMS                                        │
│   CSV files  │  JSON files  │  TSV files  │  Parquet  │  REST APIs      │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │  lands in
                           ▼
┌─────────────────────────────────────────────────────────────────────────┐
│              SOURCE STORAGE                                              │
│   AWS S3 (production)  OR  Unity Catalog Volume (demo/dev)              │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │
          ┌────────────────┼────────────────┐
          │                │                │
          ▼                ▼                ▼
┌──────────────┐  ┌─────────────────┐  ┌────────────────────┐
│  YAML Config │  │ Metadata Tables │  │  Ingestion Engine  │
│  (domain_    │─▶│ (Unity Catalog) │─▶│  (AutoLoader /     │
│  config.yml) │  │                 │  │   Batch Reader)    │
└──────────────┘  └─────────────────┘  └─────────┬──────────┘
                                                  │ writes to
                           ┌──────────────────────┼──────────────────────┐
                           ▼                      ▼                      ▼
                  ┌─────────────┐       ┌──────────────┐       ┌──────────────┐
                  │   BRONZE    │──────▶│    SILVER    │──────▶│    GOLD      │
                  │  Raw data   │       │  Cleansed    │       │  Aggregated  │
                  │  Delta Lake │       │  Validated   │       │  Business    │
                  └─────────────┘       └──────────────┘       └──────────────┘
```

---

## The Three Layers (Medallion Architecture)

### Bronze — Raw Data
- Exact copy of source data, no transformations
- Every row has 3 extra audit columns added automatically:
  - `_source_file` — which file it came from
  - `_ingestion_date` — date it was ingested
  - `_ingestion_timestamp` — exact timestamp
- If a source file has errors, they land here unmodified
- Think of it as: **"exactly what arrived"**

### Silver — Cleansed Data
- Type casting (string → integer, decimal, timestamp)
- Null handling (drop or fill)
- Deduplication by primary keys
- Data Quality rules evaluated here
- Failing rows quarantined separately
- Think of it as: **"clean, trustworthy data"**

### Gold — Business-Ready Data
- Aggregations (SUM, COUNT, AVG)
- Joins across Silver tables
- KPIs and business metrics
- What analysts and dashboards query
- Think of it as: **"answers, not raw data"**

---

## The 7 Metadata Tables — The Control Plane

Everything the framework does is driven by these 7 tables in Unity Catalog.
They are the "brain" of the system.

```
workspace.ingestion_metadata.*
```

### 1. dataset_registry
**What it is:** The master list of every dataset the framework manages.

```
dataset_id   dataset_name   source_system   data_tier   active   pii
─────────    ────────────   ─────────────   ─────────   ──────   ───
uuid-001     orders         ecommerce       bronze      true     false
uuid-002     customers      ecommerce       bronze      true     true
uuid-003     products       ecommerce       bronze      true     false
```

**Why it matters:** The ingestion job queries this to decide which datasets to process.
`active = false` means skip this dataset entirely.

---

### 2. dataset_tags
**What it is:** Key-value labels for each dataset.

```
dataset_id   tag_key          tag_value
─────────    ───────          ─────────
uuid-001     domain           ecommerce
uuid-001     owner            data-engineering-team
uuid-001     retention_days   365
uuid-002     sensitivity      pii
```

**Why it matters:** Governance, discovery, and policy enforcement (e.g. PII masking).

---

### 3. dataset_dependencies
**What it is:** Which datasets must complete before another can run.

```
upstream_dataset_id   downstream_dataset_id   dependency_type
───────────────────   ─────────────────────   ───────────────
silver_orders_id      gold_revenue_id         hard
silver_products_id    gold_revenue_id         hard
```

**Why it matters:** Gold layer won't run until its upstream Silver datasets are ready.

---

### 4. ingestion_config ← Most important
**What it is:** All ingestion parameters for every dataset.

```
dataset_id   source_type   volume_path / s3_bucket   format   load_type   target_table
─────────    ───────────   ──────────────────────    ──────   ─────────   ────────────
uuid-001     volume        /Volumes/.../orders/       tsv      full        workspace.demo_bronze.orders
uuid-002     s3            my-bucket/raw/customers/   json     incremental workspace.prod_bronze.customers
```

**Why it matters:** This is what the engine reads to know HOW to ingest each dataset —
source location, file format, whether to do a full reload or incremental append.

---

### 5. ingestion_runtime_state
**What it is:** Every single ingestion run recorded as a row.

```
dataset_name   status    rows_ingested   duration_sec   error_message
────────────   ──────    ─────────────   ────────────   ─────────────
orders         SUCCESS   30              2.4            null
customers      SUCCESS   10              1.8            null
products       FAILED    0               0.5            "'s3_bucket' required..."
```

**Why it matters:** Monitoring. You can see exactly what ran, when, how many rows,
and the full error message if something failed.

---

### 6. dq_sla_config
**What it is:** Data Quality rules per dataset.

```
dataset_name   rule_name              rule_type   column       threshold   fail_on_breach
────────────   ─────────              ─────────   ──────       ─────────   ──────────────
orders         order_id_not_null      not_null    order_id     100%        true
orders         order_amount_positive  range       order_amount 100%        true
customers      email_not_null         not_null    email        100%        true
```

**Why it matters:** Silver layer evaluates these rules before writing.
If `fail_on_breach = true` and the threshold is breached, the write is aborted.

---

### 7. schema_registry
**What it is:** Version history of every schema change.

```
dataset_name   version   change_type    changed_columns   is_current
────────────   ───────   ───────────    ───────────────   ──────────
orders         1         initial        [all columns]     false
orders         2         add_column     [discount_pct]    true
```

**Why it matters:** If a new column appears in the source files, it's detected,
recorded here, and the Bronze table is updated automatically. Full audit trail.

---

## The Two Unity Catalog Volumes

Volumes are like S3 buckets inside Unity Catalog — governed file storage.

### `/Volumes/workspace/ingestion_metadata/demo_volume/data/`
Where the TSV source files live (demo mode).
In production this would be S3.

### `/Volumes/workspace/ingestion_metadata/demo_volume/schemas/`
Auto Loader stores inferred schema JSON files here.
Prevents schema re-inference on every run — much faster.

### `/Volumes/workspace/ingestion_metadata/demo_volume/checkpoints/`
Auto Loader streaming checkpoints.
Tracks exactly which files have been processed.
Deleting this = re-process everything from scratch.

---

## The Code Architecture

```
dynamic-databricks-ingestion/
│
├── ingestion_framework/          ← REUSABLE ENGINE (no business logic)
│   ├── ingestion_engine/
│   │   ├── autoloader_engine.py  ← reads files, writes Bronze Delta tables
│   │   └── schema_registry.py   ← detects + records schema changes
│   └── metadata_service/
│       └── metadata_loader.py   ← reads YAML → writes metadata tables
│
├── src/                          ← LAYER ORCHESTRATION
│   ├── bronze/
│   │   ├── bronze_ingestion.py  ← job entry point, loops datasets
│   │   ├── bronze_loader.py     ← calls engine + schema registry
│   │   └── data_extraction.py  ← S3 path utilities
│   ├── silver/
│   │   ├── silver_transformations.py  ← cleanse + deduplicate
│   │   └── data_quality.py           ← DQ rule evaluation
│   ├── gold/
│   │   └── gold_transformations.py   ← aggregations + joins
│   └── api/
│       └── api_fetcher.py            ← REST API ingestion
│
├── demo/                         ← DEMO (no S3 needed)
│   ├── data/                    ← sample TSV files (orders, customers, products)
│   ├── demo_config.yml          ← demo YAML pointing to UC Volume
│   ├── sql/demo_metadata_tables.sql ← creates metadata tables
│   └── notebooks/demo_end_to_end.py ← runs the complete pipeline
│
├── configs/                      ← CONFIGURATION
│   ├── domain_config.yml        ← production dataset configs
│   ├── api_config.yml           ← API source configs
│   └── environments/            ← dev.yml / uat.yml / prod.yml
│
├── notebooks/                    ← DATABRICKS ENTRY POINTS
│   ├── bronze_ingestion_notebook.py
│   ├── silver_transformation_notebook.py
│   └── gold_transformation_notebook.py
│
├── resources/
│   └── databricks_job.yml       ← Databricks Workflows job definition
│
└── .github/workflows/
    └── deploy.yml               ← GitHub Actions CI/CD
```

**Dependency rule:**
```
notebooks → src → ingestion_framework → Spark / Unity Catalog
```
`ingestion_framework` never imports from `src`. One-way dependency only.

---

## The Demo — Step by Step

The demo notebook (`demo/notebooks/demo_end_to_end.py`) runs the **complete
Bronze ingestion pipeline** without needing AWS S3. It uses a Unity Catalog
Volume as the source storage.

### Demo Data
```
orders.tsv    — 30 rows — order transactions (order_id, customer_id, product_id, amount, status)
customers.tsv — 10 rows — customer master (customer_id, name, email, city, loyalty_tier)
products.tsv  — 10 rows — product catalogue (product_id, name, category, price, stock)
```

### Step 1 — Setup
```python
# Creates schemas and all 6 metadata tables from scratch
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.ingestion_metadata")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.demo_bronze")
spark.sql("DROP TABLE IF EXISTS workspace.ingestion_metadata.ingestion_config")
spark.sql("CREATE TABLE workspace.ingestion_metadata.ingestion_config (...)")
# ... all 6 tables created fresh every run
```

### Step 2 — Write TSV data to UC Volume
```python
# TSV data is embedded as Python strings — no file I/O from Repos needed
ORDERS_TSV = """order_id\tcustomer_id\t..."""

# Write to /Volumes/workspace/ingestion_metadata/demo_volume/data/orders/
with open("/Volumes/.../orders/orders.tsv", "w") as f:
    f.write(ORDERS_TSV)
```

### Step 3 — Metadata Registration
```python
# MetadataLoader reads the inline YAML config
# Computes SHA-256 hash of each dataset config
# Issues MERGE INTO for each metadata table

loader = MetadataLoader(spark, catalog="workspace", ...)
result = loader.load_from_list(datasets)

# What gets written:
# workspace.ingestion_metadata.dataset_registry  → 3 rows (orders, customers, products)
# workspace.ingestion_metadata.ingestion_config  → 3 rows (source paths, formats, etc.)
# workspace.ingestion_metadata.dataset_tags      → tags per dataset
# workspace.ingestion_metadata.dq_sla_config     → DQ rules per dataset
```

### Step 4 — Bronze Ingestion
```python
# AutoLoaderEngine is called directly with each dataset config
engine = AutoLoaderEngine(spark, env_config)

for cfg in DATASET_CONFIGS:
    result = engine.ingest(cfg)

# What engine.ingest() does:
# 1. Validates the config (source_type, format, target_table present?)
# 2. Builds source path: /Volumes/.../orders/
# 3. Reads TSV with spark.read.format("csv").option("delimiter","\t").load(source_path)
# 4. Adds 3 metadata columns: _source_file, _ingestion_date, _ingestion_timestamp
# 5. Writes to workspace.demo_bronze.orders with mode="overwrite"
# 6. Returns {status: "SUCCESS", rows_ingested: 30}
```

### Step 5 — Verify Bronze Tables
```python
# Query the 3 Bronze Delta tables
spark.table("workspace.demo_bronze.orders").show()
# → 30 rows with all original columns + 3 metadata columns

# Analytics on raw Bronze data:
# Orders by status (DELIVERED, SHIPPED, PENDING, etc.)
# Top 5 customers by total spend
# Revenue by product category
```

### Step 6 — View Metadata Tables
```python
# ingestion_runtime_state — every run recorded
# schema_registry — schema version per dataset
# dq_sla_config — rules that were registered
```

---

## The Full Workflow — From Code to Data

```
DEVELOPER
    │
    │  1. Edit YAML config (add new dataset or change settings)
    │     configs/domain_config.yml
    │
    │  2. git add . && git commit && git push
    │
    ▼
GITHUB
    │
    │  3. GitHub Actions triggers automatically
    │     .github/workflows/deploy.yml
    │
    │  4. Validates bundle (databricks bundle validate)
    │  5. Deploys bundle (databricks bundle deploy --target dev)
    │     → syncs Python files to Databricks workspace
    │     → updates job definition in Databricks Workflows
    │  6. Syncs Repos UI (databricks repos update)
    │
    ▼
DATABRICKS WORKSPACE
    │
    │  7. Job triggered (scheduled or manual)
    │     Databricks Workflows → [dev] Bronze Ingestion
    │
    │  8. Task 1: register_metadata
    │     → notebook calls MetadataLoader
    │     → reads YAML, computes hashes
    │     → MERGE INTO metadata tables (skip if unchanged)
    │
    │  9. Task 2: run_bronze_ingestion
    │     → notebook calls BronzeIngestion
    │     → queries ingestion_config for active datasets
    │     → for each dataset: AutoLoaderEngine.ingest()
    │       ├── reads from S3 (production) or Volume (demo)
    │       ├── adds metadata columns
    │       ├── writes to Delta Bronze table
    │       └── records result in ingestion_runtime_state
    │
    ▼
DELTA LAKE (Unity Catalog)
    │
    │  workspace.demo_bronze.orders     — 30 rows ✅
    │  workspace.demo_bronze.customers  — 10 rows ✅
    │  workspace.demo_bronze.products   — 10 rows ✅
    │
    ▼
ANALYSTS / DASHBOARDS
    Query Bronze, Silver, or Gold tables via Databricks SQL
```

---

## Environment Strategy

Three fully separate environments, each with its own:
- Databricks workspace
- Unity Catalog (`dev_catalog`, `uat_catalog`, `prod_catalog`)
- S3 bucket
- Secret scope

| Setting | Dev | UAT | Prod |
|---|---|---|---|
| Catalog | `workspace` | `uat_catalog` | `prod_catalog` |
| Log level | DEBUG (verbose) | INFO | INFO |
| DQ fail on breach | false (never abort) | true | true |
| Notifications | off | team email | on-call + PagerDuty |
| DAB mode | development | staging | production |

**Promoting to prod:**
```bash
# Step 1: Validate in UAT first
databricks bundle deploy --target uat --profile uat

# Step 2: After sign-off
databricks bundle deploy --target prod --profile prod
```

---

## How to Add a New Dataset (The Key Value Proposition)

**Zero code changes required.**

1. Add a new entry to `configs/domain_config.yml`:

```yaml
- dataset_name: inventory
  version: "1.0"
  dataset_type: reference
  data_tier: bronze
  source_system: warehouse
  active: true
  source_type: s3
  s3_bucket: my-production-bucket
  s3_path: raw/warehouse/inventory/
  format: csv
  delimiter: ","
  header: true
  load_type: incremental
  primary_keys: [sku_id]
  data_quality:
    - rule_name: sku_not_null
      rule_type: not_null
      column_name: sku_id
      threshold_pct: 100.0
      fail_on_breach: true
```

2. Push to GitHub:
```bash
git add configs/domain_config.yml
git commit -m "feat: add inventory dataset"
git push
```

3. GitHub Actions deploys automatically.

4. The next job run picks up `inventory` and ingests it into
   `{catalog}.warehouse_bronze.inventory` — no code written.

---

## What's Implemented vs What's Planned

### Phase 1 — Complete ✅
- Metadata loader (YAML → 7 metadata tables via MERGE)
- Auto Loader engine (incremental + full batch)
- Bronze ingestion orchestration
- Schema registry (version tracking)
- Runtime state tracking
- Demo end-to-end pipeline
- GitHub Actions CI/CD
- Databricks Asset Bundle deployment

### Phase 2 — Next 🔲
- Silver transformations (type casting, deduplication)
- Data Quality engine (not_null, range, regex rules)
- Quarantine table for DQ failures

### Phase 3 — Planned 🔲
- Gold aggregations
- Dataset dependency resolution

### Phase 4 — Planned 🔲
- REST API ingestion
- SLA monitoring and alerting

### Phase 5 — Planned 🔲
- PII column masking
- Column-level lineage

---

## Key Design Decisions

| Decision | Why |
|---|---|
| SHA-256 hash deduplication | Avoids re-writing metadata when config hasn't changed |
| UC Volumes for checkpoints | Governed, auditable — not unmanaged file paths |
| `trigger(availableNow=True)` | Processes backlog then stops — no always-on streaming |
| Append-only runtime state | Full history, no data loss, simple INSERT |
| Separate catalog per environment | Clean isolation, no cross-env accidents |
| `ingestion_framework` never imports `src` | Keeps the engine reusable and testable |
| Source type field | Same engine handles Volume/S3/DBFS — swap with config only |

---

## Glossary

| Term | Meaning |
|---|---|
| **Medallion Architecture** | Bronze → Silver → Gold data quality layers |
| **Auto Loader** | Databricks feature for incremental file ingestion with checkpointing |
| **Unity Catalog** | Databricks governance layer — manages tables, volumes, access control |
| **Delta Lake** | Open table format with ACID transactions, time travel, schema evolution |
| **DAB** | Databricks Asset Bundles — deploy jobs, clusters, notebooks as code |
| **Metadata-driven** | Behaviour controlled by config/tables, not hardcoded in scripts |
| **UC Volume** | Governed file storage inside Unity Catalog (like an S3 bucket with access control) |
| **MERGE INTO** | Delta SQL operation — upsert (insert if new, update if exists) |
| **config_hash** | SHA-256 fingerprint of a YAML config — used to detect changes |
| **Bronze table** | Raw Delta table — exact copy of source with audit columns added |
| **Schema evolution** | Auto Loader detects new columns and adds them automatically |
