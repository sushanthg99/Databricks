# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion Notebook
# MAGIC **Dynamic Databricks Ingestion Framework — Phase 1**
# MAGIC
# MAGIC Thin entry point. All logic lives in `src/bronze/bronze_ingestion.py`.
# MAGIC Parameters are injected by the Databricks job — environment-specific
# MAGIC values flow from `databricks.yml` → job parameters → widgets here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Widgets

# COMMAND ----------

dbutils.widgets.text("source_system",   "ecommerce",          "Source System")
dbutils.widgets.text("environment",     "dev",                "Environment")
dbutils.widgets.text("catalog_name",    "dev_catalog",        "Catalog Name")
dbutils.widgets.text("metadata_schema", "ingestion_metadata", "Metadata Schema")
dbutils.widgets.text("log_level",       "DEBUG",              "Log Level")
dbutils.widgets.dropdown(
    "run_mode", "incremental",
    ["incremental", "full", "metadata_only"],
    "Run Mode"
)
dbutils.widgets.text("dataset_name", "", "Dataset Name (empty = all)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Read parameters

# COMMAND ----------

source_system   = dbutils.widgets.get("source_system")
environment     = dbutils.widgets.get("environment")
catalog_name    = dbutils.widgets.get("catalog_name")
metadata_schema = dbutils.widgets.get("metadata_schema")
log_level       = dbutils.widgets.get("log_level")
run_mode        = dbutils.widgets.get("run_mode")
dataset_name    = dbutils.widgets.get("dataset_name") or None

print("=" * 55)
print("  BRONZE INGESTION — ENVIRONMENT CONFIGURATION")
print("=" * 55)
print(f"  environment     : {environment}")
print(f"  catalog_name    : {catalog_name}")
print(f"  metadata_schema : {metadata_schema}")
print(f"  source_system   : {source_system}")
print(f"  run_mode        : {run_mode}")
print(f"  dataset_name    : {dataset_name or '(all active datasets)'}")
print(f"  log_level       : {log_level}")
print("=" * 55)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Configure logging

# COMMAND ----------

import logging

logging.basicConfig(
    level=getattr(logging, log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("bronze_ingestion_notebook")
logger.info(
    "Notebook started | env=%s | catalog=%s | mode=%s",
    environment, catalog_name, run_mode,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Add bundle files to Python path

# COMMAND ----------

import sys

# Bundle files are synced to this path by `databricks bundle deploy`
bundle_files_path = (
    f"/Workspace/Users/sushanth.gangadhara@blumetra.com"
    f"/.bundle/dynamic-databricks-ingestion/{environment}/files"
)

if bundle_files_path not in sys.path:
    sys.path.insert(0, bundle_files_path)
    logger.info("Added to sys.path: %s", bundle_files_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Load environment config

# COMMAND ----------

import yaml
from pathlib import Path

env_config_path = Path(f"{bundle_files_path}/configs/environments/{environment}.yml")

if env_config_path.exists():
    with open(env_config_path, "r") as f:
        env_config = yaml.safe_load(f)
    logger.info("Environment config loaded: %s", env_config_path)
else:
    logger.warning("Config file not found at %s — using defaults", env_config_path)
    env_config = {
        "environment": environment,
        "unity_catalog": {
            "catalog_name":    catalog_name,
            "metadata_schema": metadata_schema,
        },
        "ingestion": {
            "schema_evolution_mode": "addNewColumns",
            "max_files_per_trigger": 1000,
        },
    }

# Job parameters always take precedence over YAML file values
env_config["environment"] = environment
env_config.setdefault("unity_catalog", {})
env_config["unity_catalog"]["catalog_name"]    = catalog_name
env_config["unity_catalog"]["metadata_schema"] = metadata_schema

print(f"\nEffective runtime config:")
print(f"  catalog         : {env_config['unity_catalog']['catalog_name']}")
print(f"  metadata_schema : {env_config['unity_catalog']['metadata_schema']}")
print(f"  schema_evo_mode : {env_config.get('ingestion', {}).get('schema_evolution_mode', 'addNewColumns')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Run Bronze Ingestion

# COMMAND ----------

from src.bronze.bronze_ingestion import BronzeIngestion

ingestion = BronzeIngestion(spark=spark, dbutils=dbutils)

summary = ingestion.run(
    source_system   = source_system,
    environment     = environment,
    run_mode        = run_mode,
    dataset_name    = dataset_name,
    catalog_name    = catalog_name,
    metadata_schema = metadata_schema,
    env_config      = env_config,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Print summary

# COMMAND ----------

print("\n" + "=" * 55)
print("  RUN SUMMARY")
print("=" * 55)
for key, value in summary.items():
    print(f"  {key:<22}: {value}")
print("=" * 55)

logger.info("Notebook complete | summary=%s", summary)

# Fail the job if any datasets failed — surfaces error in Databricks Workflows UI
if summary.get("failed", 0) > 0:
    raise RuntimeError(
        f"Bronze ingestion completed with {summary['failed']} failed dataset(s). "
        f"Check {catalog_name}.{metadata_schema}.ingestion_runtime_state for details."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor — check ingestion_runtime_state
# MAGIC
# MAGIC Run this in a SQL cell to see per-dataset run results:
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     dataset_id, status, rows_ingested,
# MAGIC     duration_seconds, error_message, created_at
# MAGIC FROM ${catalog_name}.${metadata_schema}.ingestion_runtime_state
# MAGIC WHERE layer = 'bronze'
# MAGIC ORDER BY created_at DESC
# MAGIC LIMIT 20;
# MAGIC ```
