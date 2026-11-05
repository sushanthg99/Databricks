# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion Notebook
# MAGIC **Dynamic Databricks Ingestion Framework**
# MAGIC
# MAGIC Thin entry point — all logic lives in `src/bronze/bronze_ingestion.py`.
# MAGIC Parameters are injected by the Databricks job (environment-specific values
# MAGIC come from `databricks.yml` variables → job parameters → notebook widgets).
# MAGIC
# MAGIC | Parameter | Dev | UAT | Prod |
# MAGIC |---|---|---|---|
# MAGIC | `environment` | dev | uat | prod |
# MAGIC | `catalog_name` | dev_catalog | uat_catalog | prod_catalog |
# MAGIC | `log_level` | DEBUG | INFO | INFO |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Widgets (receive job parameters)

# COMMAND ----------

dbutils.widgets.text("source_system",   "ecommerce",           "Source System")
dbutils.widgets.text("environment",     "dev",                 "Environment")
dbutils.widgets.text("catalog_name",    "dev_catalog",         "Catalog Name")
dbutils.widgets.text("metadata_schema", "ingestion_metadata",  "Metadata Schema")
dbutils.widgets.text("log_level",       "DEBUG",               "Log Level")
dbutils.widgets.dropdown("run_mode", "incremental",
                          ["incremental", "full", "metadata_only"], "Run Mode")
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

print("=" * 50)
print("ENVIRONMENT CONFIGURATION")
print("=" * 50)
print(f"  environment     : {environment}")
print(f"  catalog_name    : {catalog_name}")
print(f"  metadata_schema : {metadata_schema}")
print(f"  source_system   : {source_system}")
print(f"  run_mode        : {run_mode}")
print(f"  dataset_name    : {dataset_name or '(all active datasets)'}")
print(f"  log_level       : {log_level}")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Configure logging

# COMMAND ----------

import logging

log_level_map = {
    "DEBUG":    logging.DEBUG,
    "INFO":     logging.INFO,
    "WARNING":  logging.WARNING,
    "ERROR":    logging.ERROR,
}

logging.basicConfig(
    level=log_level_map.get(log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("bronze_ingestion_notebook")
logger.info(
    "Notebook started | env=%s | catalog=%s | mode=%s",
    environment, catalog_name, run_mode
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Load environment config from YAML

# COMMAND ----------

import yaml
from pathlib import Path

# Load environment-specific YAML config
# This gives runtime values: S3 paths, volume paths, DQ settings, etc.
env_config_path = Path(
    f"/Workspace/Users/sushanth.gangadhara@blumetra.com"
    f"/.bundle/dynamic-databricks-ingestion/{environment}/files"
    f"/configs/environments/{environment}.yml"
)

if env_config_path.exists():
    with open(env_config_path, "r") as f:
        env_config = yaml.safe_load(f)
    logger.info("Environment config loaded from: %s", env_config_path)
else:
    logger.warning(
        "Environment config file not found at %s — using defaults", env_config_path
    )
    env_config = {
        "environment": environment,
        "unity_catalog": {
            "catalog_name": catalog_name,
            "metadata_schema": metadata_schema,
        }
    }

# Override catalog_name and metadata_schema from job parameters
# (job parameters take precedence over YAML file)
env_config["unity_catalog"]["catalog_name"]    = catalog_name
env_config["unity_catalog"]["metadata_schema"] = metadata_schema

print(f"\nEffective config:")
print(f"  catalog  : {env_config['unity_catalog']['catalog_name']}")
print(f"  schema   : {env_config['unity_catalog']['metadata_schema']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Run Bronze Ingestion

# COMMAND ----------

# TODO (Phase 1): Uncomment once BronzeIngestion is fully implemented

# import sys
# sys.path.insert(0, "/Workspace/Users/sushanth.gangadhara@blumetra.com"
#                    "/.bundle/dynamic-databricks-ingestion/{environment}/files")
#
# from src.bronze.bronze_ingestion import BronzeIngestion
#
# ingestion = BronzeIngestion(spark=spark, dbutils=dbutils)
#
# summary = ingestion.run(
#     source_system   = source_system,
#     environment     = environment,
#     run_mode        = run_mode,
#     dataset_name    = dataset_name,
#     catalog_name    = catalog_name,
#     metadata_schema = metadata_schema,
#     env_config      = env_config,
# )
#
# print("\n=== Run Summary ===")
# for key, value in summary.items():
#     print(f"  {key}: {value}")
#
# if summary.get("failed", 0) > 0:
#     raise RuntimeError(
#         f"{summary['failed']} dataset(s) failed. "
#         f"Check {catalog_name}.{metadata_schema}.ingestion_runtime_state"
#     )

# PLACEHOLDER
print("\nTODO: Phase 1 not yet implemented.")
print(f"When implemented, this will ingest [{source_system}] datasets")
print(f"from S3 into [{catalog_name}.{environment}_bronze.*]")
logger.warning("BronzeIngestion placeholder — Phase 1 pending")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC Check runtime state:
# MAGIC ```sql
# MAGIC SELECT dataset_id, status, rows_ingested, duration_seconds, error_message
# MAGIC FROM ${catalog_name}.${metadata_schema}.ingestion_runtime_state
# MAGIC WHERE layer = 'bronze'
# MAGIC ORDER BY created_at DESC
# MAGIC LIMIT 20;
# MAGIC ```
