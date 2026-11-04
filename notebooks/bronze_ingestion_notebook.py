# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion Notebook
# MAGIC **Dynamic Databricks Ingestion Framework**
# MAGIC
# MAGIC This notebook is the entry point for the Bronze ingestion job.
# MAGIC It accepts parameters via Databricks widgets (interactive) or job parameters
# MAGIC (automated), then delegates all logic to `src/bronze/bronze_ingestion.py`.
# MAGIC
# MAGIC **This notebook contains no business logic** — it is a thin wrapper only.
# MAGIC All ingestion logic lives in the `src/` and `ingestion_framework/` packages.
# MAGIC
# MAGIC ## Parameters
# MAGIC | Parameter | Description | Default |
# MAGIC |---|---|---|
# MAGIC | `source_system` | Source system to ingest (e.g. `ecommerce`) | `ecommerce` |
# MAGIC | `environment` | Target environment (`dev`/`uat`/`prod`) | `dev` |
# MAGIC | `run_mode` | `incremental`, `full`, or `metadata_only` | `incremental` |
# MAGIC | `dataset_name` | Single dataset name (empty = all active) | `` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Widgets

# COMMAND ----------

dbutils.widgets.text("source_system", "ecommerce", "Source System")
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.dropdown("run_mode", "incremental", ["incremental", "full", "metadata_only"], "Run Mode")
dbutils.widgets.text("dataset_name", "", "Dataset Name (empty = all)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Parameters

# COMMAND ----------

source_system = dbutils.widgets.get("source_system")
environment   = dbutils.widgets.get("environment")
run_mode      = dbutils.widgets.get("run_mode")
dataset_name  = dbutils.widgets.get("dataset_name") or None  # Convert empty string to None

print(f"Parameters:")
print(f"  source_system : {source_system}")
print(f"  environment   : {environment}")
print(f"  run_mode      : {run_mode}")
print(f"  dataset_name  : {dataset_name or '(all active datasets)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Logging

# COMMAND ----------

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("bronze_ingestion_notebook")
logger.info("Notebook started | source_system=%s | env=%s | mode=%s", source_system, environment, run_mode)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Bronze Ingestion

# COMMAND ----------

# TODO (Phase 1): Uncomment once BronzeIngestion is fully implemented

# from src.bronze.bronze_ingestion import BronzeIngestion
#
# ingestion = BronzeIngestion(spark=spark, dbutils=dbutils)
#
# summary = ingestion.run(
#     source_system=source_system,
#     environment=environment,
#     run_mode=run_mode,
#     dataset_name=dataset_name,
# )
#
# print("\n=== Run Summary ===")
# for key, value in summary.items():
#     print(f"  {key}: {value}")
#
# logger.info("Notebook complete | summary=%s", summary)
#
# # Fail the notebook/job if any datasets failed
# if summary.get("failed", 0) > 0:
#     raise RuntimeError(
#         f"Bronze ingestion completed with {summary['failed']} failed dataset(s). "
#         f"Check ingestion_runtime_state for details."
#     )

# PLACEHOLDER — remove when Phase 1 is implemented
print("TODO: Bronze ingestion not yet implemented. See src/bronze/bronze_ingestion.py")
logger.warning("BronzeIngestion not yet implemented — placeholder notebook run")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC Check `ingestion_runtime_state` for detailed per-dataset run metrics:
# MAGIC ```sql
# MAGIC SELECT dataset_id, status, rows_ingested, duration_seconds, error_message
# MAGIC FROM {catalog}.ingestion_metadata.ingestion_runtime_state
# MAGIC WHERE layer = 'bronze'
# MAGIC ORDER BY created_at DESC
# MAGIC LIMIT 20;
# MAGIC ```
