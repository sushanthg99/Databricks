# Databricks notebook source
# MAGIC %md
# MAGIC # ABC Planning (TM1) P&L Pipeline
# MAGIC **Blumetra — intake → Bronze → Silver → security scope → Gold (row filter + mask)**
# MAGIC
# MAGIC Thin entry point; all logic is in `ingestion_framework/tm1/pipeline.py`.
# MAGIC Use the `stage` widget to run one stage (job tasks) or `all` (interactive).
# MAGIC See `docs/tm1_pipeline.md`.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "workspace", "Catalog")
dbutils.widgets.text("metadata_schema", "ingestion_metadata", "Metadata schema")
dbutils.widgets.text("landing_schema", "tm1_landing", "Landing schema (holds the volume)")
dbutils.widgets.text("landing_volume", "landing", "Landing volume")
dbutils.widgets.text("run_id", "RUN001", "Run id")
dbutils.widgets.dropdown("stage", "through_bronze",
    ["through_bronze", "all", "stage_landing", "intake", "bronze", "silver", "security", "gold"], "Stage")
dbutils.widgets.dropdown("stage_from_repo", "false", ["true", "false"],
    "Copy demo data from repo into the volume first")
dbutils.widgets.text("admin_group", "tm1_admins", "Group that bypasses row filter / mask")
dbutils.widgets.text("text_reader_group", "tm1_text_readers", "Group that may read comment text")
dbutils.widgets.text("dq_max_fail_pct", "5", "DQ error-rule failure threshold (%)")
dbutils.widgets.text("preview_identity", "", "Preview what this identity can see (optional)")
dbutils.widgets.text("log_level", "INFO", "Log level")

# COMMAND ----------

import logging, sys

p = {k: dbutils.widgets.get(k) for k in (
    "catalog_name", "metadata_schema", "landing_schema", "landing_volume", "run_id", "stage",
    "stage_from_repo", "admin_group", "text_reader_group", "dq_max_fail_pct",
    "preview_identity", "log_level")}
logging.basicConfig(level=getattr(logging, p["log_level"].upper(), logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

# repo / bundle root = two levels up from  <root>/notebooks/<this notebook>
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + nb_path.rsplit("/notebooks/", 1)[0]
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)
print("repo_root:", repo_root)

from ingestion_framework.tm1.pipeline import TM1Pipeline, stage_run

landing_dir = f"/Volumes/{p['catalog_name']}/{p['landing_schema']}/{p['landing_volume']}/{p['run_id']}"
pipe = TM1Pipeline(
    spark, catalog=p["catalog_name"], landing_dir=landing_dir, run_id=p["run_id"],
    meta_schema=p["metadata_schema"], admin_group=p["admin_group"],
    text_reader_group=p["text_reader_group"], dq_max_fail_pct=float(p["dq_max_fail_pct"]))

# COMMAND ----------

# MAGIC %md ## Stage 0 — land the files in a UC Volume
# MAGIC In production the extract process writes the run folder + `_READY_<run>.json` here.
# MAGIC For the demo we copy the mock data shipped in the repo.

# COMMAND ----------

if p["stage"] in ("all", "through_bronze", "stage_landing"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {p['catalog_name']}.{p['landing_schema']}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {p['catalog_name']}.{p['landing_schema']}.{p['landing_volume']}")
    if p["stage_from_repo"] == "true":
        stage_run(f"{repo_root}/demo/tm1_mock_data/{p['run_id']}", landing_dir)
        print("staged to", landing_dir)

# COMMAND ----------

# MAGIC %md ## Pipeline stages

# COMMAND ----------

import os
stage = p["stage"]
if stage != "stage_landing" and not any(n.startswith("_READY_") for n in (os.listdir(landing_dir) if os.path.isdir(landing_dir) else [])):
    raise FileNotFoundError(
        f"No _READY_*.json manifest in {landing_dir}. Upload the run folder contents to that volume path "
        f"(Catalog > {p['catalog_name']} > {p['landing_schema']} > {p['landing_volume']} > Upload) or set stage_from_repo=true.")

if stage == "through_bronze":
    pipe.intake()
    print(pipe.bronze_load())
elif stage == "all":
    result = pipe.run_all()
    print(result)
elif stage == "intake":
    pipe.intake()
elif stage == "bronze":
    print(pipe.bronze_load())
elif stage == "silver":
    print(pipe.silver_transform())
elif stage == "security":
    print(pipe.build_security_scope())
elif stage == "gold":
    print(pipe.gold_build())
    pipe.apply_security_policies()

# COMMAND ----------

# MAGIC %md ## Verify

# COMMAND ----------

if stage in ("through_bronze", "bronze", "all"):
    # reconciliation: Bronze rows vs the manifest row_count of every file
    display(spark.createDataFrame(pipe.verify_bronze()))
    display(spark.sql(f"SHOW TABLES IN {p['catalog_name']}.{pipe.bronze}"))
    display(spark.table(pipe.t(pipe.bronze, "bronze_pnl_fact")).limit(10))

# COMMAND ----------

if stage in ("all", "gold"):
    display(spark.sql(f"SELECT stage, status, row_count, message, event_ts "
                      f"FROM {p['catalog_name']}.{p['metadata_schema']}.tm1_run_log "
                      f"WHERE run_id = '{p['run_id']}' ORDER BY event_ts"))
    display(spark.sql(f"SELECT rule, severity, failed_rows, ROUND(fail_pct, 2) AS fail_pct, breached "
                      f"FROM {p['catalog_name']}.{p['metadata_schema']}.tm1_dq_results "
                      f"WHERE run_id = '{p['run_id']}' ORDER BY failed_rows DESC"))
    display(spark.sql(f"SELECT identity, dimension, count(*) AS members, "
                      f"sum(CASE WHEN member = '*' THEN 1 ELSE 0 END) AS open_dim "
                      f"FROM {p['catalog_name']}.{p['metadata_schema']}.tm1_user_dim_scope "
                      f"GROUP BY identity, dimension ORDER BY identity, dimension"))

if p["preview_identity"]:
    display(pipe.preview_access(p["preview_identity"]))
