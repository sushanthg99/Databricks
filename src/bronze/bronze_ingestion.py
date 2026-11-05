"""
bronze_ingestion.py
===================
Bronze layer top-level ingestion orchestrator.

Responsibilities
----------------
1. Accept job/notebook parameters (source_system, environment, run_mode, dataset_name).
2. Load environment configuration from configs/environments/{env}.yml.
3. Query Unity Catalog metadata tables for active Bronze datasets.
4. Loop through each dataset and call BronzeLoader.load().
5. Write per-dataset run metrics to ingestion_runtime_state.
6. A single dataset failure must NOT abort the loop — log and continue.
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class BronzeIngestion:
    """
    Orchestrates Bronze layer ingestion for one or more datasets.

    Parameters
    ----------
    spark : SparkSession
    dbutils : object
        Databricks dbutils for secrets and widget access.
        Pass None when running unit tests.
    """

    def __init__(self, spark: "SparkSession", dbutils: Any = None) -> None:
        self.spark   = spark
        self.dbutils = dbutils
        self._env_config: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def run(
        self,
        source_system:   str,
        environment:     str,
        run_mode:        str = "incremental",
        dataset_name:    str | None = None,
        catalog_name:    str | None = None,
        metadata_schema: str | None = None,
        env_config:      dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Execute Bronze ingestion for active datasets in the given source system.

        Parameters
        ----------
        source_system : str
            Filters dataset_registry by source_system value.
        environment : str
            Target environment (dev | uat | prod).
        run_mode : str
            incremental | full | metadata_only
        dataset_name : str, optional
            Run a single named dataset only.
        catalog_name : str, optional
            Override catalog from env config (injected by job parameter).
        metadata_schema : str, optional
            Override metadata schema from env config.
        env_config : dict, optional
            Pre-loaded env config dict (injected by notebook, avoids double load).

        Returns
        -------
        dict[str, Any]
            ``{total, succeeded, failed, skipped, duration_seconds}``
        """
        start_time = time.time()
        logger.info(
            "BronzeIngestion.run() | source_system=%s | env=%s | mode=%s",
            source_system, environment, run_mode,
        )

        # Load environment config — use injected if provided, else load from file
        if env_config:
            self._env_config = env_config
        else:
            self._env_config = self._load_env_config(environment)

        # Allow job parameter overrides to take precedence
        catalog = (
            catalog_name
            or self._env_config.get("unity_catalog", {}).get("catalog_name", "dev_catalog")
        )
        schema = (
            metadata_schema
            or self._env_config.get("unity_catalog", {}).get("metadata_schema", "ingestion_metadata")
        )

        logger.info("Using catalog=%s | metadata_schema=%s", catalog, schema)

        # Metadata-only run — register YAML configs, no ingestion
        if run_mode == "metadata_only":
            return self._run_metadata_only(catalog, schema, environment)

        # Query active datasets from Unity Catalog
        datasets = self._query_active_datasets(
            catalog=catalog,
            metadata_schema=schema,
            source_system=source_system,
            environment=environment,
            dataset_name_filter=dataset_name,
        )

        if not datasets:
            logger.warning(
                "No active datasets found | source_system=%s | env=%s",
                source_system, environment,
            )
            return {
                "total": 0, "succeeded": 0,
                "failed": 0, "skipped": 0,
                "duration_seconds": 0,
            }

        logger.info("Active datasets found: %d", len(datasets))

        # Ingestion loop — one failure does NOT stop other datasets
        succeeded = failed = skipped = 0
        run_id = str(uuid.uuid4())

        for dataset_cfg in datasets:
            result = self._ingest_one(
                dataset_cfg=dataset_cfg,
                run_mode=run_mode,
                catalog=catalog,
                metadata_schema=schema,
                run_id=run_id,
            )
            if result["status"] == "SUCCESS":
                succeeded += 1
            elif result["status"] == "SKIPPED":
                skipped += 1
            else:
                failed += 1

        duration = round(time.time() - start_time, 2)
        summary  = {
            "total":            len(datasets),
            "succeeded":        succeeded,
            "failed":           failed,
            "skipped":          skipped,
            "duration_seconds": duration,
        }
        logger.info("BronzeIngestion complete | %s", summary)
        return summary

    # ------------------------------------------------------------------
    # Single dataset ingestion
    # ------------------------------------------------------------------

    def _ingest_one(
        self,
        dataset_cfg:     dict[str, Any],
        run_mode:        str,
        catalog:         str,
        metadata_schema: str,
        run_id:          str,
    ) -> dict[str, Any]:
        """
        Ingest a single dataset, then write its result to ingestion_runtime_state.
        Never raises — all exceptions are caught, logged, and returned as FAILED.
        """
        from src.bronze.bronze_loader import BronzeLoader

        dataset_name = dataset_cfg.get("dataset_name", "UNKNOWN")
        start        = time.time()
        logger.info("Processing dataset: %s", dataset_name)

        try:
            loader = BronzeLoader(
                spark=self.spark,
                env_config=self._env_config,
                catalog=catalog,
                metadata_schema=metadata_schema,
            )
            result = loader.load(dataset_cfg=dataset_cfg, run_mode=run_mode)

        except Exception as exc:
            logger.exception(
                "Dataset ingestion failed | dataset=%s | error=%s", dataset_name, exc
            )
            result = {
                "status":        "FAILED",
                "rows_ingested": 0,
                "error_message": str(exc),
            }

        result["duration_seconds"] = round(time.time() - start, 2)

        self._update_runtime_state(
            dataset_cfg=dataset_cfg,
            result=result,
            catalog=catalog,
            metadata_schema=metadata_schema,
            run_id=run_id,
        )
        return result

    # ------------------------------------------------------------------
    # Metadata-only run
    # ------------------------------------------------------------------

    def _run_metadata_only(
        self,
        catalog:         str,
        metadata_schema: str,
        environment:     str,
    ) -> dict[str, Any]:
        """
        Register YAML configs into Unity Catalog metadata tables.
        No ingestion is performed — only metadata MERGE operations.
        """
        from ingestion_framework.metadata_service.metadata_loader import MetadataLoader

        loader = MetadataLoader(
            spark=self.spark,
            catalog=catalog,
            metadata_schema=metadata_schema,
            environment=environment,
        )

        # Resolve config path relative to this file
        config_path = Path(__file__).parents[2] / "configs" / "domain_config.yml"
        result = loader.load_from_file(config_path)
        logger.info("Metadata-only run complete | %s", result)
        return result

    # ------------------------------------------------------------------
    # Active dataset query
    # ------------------------------------------------------------------

    def _query_active_datasets(
        self,
        catalog:              str,
        metadata_schema:      str,
        source_system:        str,
        environment:          str,
        dataset_name_filter:  str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Query Unity Catalog for active Bronze datasets matching the given
        source_system and environment.

        Joins dataset_registry + ingestion_config to produce one merged
        config dict per dataset — ready to pass directly to AutoLoaderEngine.

        Returns
        -------
        list[dict[str, Any]]
            One dict per active dataset with all ingestion config fields.
        """
        name_filter_clause = (
            f"AND dr.dataset_name = '{dataset_name_filter}'"
            if dataset_name_filter else ""
        )

        query = f"""
            SELECT
                dr.dataset_id,
                dr.dataset_name,
                dr.source_system,
                dr.data_tier,
                dr.criticality,
                dr.pii,
                ic.config_id,
                ic.ingestion_mode,
                ic.load_type,
                ic.source_type,
                ic.volume_path,
                ic.dbfs_path,
                ic.s3_bucket,
                ic.s3_path,
                ic.file_pattern,
                ic.format,
                ic.delimiter,
                ic.encoding,
                ic.header,
                ic.multiline,
                ic.schema_location,
                ic.checkpoint_location,
                ic.primary_keys,
                ic.watermark_column,
                ic.watermark_delay,
                ic.frequency,
                ic.target_table
            FROM {catalog}.{metadata_schema}.dataset_registry  dr
            JOIN {catalog}.{metadata_schema}.ingestion_config  ic
              ON dr.dataset_id  = ic.dataset_id
            WHERE dr.source_system = '{source_system}'
              AND dr.active        = true
              AND dr.data_tier     = 'bronze'
              AND ic.environment   = '{environment}'
              {name_filter_clause}
            ORDER BY dr.dataset_name
        """

        logger.debug("Querying active datasets:\n%s", query)

        try:
            rows = self.spark.sql(query).collect()
            datasets = [row.asDict() for row in rows]
            logger.info(
                "Found %d active dataset(s) | source_system=%s | env=%s",
                len(datasets), source_system, environment,
            )
            return datasets

        except Exception as exc:
            logger.exception(
                "Failed to query active datasets | source_system=%s | error=%s",
                source_system, exc,
            )
            return []

    # ------------------------------------------------------------------
    # Runtime state update
    # ------------------------------------------------------------------

    def _update_runtime_state(
        self,
        dataset_cfg:     dict[str, Any],
        result:          dict[str, Any],
        catalog:         str,
        metadata_schema: str,
        run_id:          str,
    ) -> None:
        """
        Append one row to ingestion_runtime_state for this dataset run.

        Uses INSERT INTO (append-only). One row per dataset per run.
        Non-fatal — failure to write runtime state does not abort ingestion.
        """
        runtime_table = f"{catalog}.{metadata_schema}.ingestion_runtime_state"
        dataset_id    = dataset_cfg.get("dataset_id", "")
        dataset_name  = dataset_cfg.get("dataset_name", "UNKNOWN")
        now           = datetime.now(timezone.utc).isoformat()
        status        = result.get("status", "FAILED")
        rows_ingested = result.get("rows_ingested", 0)
        duration      = result.get("duration_seconds", 0)
        error_msg     = (result.get("error_message") or "").replace("'", "''")[:2000]
        environment   = self._env_config.get("environment", "dev")

        sql = f"""
            INSERT INTO {runtime_table} (
                run_id, dataset_id, environment, layer,
                start_time, end_time, duration_seconds,
                status, rows_ingested, rows_failed_dq,
                source_file_count, error_message, created_at
            ) VALUES (
                '{run_id}',
                '{dataset_id}',
                '{environment}',
                'bronze',
                '{now}',
                '{now}',
                {duration},
                '{status}',
                {rows_ingested},
                0,
                {result.get("source_file_count", 0)},
                '{error_msg}',
                '{now}'
            )
        """

        try:
            self.spark.sql(sql)
            logger.debug(
                "Runtime state written | dataset=%s | status=%s | rows=%d",
                dataset_name, status, rows_ingested,
            )
        except Exception as exc:
            # Non-fatal: log warning but do not raise — ingestion already done
            logger.warning(
                "Failed to write runtime state | dataset=%s | error=%s",
                dataset_name, exc,
            )

    # ------------------------------------------------------------------
    # Environment config loader
    # ------------------------------------------------------------------

    def _load_env_config(self, environment: str) -> dict[str, Any]:
        """
        Load environment YAML config from configs/environments/{env}.yml.
        Tries the path relative to this file first, then falls back to
        the Databricks workspace bundle path.
        """
        # Path relative to this source file (local / bundle)
        local_path = (
            Path(__file__).parents[2]
            / "configs" / "environments" / f"{environment}.yml"
        )

        if local_path.exists():
            config_path = local_path
        else:
            # Databricks workspace path when running from synced bundle
            config_path = Path(
                f"/Workspace/Users/sushanth.gangadhara@blumetra.com"
                f"/.bundle/dynamic-databricks-ingestion/{environment}/files"
                f"/configs/environments/{environment}.yml"
            )

        if not Path(config_path).exists():
            logger.warning(
                "Environment config not found at %s — using defaults", config_path
            )
            return {
                "environment": environment,
                "unity_catalog": {
                    "catalog_name":    f"{environment}_catalog",
                    "metadata_schema": "ingestion_metadata",
                },
                "ingestion": {
                    "schema_evolution_mode": "addNewColumns",
                    "max_files_per_trigger": 1000,
                },
            }

        with open(config_path, "r", encoding="utf-8") as fh:
            config = yaml.safe_load(fh)

        logger.info("Environment config loaded from: %s", config_path)
        return config
