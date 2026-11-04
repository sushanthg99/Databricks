"""
bronze_ingestion.py
===================
Bronze layer top-level ingestion orchestrator.

Responsibilities
----------------
1. Accept job/notebook parameters:
   - ``source_system``: filter datasets by source (e.g., ``ecommerce``)
   - ``environment``: target environment (``dev``, ``uat``, ``prod``)
   - ``run_mode``: ``incremental``, ``full``, or ``metadata_only``
   - ``dataset_name``: optional — run a single dataset instead of all active ones
2. Load environment configuration from ``configs/environments/{env}.yml``.
3. Query Unity Catalog metadata tables to get the list of active datasets
   for the given ``source_system`` and ``environment``.
4. Optionally run the metadata loader first (when ``run_mode=metadata_only``
   or as a pre-step before ingestion).
5. Loop through each active dataset and call ``BronzeLoader.load()``.
6. Track per-dataset execution results.
7. Write a summary row to ``ingestion_runtime_state`` for each dataset.
8. A single dataset failure must NOT abort the loop — log and continue.

Usage (from notebook)
---------------------
    from src.bronze.bronze_ingestion import BronzeIngestion

    ingestion = BronzeIngestion(spark=spark, dbutils=dbutils)
    ingestion.run(
        source_system="ecommerce",
        environment="dev",
        run_mode="incremental",
    )

Parameters (Databricks widget / job parameter)
----------------------------------------------
source_system   : str  — required
environment     : str  — required (dev | uat | prod)
run_mode        : str  — optional, default "incremental"
dataset_name    : str  — optional, run a single named dataset only

TODO (Phase 1)
--------------
- Implement ``_query_active_datasets()`` SQL query.
- Implement ``_update_runtime_state()`` INSERT into ingestion_runtime_state.
- Add backfill run_mode support (OQ-011).
"""

from __future__ import annotations

import logging
import time
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
        Active Spark session.
    dbutils : object
        Databricks ``dbutils`` object for widget access and secrets.
        Pass ``None`` when running outside Databricks (e.g., unit tests).
    """

    def __init__(self, spark: "SparkSession", dbutils: Any = None) -> None:
        self.spark = spark
        self.dbutils = dbutils
        self._env_config: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def run(
        self,
        source_system: str,
        environment: str,
        run_mode: str = "incremental",
        dataset_name: str | None = None,
    ) -> dict[str, Any]:
        """
        Execute Bronze ingestion for active datasets in the given source system.

        Parameters
        ----------
        source_system : str
            Source system identifier. Filters ``dataset_registry`` by this value.
        environment : str
            Target environment (``dev``, ``uat``, ``prod``).
        run_mode : str
            Ingestion mode: ``incremental``, ``full``, or ``metadata_only``.
        dataset_name : str, optional
            If provided, only this dataset is processed (useful for reruns).

        Returns
        -------
        dict[str, Any]
            Run summary: ``{total, succeeded, failed, skipped, duration_seconds}``
        """
        start_time = time.time()
        logger.info(
            "BronzeIngestion.run() | source_system=%s | env=%s | mode=%s",
            source_system, environment, run_mode,
        )

        self._env_config = self._load_env_config(environment)
        catalog = self._env_config["unity_catalog"]["catalog_name"]
        metadata_schema = self._env_config["unity_catalog"]["metadata_schema"]

        # Optional: run metadata loader as a pre-step
        if run_mode == "metadata_only":
            return self._run_metadata_only(catalog, metadata_schema, environment)

        # Query active datasets from metadata tables
        datasets = self._query_active_datasets(
            catalog=catalog,
            metadata_schema=metadata_schema,
            source_system=source_system,
            environment=environment,
            dataset_name_filter=dataset_name,
        )

        if not datasets:
            logger.warning(
                "No active datasets found | source_system=%s | env=%s",
                source_system, environment,
            )
            return {"total": 0, "succeeded": 0, "failed": 0, "skipped": 0,
                    "duration_seconds": 0}

        logger.info("Active datasets found: %d", len(datasets))

        # Ingestion loop — failure of one dataset does NOT abort others
        succeeded = failed = skipped = 0
        for dataset_cfg in datasets:
            result = self._ingest_one(
                dataset_cfg=dataset_cfg,
                run_mode=run_mode,
                catalog=catalog,
                metadata_schema=metadata_schema,
            )
            if result["status"] == "SUCCESS":
                succeeded += 1
            elif result["status"] == "SKIPPED":
                skipped += 1
            else:
                failed += 1

        duration = round(time.time() - start_time, 2)
        summary = {
            "total": len(datasets),
            "succeeded": succeeded,
            "failed": failed,
            "skipped": skipped,
            "duration_seconds": duration,
        }
        logger.info("BronzeIngestion complete | %s", summary)
        return summary

    # ------------------------------------------------------------------
    # Single dataset ingestion
    # ------------------------------------------------------------------

    def _ingest_one(
        self,
        dataset_cfg: dict[str, Any],
        run_mode: str,
        catalog: str,
        metadata_schema: str,
    ) -> dict[str, Any]:
        """
        Ingest a single dataset and update its runtime state.

        Parameters
        ----------
        dataset_cfg : dict[str, Any]
            Merged dataset config from metadata tables.
        run_mode : str
            Ingestion mode override (``incremental`` | ``full``).
        catalog : str
            Unity Catalog catalog name.
        metadata_schema : str
            Metadata schema name.

        Returns
        -------
        dict[str, Any]
            ``{status, rows_ingested, error_message, duration_seconds}``
        """
        from src.bronze.bronze_loader import BronzeLoader  # noqa: PLC0415

        dataset_name = dataset_cfg.get("dataset_name", "UNKNOWN")
        start = time.time()

        logger.info("Processing dataset: %s", dataset_name)

        try:
            loader = BronzeLoader(
                spark=self.spark,
                env_config=self._env_config,
                catalog=catalog,
                metadata_schema=metadata_schema,
            )
            result = loader.load(dataset_cfg=dataset_cfg, run_mode=run_mode)
            result["duration_seconds"] = round(time.time() - start, 2)

        except Exception as exc:
            logger.exception("Dataset ingestion failed: %s | error=%s", dataset_name, exc)
            result = {
                "status": "FAILED",
                "rows_ingested": 0,
                "error_message": str(exc),
                "duration_seconds": round(time.time() - start, 2),
            }

        self._update_runtime_state(
            dataset_cfg=dataset_cfg,
            result=result,
            catalog=catalog,
            metadata_schema=metadata_schema,
        )
        return result

    # ------------------------------------------------------------------
    # Metadata-only run
    # ------------------------------------------------------------------

    def _run_metadata_only(
        self,
        catalog: str,
        metadata_schema: str,
        environment: str,
    ) -> dict[str, Any]:
        """
        Run only the metadata loader step — register YAML configs into
        Unity Catalog without executing any ingestion.

        Parameters
        ----------
        catalog : str
        metadata_schema : str
        environment : str

        Returns
        -------
        dict[str, Any]
            Metadata loader summary.

        TODO
        ----
        Implement by instantiating MetadataLoader and calling load_from_file()
        for each config file in configs/.
        """
        from ingestion_framework.metadata_service.metadata_loader import (  # noqa: PLC0415
            MetadataLoader,
        )

        loader = MetadataLoader(
            spark=self.spark,
            catalog=catalog,
            metadata_schema=metadata_schema,
            environment=environment,
        )

        config_path = Path(__file__).parents[2] / "configs" / "domain_config.yml"
        result = loader.load_from_file(config_path)
        logger.info("Metadata-only run complete | %s", result)
        return result

    # ------------------------------------------------------------------
    # Metadata table queries
    # ------------------------------------------------------------------

    def _query_active_datasets(
        self,
        catalog: str,
        metadata_schema: str,
        source_system: str,
        environment: str,
        dataset_name_filter: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Query Unity Catalog metadata tables for active Bronze datasets.

        Joins ``dataset_registry`` and ``ingestion_config`` to build a
        merged configuration dict per dataset.

        Parameters
        ----------
        catalog : str
        metadata_schema : str
        source_system : str
        environment : str
        dataset_name_filter : str, optional
            Limit results to a single dataset name.

        Returns
        -------
        list[dict[str, Any]]
            List of merged dataset + ingestion config dicts.

        TODO (Phase 1)
        --------------
        Implement SQL query:
            SELECT dr.*, ic.*
            FROM {catalog}.{metadata_schema}.dataset_registry dr
            JOIN {catalog}.{metadata_schema}.ingestion_config ic
              ON dr.dataset_id = ic.dataset_id
            WHERE dr.source_system = '{source_system}'
              AND dr.active = true
              AND dr.data_tier = 'bronze'
              AND ic.environment = '{environment}'
              [AND dr.dataset_name = '{dataset_name_filter}']
        """
        logger.debug(
            "_query_active_datasets() not yet implemented. Returning empty list."
        )
        # TODO: Implement
        # query = f"""
        #     SELECT dr.dataset_id, dr.dataset_name, dr.source_system, dr.criticality,
        #            ic.load_type, ic.ingestion_mode, ic.s3_bucket, ic.s3_path,
        #            ic.format, ic.delimiter, ic.encoding, ic.header, ic.multiline,
        #            ic.schema_location, ic.checkpoint_location,
        #            ic.primary_keys, ic.watermark_column, ic.target_table
        #     FROM {catalog}.{metadata_schema}.dataset_registry dr
        #     JOIN {catalog}.{metadata_schema}.ingestion_config ic
        #       ON dr.dataset_id = ic.dataset_id
        #     WHERE dr.source_system = '{source_system}'
        #       AND dr.active = true
        #       AND dr.data_tier = 'bronze'
        #       AND ic.environment = '{environment}'
        # """
        # if dataset_name_filter:
        #     query += f" AND dr.dataset_name = '{dataset_name_filter}'"
        # rows = self.spark.sql(query).collect()
        # return [row.asDict() for row in rows]
        return []

    def _update_runtime_state(
        self,
        dataset_cfg: dict[str, Any],
        result: dict[str, Any],
        catalog: str,
        metadata_schema: str,
    ) -> None:
        """
        Append a run record to ``ingestion_runtime_state``.

        Parameters
        ----------
        dataset_cfg : dict[str, Any]
        result : dict[str, Any]
        catalog : str
        metadata_schema : str

        TODO (Phase 1)
        --------------
        Implement INSERT INTO {catalog}.{metadata_schema}.ingestion_runtime_state
        with run_id, dataset_id, layer, status, rows_ingested, error_message, timestamps.
        """
        logger.debug(
            "_update_runtime_state() | dataset=%s | status=%s",
            dataset_cfg.get("dataset_name"), result.get("status"),
        )
        # TODO: Implement
        pass

    # ------------------------------------------------------------------
    # Environment config
    # ------------------------------------------------------------------

    def _load_env_config(self, environment: str) -> dict[str, Any]:
        """
        Load the environment-specific YAML configuration.

        Parameters
        ----------
        environment : str
            Environment name (``dev``, ``uat``, ``prod``).

        Returns
        -------
        dict[str, Any]
            Parsed environment config dict.

        Raises
        ------
        FileNotFoundError
            If the environment config file does not exist.
        """
        config_path = (
            Path(__file__).parents[2]
            / "configs"
            / "environments"
            / f"{environment}.yml"
        )
        if not config_path.exists():
            raise FileNotFoundError(
                f"Environment config not found: {config_path}. "
                f"Valid environments: dev, uat, prod"
            )
        with config_path.open("r", encoding="utf-8") as fh:
            return yaml.safe_load(fh)
