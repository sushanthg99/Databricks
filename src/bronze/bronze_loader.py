"""
bronze_loader.py
================
Bronze layer dataset loader — delegates to the ingestion engine.

Responsibilities
----------------
1. Receive a merged dataset configuration from ``BronzeIngestion``.
2. Override ``load_type`` if the caller passes ``run_mode=full`` (manual override).
3. Instantiate ``AutoLoaderEngine`` and call ``ingest()``.
4. After write, instantiate ``SchemaRegistry`` and call ``check_and_register()``
   to detect and record any schema changes.
5. Return ingestion metrics (rows ingested, status, error message) to
   the calling ``BronzeIngestion`` loop.

Design note
-----------
This module is intentionally thin — it does not contain Spark read/write logic.
All file reading and Delta writing is handled by ``AutoLoaderEngine``.
``BronzeLoader`` exists to apply Bronze-specific concerns (schema check, metrics)
without polluting the reusable engine.

TODO (Phase 1)
--------------
- Implement full ``load()`` method once ``AutoLoaderEngine.ingest()`` is complete.
- Add post-write row count retrieval.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class BronzeLoader:
    """
    Thin orchestration wrapper around ``AutoLoaderEngine`` and ``SchemaRegistry``
    for the Bronze ingestion layer.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    env_config : dict[str, Any]
        Environment configuration loaded from ``configs/environments/{env}.yml``.
    catalog : str
        Unity Catalog catalog name.
    metadata_schema : str
        Metadata schema name (e.g., ``ingestion_metadata``).
    """

    def __init__(
        self,
        spark: "SparkSession",
        env_config: dict[str, Any],
        catalog: str,
        metadata_schema: str,
    ) -> None:
        self.spark = spark
        self.env_config = env_config
        self.catalog = catalog
        self.metadata_schema = metadata_schema

    def load(
        self,
        dataset_cfg: dict[str, Any],
        run_mode: str = "incremental",
    ) -> dict[str, Any]:
        """
        Load one dataset from S3 into a Delta Bronze table.

        Orchestrates:
        1. Resolve load_type from dataset config or run_mode override.
        2. Call ``AutoLoaderEngine.ingest()``.
        3. Call ``SchemaRegistry.check_and_register()`` after successful write.
        4. Return metrics.

        Parameters
        ----------
        dataset_cfg : dict[str, Any]
            Merged ingestion config for one dataset.
        run_mode : str
            If ``"full"``, overrides dataset ``load_type`` to ``full``.

        Returns
        -------
        dict[str, Any]
            ``{status, rows_ingested, schema_changed, error_message}``

        TODO (Phase 1)
        --------------
        - Implement after AutoLoaderEngine.ingest() is built.
        - Add post-write row count via:
              self.spark.table(target_table).count()
          or from streaming query progress metrics.
        """
        from ingestion_framework.ingestion_engine.autoloader_engine import (  # noqa: PLC0415
            AutoLoaderEngine,
        )
        from ingestion_framework.ingestion_engine.schema_registry import (  # noqa: PLC0415
            SchemaRegistry,
        )

        dataset_name = dataset_cfg.get("dataset_name", "UNKNOWN")

        # Apply run_mode override
        effective_cfg = dict(dataset_cfg)
        if run_mode == "full":
            effective_cfg["load_type"] = "full"
            logger.info(
                "load_type overridden to 'full' by run_mode | dataset=%s", dataset_name
            )

        # Step 1: Ingest via AutoLoaderEngine
        engine = AutoLoaderEngine(spark=self.spark, env_config=self.env_config)
        ingest_result = engine.ingest(effective_cfg)

        if ingest_result["status"] == "FAILED":
            logger.error("Ingestion failed for dataset: %s", dataset_name)
            return ingest_result

        # Step 2: Check and register schema
        schema_result = {"schema_changed": False, "version_number": None}
        try:
            registry = SchemaRegistry(
                spark=self.spark,
                catalog=self.catalog,
                metadata_schema=self.metadata_schema,
            )
            # TODO: Get current schema from the written Delta table
            # current_schema = self.spark.table(effective_cfg["target_table"]).schema
            # schema_result = registry.check_and_register(
            #     dataset_id=effective_cfg["dataset_id"],
            #     dataset_name=dataset_name,
            #     current_schema=current_schema,
            # )
            logger.debug(
                "Schema registry check skipped — not yet implemented for dataset: %s",
                dataset_name,
            )
        except Exception as exc:
            # Schema registry failure is non-fatal — log and continue
            logger.warning(
                "Schema registry check failed (non-fatal) | dataset=%s | error=%s",
                dataset_name, exc,
            )

        return {
            **ingest_result,
            "schema_changed": schema_result.get("schema_changed", False),
            "schema_version": schema_result.get("version_number"),
        }
