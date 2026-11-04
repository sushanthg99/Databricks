"""
autoloader_engine.py
====================
Core file ingestion engine for the Dynamic Databricks Ingestion Framework.

Responsibilities
----------------
1. Discover source files on AWS S3 based on dataset configuration.
2. Select the appropriate Spark reader based on the configured file format
   (CSV, JSON, Parquet, Avro, Delta).
3. Support Databricks Auto Loader (``cloudFiles``) for incremental ingestion —
   tracks processed files via a Unity Catalog Volume checkpoint and stores
   the inferred schema in a Unity Catalog Volume schema location.
4. Support batch overwrite mode for full-load datasets.
5. Apply per-format reader options (delimiter, encoding, header, multiline, etc.)
   from the ingestion configuration.
6. Add standard metadata columns to every ingested DataFrame:
   - ``_source_file``: originating file path (``input_file_name()``)
   - ``_ingestion_date``: date of ingestion run (``current_date()``)
   - ``_ingestion_timestamp``: precise ingestion timestamp (``current_timestamp()``)
7. Write the resulting DataFrame to the target Delta table using the
   mode specified by ``load_type``.

Supported formats
-----------------
csv, json, parquet, avro, delta

Load types
----------
- ``incremental``: Auto Loader streaming with ``trigger(availableNow=True)``
- ``full``: ``spark.read`` + ``mode=overwrite``

Dependencies
------------
- PySpark (provided by Databricks Runtime)
- Unity Catalog External Location for S3 access (no hardcoded credentials)

Notes
-----
- Schema evolution mode is configurable via ``schema_evolution_mode`` in the
  environment config (``addNewColumns`` | ``failOnNewColumns`` | ``rescue``).
- All reader options are passed through from ``ingestion_config`` without
  hardcoding format-specific defaults here (options are merged at call time).

TODO (OQ-004)
-------------
Confirm the full set of reader options that must be supported per format.
Currently implementing: delimiter, encoding, header, multiline, inferSchema.
Add additional options (dateFormat, timestampFormat, escape, quote, nullValue)
once confirmed.

TODO (OQ-008)
-------------
Implement ``replaceWhere`` support for full-load datasets that should only
overwrite a specific partition rather than the entire table.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SUPPORTED_FORMATS = {"csv", "json", "parquet", "avro", "delta"}

INCREMENTAL_LOAD = "incremental"
FULL_LOAD = "full"

METADATA_COL_SOURCE_FILE = "_source_file"
METADATA_COL_INGESTION_DATE = "_ingestion_date"
METADATA_COL_INGESTION_TS = "_ingestion_timestamp"


# ---------------------------------------------------------------------------
# AutoLoaderEngine
# ---------------------------------------------------------------------------


class AutoLoaderEngine:
    """
    Orchestrates file ingestion from AWS S3 into Delta Bronze tables.

    Supports both Databricks Auto Loader (incremental) and batch overwrite
    (full load) based on the dataset's ``load_type`` configuration.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session provided by the Databricks cluster.
    env_config : dict[str, Any]
        Environment-level configuration loaded from
        ``configs/environments/{env}.yml``.
    """

    def __init__(self, spark: "SparkSession", env_config: dict[str, Any]) -> None:
        self.spark = spark
        self.env_config = env_config
        self._ingestion_cfg = env_config.get("ingestion", {})

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def ingest(self, dataset_config: dict[str, Any]) -> dict[str, Any]:
        """
        Ingest a single dataset from S3 into a Delta Bronze table.

        Selects the ingestion path (Auto Loader vs batch) based on
        ``load_type`` in *dataset_config*, applies reader options,
        adds metadata columns, and writes to the target Delta table.

        Parameters
        ----------
        dataset_config : dict[str, Any]
            A single dataset's merged configuration containing fields from
            ``ingestion_config`` and ``dataset_registry``.

        Returns
        -------
        dict[str, Any]
            Execution metrics: ``rows_ingested``, ``status``, ``error_message``.

        Raises
        ------
        ValueError
            If ``format`` is not in ``SUPPORTED_FORMATS`` or ``load_type``
            is not ``incremental`` / ``full``.
        RuntimeError
            If the write operation fails.
        """
        dataset_name: str = dataset_config["dataset_name"]
        load_type: str = dataset_config.get("load_type", INCREMENTAL_LOAD)
        fmt: str = dataset_config.get("format", "").lower()

        logger.info(
            "Starting ingestion | dataset=%s | load_type=%s | format=%s",
            dataset_name, load_type, fmt,
        )

        self._validate_config(dataset_config)

        try:
            if load_type == INCREMENTAL_LOAD:
                metrics = self._ingest_incremental(dataset_config)
            else:
                metrics = self._ingest_full(dataset_config)

            logger.info(
                "Ingestion complete | dataset=%s | rows=%s | status=SUCCESS",
                dataset_name, metrics.get("rows_ingested", "unknown"),
            )
            return {"status": "SUCCESS", "error_message": None, **metrics}

        except Exception as exc:
            logger.exception(
                "Ingestion failed | dataset=%s | error=%s", dataset_name, exc
            )
            return {"status": "FAILED", "rows_ingested": 0, "error_message": str(exc)}

    # ------------------------------------------------------------------
    # Incremental ingestion (Auto Loader)
    # ------------------------------------------------------------------

    def _ingest_incremental(self, cfg: dict[str, Any]) -> dict[str, Any]:
        """
        Read files incrementally using Databricks Auto Loader (``cloudFiles``).

        Auto Loader tracks processed files via the checkpoint location and
        stores the inferred/evolved schema at the schema location. Both paths
        point to Unity Catalog Volumes for governed storage.

        Parameters
        ----------
        cfg : dict[str, Any]
            Dataset ingestion configuration.

        Returns
        -------
        dict[str, Any]
            Ingestion metrics dict with ``rows_ingested`` and ``source_file_count``.

        TODO
        ----
        - Add ``trigger(availableNow=True)`` for scheduled batch-style streaming.
        - Add ``maxFilesPerTrigger`` from env_config.
        - Confirm schema evolution mode options with the team (OQ-004).
        """
        source_path = self._build_source_path(cfg)
        fmt = cfg["format"].lower()
        reader_options = self._build_reader_options(cfg)
        schema_location = cfg["schema_location"]
        checkpoint_location = cfg["checkpoint_location"]
        target_table = cfg["target_table"]

        logger.debug(
            "Auto Loader config | source=%s | schema_loc=%s | checkpoint=%s",
            source_path, schema_location, checkpoint_location,
        )

        # TODO: Implement Auto Loader streaming read
        # df = (
        #     self.spark.readStream
        #     .format("cloudFiles")
        #     .option("cloudFiles.format", fmt)
        #     .option("cloudFiles.schemaLocation", schema_location)
        #     .option("cloudFiles.schemaEvolutionMode", self._schema_evolution_mode())
        #     .option("cloudFiles.inferColumnTypes", "true")
        #     .option("rescuedDataColumn", "_rescued_data")
        #     .options(**reader_options)
        #     .load(source_path)
        # )
        # df = self._add_metadata_columns(df)
        # query = (
        #     df.writeStream
        #     .format("delta")
        #     .outputMode("append")
        #     .option("checkpointLocation", checkpoint_location)
        #     .option("mergeSchema", "true")
        #     .trigger(availableNow=True)
        #     .toTable(target_table)
        # )
        # query.awaitTermination()

        raise NotImplementedError(
            "Auto Loader ingestion not yet implemented. "
            "See TODO block in _ingest_incremental(). Phase 1 implementation task."
        )

    # ------------------------------------------------------------------
    # Full load (batch overwrite)
    # ------------------------------------------------------------------

    def _ingest_full(self, cfg: dict[str, Any]) -> dict[str, Any]:
        """
        Read all files in the source path and overwrite the target Delta table.

        Used for datasets where ``load_type = full`` — the entire table is
        replaced on each run. Suitable for daily full snapshots.

        Parameters
        ----------
        cfg : dict[str, Any]
            Dataset ingestion configuration.

        Returns
        -------
        dict[str, Any]
            Ingestion metrics dict with ``rows_ingested``.

        TODO (OQ-008)
        -------------
        Add ``replaceWhere`` support for partition-level overwrite.
        """
        source_path = self._build_source_path(cfg)
        fmt = cfg["format"].lower()
        reader_options = self._build_reader_options(cfg)
        target_table = cfg["target_table"]

        logger.debug("Batch read | source=%s | format=%s", source_path, fmt)

        # TODO: Implement batch read + overwrite
        # df = (
        #     self.spark.read
        #     .format(fmt)
        #     .options(**reader_options)
        #     .load(source_path)
        # )
        # df = self._add_metadata_columns(df)
        # df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
        # return {"rows_ingested": df.count()}

        raise NotImplementedError(
            "Batch overwrite ingestion not yet implemented. "
            "See TODO block in _ingest_full(). Phase 1 implementation task."
        )

    # ------------------------------------------------------------------
    # Reader options
    # ------------------------------------------------------------------

    def _build_reader_options(self, cfg: dict[str, Any]) -> dict[str, str]:
        """
        Build a dict of Spark reader options from the dataset configuration.

        Merges format-specific defaults with per-dataset overrides from
        ``ingestion_config``.

        Parameters
        ----------
        cfg : dict[str, Any]
            Dataset ingestion configuration.

        Returns
        -------
        dict[str, str]
            Flat key-value dict of Spark reader options.

        TODO (OQ-004)
        -------------
        Confirm full set of required reader options and add them here.
        """
        fmt = cfg.get("format", "").lower()
        options: dict[str, str] = {}

        if fmt == "csv":
            options["header"] = str(cfg.get("header", True)).lower()
            options["delimiter"] = cfg.get("delimiter", ",")
            options["encoding"] = cfg.get("encoding", "UTF-8")
            options["inferSchema"] = "true"
            # TODO: Add dateFormat, timestampFormat, escape, quote, nullValue

        elif fmt == "json":
            options["encoding"] = cfg.get("encoding", "UTF-8")
            options["multiLine"] = str(cfg.get("multiline", False)).lower()
            # TODO: Add dateFormat, timestampFormat, primitivesAsString

        elif fmt == "parquet":
            options["mergeSchema"] = "true"

        elif fmt == "avro":
            pass  # TODO: Add Avro-specific options

        elif fmt == "delta":
            pass  # Delta reads rarely need additional options

        return options

    # ------------------------------------------------------------------
    # Source path builder
    # ------------------------------------------------------------------

    def _build_source_path(self, cfg: dict[str, Any]) -> str:
        """
        Build the full S3 source path from bucket and path components.

        Parameters
        ----------
        cfg : dict[str, Any]
            Dataset configuration containing ``s3_bucket`` and ``s3_path``.

        Returns
        -------
        str
            Full ``s3://bucket/path/`` URI.
        """
        bucket: str = cfg["s3_bucket"]
        path: str = cfg.get("s3_path", "").strip("/")
        return f"s3://{bucket}/{path}/"

    # ------------------------------------------------------------------
    # Metadata columns
    # ------------------------------------------------------------------

    def _add_metadata_columns(self, df: "DataFrame") -> "DataFrame":
        """
        Add standard audit/metadata columns to the ingested DataFrame.

        Columns added
        -------------
        _source_file        : originating file path
        _ingestion_date     : date the data was ingested
        _ingestion_timestamp: precise ingestion timestamp

        Parameters
        ----------
        df : DataFrame
            Input DataFrame from the file reader.

        Returns
        -------
        DataFrame
            DataFrame with metadata columns appended.
        """
        from pyspark.sql import functions as F  # noqa: PLC0415 — deferred import

        return (
            df
            .withColumn(METADATA_COL_SOURCE_FILE, F.input_file_name())
            .withColumn(METADATA_COL_INGESTION_DATE, F.current_date())
            .withColumn(METADATA_COL_INGESTION_TS, F.current_timestamp())
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_config(self, cfg: dict[str, Any]) -> None:
        """
        Validate that required configuration keys are present and values
        are within expected ranges.

        Parameters
        ----------
        cfg : dict[str, Any]
            Dataset ingestion configuration.

        Raises
        ------
        ValueError
            If a required key is missing or has an unsupported value.
        """
        required_keys = [
            "dataset_name", "format", "load_type",
            "s3_bucket", "s3_path", "target_table",
        ]
        for key in required_keys:
            if not cfg.get(key):
                raise ValueError(
                    f"Missing required ingestion config key: '{key}' "
                    f"for dataset '{cfg.get('dataset_name', 'UNKNOWN')}'"
                )

        fmt = cfg["format"].lower()
        if fmt not in SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format '{fmt}' for dataset '{cfg['dataset_name']}'. "
                f"Supported formats: {SUPPORTED_FORMATS}"
            )

        load_type = cfg.get("load_type", "").lower()
        if load_type not in (INCREMENTAL_LOAD, FULL_LOAD):
            raise ValueError(
                f"Invalid load_type '{load_type}' for dataset '{cfg['dataset_name']}'. "
                f"Must be '{INCREMENTAL_LOAD}' or '{FULL_LOAD}'."
            )

        if load_type == INCREMENTAL_LOAD:
            for vol_key in ("schema_location", "checkpoint_location"):
                if not cfg.get(vol_key):
                    raise ValueError(
                        f"'{vol_key}' is required for incremental load datasets. "
                        f"Dataset: '{cfg['dataset_name']}'"
                    )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _schema_evolution_mode(self) -> str:
        """Return the configured schema evolution mode from env config."""
        return self._ingestion_cfg.get("schema_evolution_mode", "addNewColumns")
