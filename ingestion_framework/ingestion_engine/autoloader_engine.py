"""
autoloader_engine.py
====================
Core file ingestion engine for the Dynamic Databricks Ingestion Framework.

Responsibilities
----------------
1. Discover source files on AWS S3 based on dataset configuration.
2. Select the appropriate Spark reader based on file format.
3. Support Databricks Auto Loader (cloudFiles) for incremental ingestion.
4. Support batch overwrite for full-load datasets.
5. Apply per-format reader options from ingestion_config.
6. Add standard metadata columns to every ingested DataFrame.
7. Write the resulting DataFrame to the target Delta table.

Supported formats : csv, json, parquet, avro, delta
Load types        : incremental (Auto Loader), full (batch overwrite)
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
INCREMENTAL_LOAD  = "incremental"
FULL_LOAD         = "full"

METADATA_COL_SOURCE_FILE    = "_source_file"
METADATA_COL_INGESTION_DATE = "_ingestion_date"
METADATA_COL_INGESTION_TS   = "_ingestion_timestamp"


# ---------------------------------------------------------------------------
# AutoLoaderEngine
# ---------------------------------------------------------------------------

class AutoLoaderEngine:
    """
    Orchestrates file ingestion from AWS S3 into Delta Bronze tables.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session provided by the Databricks cluster.
    env_config : dict[str, Any]
        Environment configuration from configs/environments/{env}.yml.
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

        Parameters
        ----------
        dataset_config : dict[str, Any]
            Merged configuration from ingestion_config + dataset_registry.

        Returns
        -------
        dict[str, Any]
            ``{status, rows_ingested, source_file_count, error_message}``
        """
        dataset_name = dataset_config["dataset_name"]
        load_type    = dataset_config.get("load_type", INCREMENTAL_LOAD)
        fmt          = dataset_config.get("format", "").lower()

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
            logger.exception("Ingestion failed | dataset=%s | error=%s", dataset_name, exc)
            return {
                "status": "FAILED",
                "rows_ingested": 0,
                "source_file_count": 0,
                "error_message": str(exc),
            }

    # ------------------------------------------------------------------
    # Incremental ingestion — Databricks Auto Loader
    # ------------------------------------------------------------------

    def _ingest_incremental(self, cfg: dict[str, Any]) -> dict[str, Any]:
        """
        Read files incrementally using Databricks Auto Loader (cloudFiles).

        Auto Loader tracks processed files via a checkpoint directory stored
        in a Unity Catalog Volume. The inferred schema is stored in a schema
        location Volume to avoid re-inference on every run.

        trigger(availableNow=True) processes all backlog then stops —
        ideal for scheduled jobs (no always-on streaming cluster needed).
        """
        source_path        = self._build_source_path(cfg)
        fmt                = cfg["format"].lower()
        reader_options     = self._build_reader_options(cfg)
        schema_location    = cfg["schema_location"]
        checkpoint_location= cfg["checkpoint_location"]
        target_table       = cfg["target_table"]
        max_files          = self._ingestion_cfg.get("max_files_per_trigger", 1000)
        schema_evo_mode    = self._schema_evolution_mode()

        logger.info(
            "Auto Loader | source=%s | schema_loc=%s | checkpoint=%s | target=%s",
            source_path, schema_location, checkpoint_location, target_table,
        )

        # Build the Auto Loader streaming reader
        stream_reader = (
            self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", fmt)
            .option("cloudFiles.schemaLocation", schema_location)
            .option("cloudFiles.schemaEvolutionMode", schema_evo_mode)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.maxFilesPerTrigger", str(max_files))
            .option("rescuedDataColumn", "_rescued_data")
        )

        # Apply format-specific reader options
        for k, v in reader_options.items():
            stream_reader = stream_reader.option(k, v)

        # Load from S3 source
        df = stream_reader.load(source_path)

        # Add standard metadata columns
        df = self._add_metadata_columns(df)

        # Ensure target schema exists before writing
        self._ensure_schema_exists(target_table)

        # Write stream — availableNow processes backlog then stops
        query = (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_location)
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(target_table)
        )

        # Wait for the streaming query to finish processing
        query.awaitTermination()

        # Get row count from the written table
        rows_ingested = self.spark.table(target_table).count()

        logger.info(
            "Auto Loader complete | dataset=%s | rows=%d",
            cfg["dataset_name"], rows_ingested,
        )
        return {"rows_ingested": rows_ingested, "source_file_count": -1}
        # source_file_count = -1 means "managed by Auto Loader checkpoint"

    # ------------------------------------------------------------------
    # Full load — batch overwrite
    # ------------------------------------------------------------------

    def _ingest_full(self, cfg: dict[str, Any]) -> dict[str, Any]:
        """
        Read all files in the source path and overwrite the target Delta table.

        Used for datasets where load_type = full — entire table is replaced
        on each run. Safe to re-run (idempotent).
        """
        source_path    = self._build_source_path(cfg)
        fmt            = cfg["format"].lower()
        reader_options = self._build_reader_options(cfg)
        target_table   = cfg["target_table"]

        logger.info(
            "Batch full load | source=%s | format=%s | target=%s",
            source_path, fmt, target_table,
        )

        # Read all files from S3
        df = (
            self.spark.read
            .format(fmt)
            .options(**reader_options)
            .load(source_path)
        )

        # Add standard metadata columns
        df = self._add_metadata_columns(df)

        # Count before write (batch reads are not lazy after load)
        rows_ingested = df.count()

        # Ensure target schema exists
        self._ensure_schema_exists(target_table)

        # Overwrite entire table — safe, idempotent full refresh
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )

        logger.info(
            "Batch full load complete | dataset=%s | rows=%d",
            cfg["dataset_name"], rows_ingested,
        )
        return {"rows_ingested": rows_ingested, "source_file_count": -1}

    # ------------------------------------------------------------------
    # Reader options builder
    # ------------------------------------------------------------------

    def _build_reader_options(self, cfg: dict[str, Any]) -> dict[str, str]:
        """
        Build Spark reader options dict from dataset configuration.
        Merges format-specific defaults with per-dataset overrides.
        """
        fmt     = cfg.get("format", "").lower()
        options: dict[str, str] = {}

        if fmt == "csv":
            options["header"]      = str(cfg.get("header", True)).lower()
            options["delimiter"]   = cfg.get("delimiter", ",")
            options["encoding"]    = cfg.get("encoding", "UTF-8")
            options["inferSchema"] = "true"
            # Optional overrides from config
            if cfg.get("date_format"):
                options["dateFormat"] = cfg["date_format"]
            if cfg.get("timestamp_format"):
                options["timestampFormat"] = cfg["timestamp_format"]
            if cfg.get("null_value"):
                options["nullValue"] = cfg["null_value"]
            if cfg.get("escape_char"):
                options["escape"] = cfg["escape_char"]
            if cfg.get("quote_char"):
                options["quote"] = cfg["quote_char"]

        elif fmt == "json":
            options["encoding"]  = cfg.get("encoding", "UTF-8")
            options["multiLine"] = str(cfg.get("multiline", False)).lower()
            if cfg.get("date_format"):
                options["dateFormat"] = cfg["date_format"]
            if cfg.get("timestamp_format"):
                options["timestampFormat"] = cfg["timestamp_format"]

        elif fmt == "parquet":
            options["mergeSchema"] = "true"

        elif fmt == "avro":
            options["ignoreExtension"] = "false"

        elif fmt == "delta":
            pass  # Delta reads rarely need additional options

        return options

    # ------------------------------------------------------------------
    # Source path builder
    # ------------------------------------------------------------------

    def _build_source_path(self, cfg: dict[str, Any]) -> str:
        """Build the full s3://bucket/path/ URI from config components."""
        bucket = cfg["s3_bucket"]
        path   = cfg.get("s3_path", "").strip("/")
        return f"s3://{bucket}/{path}/"

    # ------------------------------------------------------------------
    # Metadata columns
    # ------------------------------------------------------------------

    def _add_metadata_columns(self, df: "DataFrame") -> "DataFrame":
        """
        Append standard audit columns to every ingested DataFrame.

        Columns added
        -------------
        _source_file        : originating S3 file path
        _ingestion_date     : date of the ingestion run
        _ingestion_timestamp: precise UTC timestamp of ingestion
        """
        from pyspark.sql import functions as F

        return (
            df
            .withColumn(METADATA_COL_SOURCE_FILE,    F.input_file_name())
            .withColumn(METADATA_COL_INGESTION_DATE, F.current_date())
            .withColumn(METADATA_COL_INGESTION_TS,   F.current_timestamp())
        )

    # ------------------------------------------------------------------
    # Schema / catalog helpers
    # ------------------------------------------------------------------

    def _ensure_schema_exists(self, target_table: str) -> None:
        """
        Create the target schema (database) if it does not exist.

        target_table format: catalog.schema.table
        This extracts catalog.schema and issues CREATE SCHEMA IF NOT EXISTS.
        """
        parts = target_table.split(".")
        if len(parts) == 3:
            catalog_schema = f"{parts[0]}.{parts[1]}"
            try:
                self.spark.sql(
                    f"CREATE SCHEMA IF NOT EXISTS {catalog_schema}"
                )
                logger.debug("Schema ensured: %s", catalog_schema)
            except Exception as exc:
                # Non-fatal — table write will surface the real error if schema missing
                logger.warning(
                    "Could not create schema %s: %s", catalog_schema, exc
                )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_config(self, cfg: dict[str, Any]) -> None:
        """
        Validate required config keys and value constraints.

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
                    f"Missing required key '{key}' for dataset "
                    f"'{cfg.get('dataset_name', 'UNKNOWN')}'"
                )

        fmt = cfg["format"].lower()
        if fmt not in SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format '{fmt}' for dataset '{cfg['dataset_name']}'. "
                f"Supported: {SUPPORTED_FORMATS}"
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
                        f"'{vol_key}' is required for incremental datasets. "
                        f"Dataset: '{cfg['dataset_name']}'"
                    )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _schema_evolution_mode(self) -> str:
        """Return schema evolution mode from env config. Default: addNewColumns."""
        return self._ingestion_cfg.get("schema_evolution_mode", "addNewColumns")
