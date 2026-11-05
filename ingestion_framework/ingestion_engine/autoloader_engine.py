"""
autoloader_engine.py
====================
Core file ingestion engine for the Dynamic Databricks Ingestion Framework.

Responsibilities
----------------
1. Discover source files based on dataset configuration.
2. Select the appropriate Spark reader based on file format.
3. Support Databricks Auto Loader (cloudFiles) for incremental ingestion.
4. Support batch overwrite for full-load datasets.
5. Apply per-format reader options from ingestion_config.
6. Add standard metadata columns to every ingested DataFrame.
7. Write the resulting DataFrame to the target Delta table.

Supported source types
----------------------
- volume: /Volumes/catalog/schema/vol/path/ — Unity Catalog Volume (demo)
- dbfs  : dbfs:/FileStore/...               — Databricks File System (legacy)
- s3    : s3://bucket/path/                 — AWS S3 (production)
- abfss : abfss://...                       — Azure Data Lake (future)

Supported formats : csv, json, parquet, avro, delta, tsv
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

SUPPORTED_FORMATS  = {"csv", "json", "parquet", "avro", "delta", "tsv"}
INCREMENTAL_LOAD   = "incremental"
FULL_LOAD          = "full"

SOURCE_TYPE_DBFS   = "dbfs"
SOURCE_TYPE_S3     = "s3"
SOURCE_TYPE_ABFSS  = "abfss"
SOURCE_TYPE_VOLUME = "volume"

METADATA_COL_SOURCE_FILE    = "_source_file"
METADATA_COL_INGESTION_DATE = "_ingestion_date"
METADATA_COL_INGESTION_TS   = "_ingestion_timestamp"


# ---------------------------------------------------------------------------
# AutoLoaderEngine
# ---------------------------------------------------------------------------

class AutoLoaderEngine:
    """
    Orchestrates file ingestion from DBFS or AWS S3 into Delta Bronze tables.

    Supports both Databricks Auto Loader (incremental) and batch overwrite
    (full load). Source type is driven by dataset configuration:
      - source_type: dbfs  → reads from DBFS (demo mode, no S3 needed)
      - source_type: s3    → reads from AWS S3 (production)

    Parameters
    ----------
    spark : SparkSession
        Active Spark session provided by the Databricks cluster.
    env_config : dict[str, Any]
        Environment configuration from configs/environments/{env}.yml.
    """

    def __init__(self, spark: "SparkSession", env_config: dict[str, Any]) -> None:
        self.spark        = spark
        self.env_config   = env_config
        self._ingestion_cfg = env_config.get("ingestion", {})

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def ingest(self, dataset_config: dict[str, Any]) -> dict[str, Any]:
        """
        Ingest a single dataset into a Delta Bronze table.

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
        source_type  = dataset_config.get("source_type", SOURCE_TYPE_S3).lower()

        logger.info(
            "Starting ingestion | dataset=%s | source_type=%s | load_type=%s | format=%s",
            dataset_name, source_type, load_type, fmt,
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
            return {
                "status":            "FAILED",
                "rows_ingested":     0,
                "source_file_count": 0,
                "error_message":     str(exc),
            }

    # ------------------------------------------------------------------
    # Incremental ingestion — Databricks Auto Loader
    # ------------------------------------------------------------------

    def _ingest_incremental(self, cfg: dict[str, Any]) -> dict[str, Any]:
        """
        Read files incrementally using Databricks Auto Loader (cloudFiles).

        Works for both DBFS (demo) and S3 (production) source types.
        trigger(availableNow=True) processes all backlog then stops —
        ideal for scheduled jobs (no always-on streaming cluster needed).
        """
        source_path         = self._build_source_path(cfg)
        fmt                 = self._resolve_format(cfg)
        reader_options      = self._build_reader_options(cfg)
        schema_location     = cfg["schema_location"]
        checkpoint_location = cfg["checkpoint_location"]
        target_table        = cfg["target_table"]
        max_files           = self._ingestion_cfg.get("max_files_per_trigger", 1000)
        schema_evo_mode     = self._schema_evolution_mode()

        logger.info(
            "Auto Loader | source=%s | fmt=%s | schema_loc=%s | checkpoint=%s | target=%s",
            source_path, fmt, schema_location, checkpoint_location, target_table,
        )

        # Build Auto Loader streaming reader
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

        df = stream_reader.load(source_path)
        df = self._add_metadata_columns(df)

        self._ensure_schema_exists(target_table)

        # availableNow = process all backlog then stop (batch-like streaming)
        query = (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_location)
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(target_table)
        )
        query.awaitTermination()

        rows_ingested = self.spark.table(target_table).count()
        logger.info(
            "Auto Loader complete | dataset=%s | rows=%d",
            cfg["dataset_name"], rows_ingested,
        )
        return {"rows_ingested": rows_ingested, "source_file_count": -1}

    # ------------------------------------------------------------------
    # Full load — batch overwrite
    # ------------------------------------------------------------------

    def _ingest_full(self, cfg: dict[str, Any]) -> dict[str, Any]:
        """
        Read all files in the source path and overwrite the target Delta table.

        Works for both DBFS (demo) and S3 (production).
        Entire table is replaced on each run — safe to re-run (idempotent).
        """
        source_path    = self._build_source_path(cfg)
        fmt            = self._resolve_format(cfg)
        reader_options = self._build_reader_options(cfg)
        target_table   = cfg["target_table"]

        logger.info(
            "Batch full load | source=%s | format=%s | target=%s",
            source_path, fmt, target_table,
        )

        df = (
            self.spark.read
            .format(fmt)
            .options(**reader_options)
            .load(source_path)
        )

        df = self._add_metadata_columns(df)
        rows_ingested = df.count()

        self._ensure_schema_exists(target_table)

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
    # Source path builder — supports DBFS, S3, ABFSS
    # ------------------------------------------------------------------

    def _build_source_path(self, cfg: dict[str, Any]) -> str:
        """
        Build the source path URI based on source_type in the dataset config.

        source_type = volume → /Volumes/catalog/schema/volume/path/  (UC Volume)
        source_type = dbfs   → dbfs:/FileStore/{dbfs_path}/
        source_type = s3     → s3://{s3_bucket}/{s3_path}/
        source_type = abfss  → uses abfss_path directly

        Default is s3 for backward compatibility.
        """
        source_type = cfg.get("source_type", SOURCE_TYPE_S3).lower()

        if source_type == SOURCE_TYPE_VOLUME:
            # Unity Catalog Volume — direct path, no prefix needed
            path = cfg.get("volume_path", "").rstrip("/") + "/"
            logger.debug("UC Volume source path: %s", path)
            return path

        elif source_type == SOURCE_TYPE_DBFS:
            dbfs_path = cfg.get("dbfs_path", "").strip("/")
            path = f"dbfs:/{dbfs_path}/"
            logger.debug("DBFS source path: %s", path)
            return path

        elif source_type == SOURCE_TYPE_ABFSS:
            path = cfg.get("abfss_path", "")
            logger.debug("ABFSS source path: %s", path)
            return path

        else:
            # Default: AWS S3
            bucket = cfg["s3_bucket"]
            s3path = cfg.get("s3_path", "").strip("/")
            path = f"s3://{bucket}/{s3path}/"
            logger.debug("S3 source path: %s", path)
            return path

    # ------------------------------------------------------------------
    # Format resolver — TSV is CSV with tab delimiter
    # ------------------------------------------------------------------

    def _resolve_format(self, cfg: dict[str, Any]) -> str:
        """
        Resolve the Spark reader format.
        TSV files are read using the 'csv' format with delimiter=\\t.
        """
        fmt = cfg.get("format", "csv").lower()
        return "csv" if fmt == "tsv" else fmt

    # ------------------------------------------------------------------
    # Reader options builder
    # ------------------------------------------------------------------

    def _build_reader_options(self, cfg: dict[str, Any]) -> dict[str, str]:
        """
        Build Spark reader options from dataset configuration.

        TSV is treated as CSV with tab delimiter.
        Merges format-specific defaults with per-dataset overrides.
        """
        fmt     = cfg.get("format", "csv").lower()
        options: dict[str, str] = {}

        if fmt in ("csv", "tsv"):
            # TSV uses tab as delimiter; CSV uses comma by default
            default_delimiter = "\t" if fmt == "tsv" else ","
            options["header"]      = str(cfg.get("header", True)).lower()
            options["delimiter"]   = cfg.get("delimiter", default_delimiter)
            options["encoding"]    = cfg.get("encoding", "UTF-8")
            options["inferSchema"] = "true"
            if cfg.get("date_format"):
                options["dateFormat"] = cfg["date_format"]
            if cfg.get("timestamp_format"):
                options["timestampFormat"] = cfg["timestamp_format"]
            if cfg.get("null_value"):
                options["nullValue"] = cfg["null_value"]

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
            pass

        return options

    # ------------------------------------------------------------------
    # Metadata columns
    # ------------------------------------------------------------------

    def _add_metadata_columns(self, df: "DataFrame") -> "DataFrame":
        """
        Append standard audit columns to every ingested DataFrame.

        _source_file        : originating file path (DBFS or S3)
        _ingestion_date     : date of the ingestion run
        _ingestion_timestamp: precise UTC timestamp
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
        Create the target catalog.schema if it does not exist.
        Handles both 3-part (catalog.schema.table) and 2-part (schema.table).
        """
        parts = target_table.split(".")
        if len(parts) >= 2:
            catalog_schema = ".".join(parts[:-1])
            try:
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_schema}")
                logger.debug("Schema ensured: %s", catalog_schema)
            except Exception as exc:
                logger.warning(
                    "Could not create schema %s: %s", catalog_schema, exc
                )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_config(self, cfg: dict[str, Any]) -> None:
        """
        Validate required config keys.

        source_type = dbfs  → requires dbfs_path
        source_type = s3    → requires s3_bucket + s3_path
        All types require: dataset_name, format, load_type, target_table
        """
        always_required = ["dataset_name", "format", "load_type", "target_table"]
        for key in always_required:
            if not cfg.get(key):
                raise ValueError(
                    f"Missing required key '{key}' for dataset "
                    f"'{cfg.get('dataset_name', 'UNKNOWN')}'"
                )

        source_type = cfg.get("source_type", SOURCE_TYPE_S3).lower()

        if source_type == SOURCE_TYPE_VOLUME:
            if not cfg.get("volume_path"):
                raise ValueError(
                    f"'volume_path' is required when source_type=volume. "
                    f"Dataset: '{cfg.get('dataset_name')}'"
                )
        elif source_type == SOURCE_TYPE_DBFS:
            if not cfg.get("dbfs_path"):
                raise ValueError(
                    f"'dbfs_path' is required when source_type=dbfs. "
                    f"Dataset: '{cfg.get('dataset_name')}'"
                )
        elif source_type == SOURCE_TYPE_S3:
            for key in ("s3_bucket", "s3_path"):
                if not cfg.get(key):
                    raise ValueError(
                        f"'{key}' is required when source_type=s3. "
                        f"Dataset: '{cfg.get('dataset_name')}'"
                    )

        fmt = cfg.get("format", "").lower()
        if fmt not in SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format '{fmt}' for dataset '{cfg['dataset_name']}'. "
                f"Supported: {SUPPORTED_FORMATS}"
            )

        load_type = cfg.get("load_type", "").lower()
        if load_type not in (INCREMENTAL_LOAD, FULL_LOAD):
            raise ValueError(
                f"Invalid load_type '{load_type}' for dataset "
                f"'{cfg['dataset_name']}'. Must be 'incremental' or 'full'."
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
        """Return schema evolution mode from env config."""
        return self._ingestion_cfg.get("schema_evolution_mode", "addNewColumns")
