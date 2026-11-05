"""
metadata_loader.py
==================
YAML-to-Unity-Catalog metadata registration service.

Responsibilities
----------------
1. Read YAML configuration files from the ``configs/`` directory.
2. Parse and validate each dataset configuration block.
3. Compute a SHA-256 hash of the configuration to detect changes.
4. Compare the hash against the existing row in ``ingestion_config``.
5. If unchanged → skip. If changed or new → MERGE INTO metadata tables.

Tables written
--------------
- dataset_registry   : dataset identity and classification
- dataset_tags       : key-value tags
- ingestion_config   : ingestion parameters
- dq_sla_config      : initial DQ rule definitions
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_UUID_NAMESPACE = uuid.NAMESPACE_DNS


class MetadataLoader:
    """
    Loads YAML dataset configurations and registers them into Unity Catalog
    metadata tables using SHA-256-based change detection.

    Parameters
    ----------
    spark : SparkSession
    catalog : str
    metadata_schema : str
    environment : str
    """

    def __init__(
        self,
        spark: "SparkSession",
        catalog: str,
        metadata_schema: str,
        environment: str,
    ) -> None:
        self.spark = spark
        self.catalog = catalog
        self.metadata_schema = metadata_schema
        self.environment = environment

        self._meta_prefix = f"{catalog}.{metadata_schema}"
        self._registry_table = f"{self._meta_prefix}.dataset_registry"
        self._tags_table = f"{self._meta_prefix}.dataset_tags"
        self._ingestion_config_table = f"{self._meta_prefix}.ingestion_config"
        self._dq_table = f"{self._meta_prefix}.dq_sla_config"

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def load_from_file(self, yaml_path: str | Path) -> dict[str, Any]:
        """Load, validate, and register all datasets from a YAML file."""
        path = Path(yaml_path)
        logger.info("Loading metadata from: %s", path)

        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        with path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)

        source_system: str = raw.get("source_system", "unknown")
        datasets: list[dict[str, Any]] = raw.get("datasets", [])

        if not datasets:
            logger.warning("No datasets found in config file: %s", path)
            return {"total": 0, "registered": 0, "skipped": 0, "failed": 0}

        return self.load_from_list(datasets, source_system=source_system)

    def load_from_list(
        self,
        dataset_configs: list[dict[str, Any]],
        source_system: str = "unknown",
    ) -> dict[str, Any]:
        """Register a list of dataset configuration dicts."""
        total = len(dataset_configs)
        registered = skipped = failed = 0

        for cfg in dataset_configs:
            dataset_name = cfg.get("dataset_name", "UNKNOWN")
            cfg.setdefault("source_system", source_system)
            cfg.setdefault("environment", self.environment)

            try:
                result = self._process_single(cfg)
                if result == "skipped":
                    skipped += 1
                else:
                    registered += 1
            except Exception as exc:
                logger.exception("Failed to register dataset '%s': %s", dataset_name, exc)
                failed += 1

        logger.info(
            "Metadata load complete | total=%d | registered=%d | skipped=%d | failed=%d",
            total, registered, skipped, failed,
        )
        return {"total": total, "registered": registered, "skipped": skipped, "failed": failed}

    # ------------------------------------------------------------------
    # Single dataset processing
    # ------------------------------------------------------------------

    def _process_single(self, cfg: dict[str, Any]) -> str:
        """Process one dataset — hash check then MERGE if changed."""
        self._validate_required_fields(cfg)

        dataset_id = self._generate_dataset_id(cfg["source_system"], cfg["dataset_name"])
        cfg["dataset_id"] = dataset_id

        new_hash = self._compute_hash(cfg)
        existing_hash = self._get_existing_hash(dataset_id)

        if existing_hash == new_hash:
            logger.debug(
                "Config unchanged — skipping | dataset=%s | hash=%s",
                cfg["dataset_name"], new_hash[:8],
            )
            return "skipped"

        logger.info(
            "Config changed — registering | dataset=%s | old=%s | new=%s",
            cfg["dataset_name"],
            (existing_hash or "NONE")[:8],
            new_hash[:8],
        )
        cfg["config_hash"] = new_hash
        self._register(cfg)
        return "registered"

    def _register(self, cfg: dict[str, Any]) -> None:
        """Execute all MERGE operations for one dataset."""
        self._merge_dataset_registry(cfg)
        self._merge_dataset_tags(cfg)
        self._merge_ingestion_config(cfg)
        if cfg.get("data_quality"):
            self._merge_dq_rules(cfg)

    # ------------------------------------------------------------------
    # Hash + ID
    # ------------------------------------------------------------------

    def _compute_hash(self, cfg: dict[str, Any]) -> str:
        """SHA-256 hash of config (excluding runtime-injected keys)."""
        exclude = {"dataset_id", "config_hash", "environment"}
        hashable = {k: v for k, v in cfg.items() if k not in exclude}
        canonical = json.dumps(hashable, sort_keys=True, default=str)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _generate_dataset_id(source_system: str, dataset_name: str) -> str:
        """Deterministic UUID v5 from source_system::dataset_name."""
        return str(uuid.uuid5(_UUID_NAMESPACE, f"{source_system}::{dataset_name}"))

    # ------------------------------------------------------------------
    # Existing hash lookup
    # ------------------------------------------------------------------

    def _get_existing_hash(self, dataset_id: str) -> str | None:
        """
        Retrieve stored config_hash from ingestion_config.
        Returns None if the table doesn't exist yet or no row found.
        """
        try:
            row = self.spark.sql(f"""
                SELECT config_hash
                FROM {self._ingestion_config_table}
                WHERE dataset_id = '{dataset_id}'
                  AND environment = '{self.environment}'
                LIMIT 1
            """).first()
            return row["config_hash"] if row else None
        except Exception:
            # Table may not exist on first run — treat as new registration
            return None

    # ------------------------------------------------------------------
    # MERGE: dataset_registry
    # ------------------------------------------------------------------

    def _merge_dataset_registry(self, cfg: dict[str, Any]) -> None:
        """
        MERGE INTO dataset_registry.
        Match on dataset_id. Update on match, insert on new.
        """
        now = datetime.now(timezone.utc).isoformat()
        dataset_id      = cfg["dataset_id"]
        dataset_name    = cfg["dataset_name"]
        version         = cfg.get("version", "1.0")
        dataset_type    = cfg.get("dataset_type", "")
        data_tier       = cfg.get("data_tier", "bronze")
        source_system   = cfg.get("source_system", "")
        description     = cfg.get("business_description", "").replace("'", "''")
        criticality     = cfg.get("criticality", "medium")
        classification  = cfg.get("classification", "internal")
        pii             = str(cfg.get("pii", False)).lower()
        active          = str(cfg.get("active", True)).lower()

        sql = f"""
        MERGE INTO {self._registry_table} AS target
        USING (
            SELECT
                '{dataset_id}'    AS dataset_id,
                '{dataset_name}'  AS dataset_name,
                '{version}'       AS version,
                '{dataset_type}'  AS dataset_type,
                '{data_tier}'     AS data_tier,
                '{source_system}' AS source_system,
                '{description}'   AS business_description,
                '{criticality}'   AS criticality,
                '{classification}'AS classification,
                {pii}             AS pii,
                {active}          AS active,
                '{now}'           AS updated_at
        ) AS source
        ON target.dataset_id = source.dataset_id
        WHEN MATCHED THEN UPDATE SET
            target.version             = source.version,
            target.dataset_type        = source.dataset_type,
            target.data_tier           = source.data_tier,
            target.business_description= source.business_description,
            target.criticality         = source.criticality,
            target.classification      = source.classification,
            target.pii                 = source.pii,
            target.active              = source.active,
            target.updated_at          = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            dataset_id, dataset_name, version, dataset_type, data_tier,
            source_system, business_description, criticality, classification,
            pii, active, created_at, updated_at, created_by
        ) VALUES (
            source.dataset_id, source.dataset_name, source.version,
            source.dataset_type, source.data_tier, source.source_system,
            source.business_description, source.criticality,
            source.classification, source.pii, source.active,
            '{now}', '{now}', 'ingestion_framework'
        )
        """
        self.spark.sql(sql)
        logger.debug("dataset_registry merged | dataset=%s", dataset_name)

    # ------------------------------------------------------------------
    # MERGE: dataset_tags
    # ------------------------------------------------------------------

    def _merge_dataset_tags(self, cfg: dict[str, Any]) -> None:
        """
        Refresh dataset_tags: delete existing rows then insert current tags.
        This handles tag additions, removals, and value changes cleanly.
        """
        tags: dict[str, str] = cfg.get("tags", {})
        dataset_id = cfg["dataset_id"]
        now = datetime.now(timezone.utc).isoformat()

        # Delete all existing tags for this dataset
        self.spark.sql(f"""
            DELETE FROM {self._tags_table}
            WHERE dataset_id = '{dataset_id}'
        """)

        if not tags:
            return

        # Build VALUES rows
        rows = ", ".join(
            f"('{dataset_id}', '{k}', '{str(v).replace(chr(39), chr(39)*2)}', '{now}')"
            for k, v in tags.items()
        )
        self.spark.sql(f"""
            INSERT INTO {self._tags_table}
                (dataset_id, tag_key, tag_value, created_at)
            VALUES {rows}
        """)
        logger.debug(
            "dataset_tags refreshed | dataset=%s | count=%d",
            cfg["dataset_name"], len(tags),
        )

    # ------------------------------------------------------------------
    # MERGE: ingestion_config
    # ------------------------------------------------------------------

    def _merge_ingestion_config(self, cfg: dict[str, Any]) -> None:
        """
        MERGE INTO ingestion_config.
        Match on (dataset_id, environment). Update on match, insert on new.
        The config_hash is always updated so next run detects changes.
        """
        now             = datetime.now(timezone.utc).isoformat()
        config_id       = str(uuid.uuid4())
        dataset_id      = cfg["dataset_id"]
        environment     = self.environment
        ingestion_mode  = cfg.get("ingestion_mode", "auto_loader")
        load_type       = cfg.get("load_type", "incremental")
        source_type     = cfg.get("source_type", "s3")
        volume_path     = cfg.get("volume_path", "")
        s3_bucket       = cfg.get("s3_bucket", "")
        s3_path         = cfg.get("s3_path", "")
        file_pattern    = cfg.get("file_pattern", "")
        fmt             = cfg.get("format", "")
        delimiter       = cfg.get("delimiter", ",")
        encoding        = cfg.get("encoding", "UTF-8")
        header          = str(cfg.get("header", True)).lower()
        multiline       = str(cfg.get("multiline", False)).lower()
        schema_loc      = cfg.get("schema_location", "")
        checkpoint_loc  = cfg.get("checkpoint_location", "")
        primary_keys    = json.dumps(cfg.get("primary_keys", []))
        watermark_col   = ""
        watermark_delay = ""
        if isinstance(cfg.get("watermark"), dict):
            watermark_col   = cfg["watermark"].get("column", "")
            watermark_delay = cfg["watermark"].get("delay", "")
        frequency       = cfg.get("frequency", "daily")
        config_hash     = cfg["config_hash"]

        source_system = cfg.get("source_system", "")
        data_tier     = cfg.get("data_tier", "bronze")
        dataset_name  = cfg["dataset_name"]
        # Use explicit target_table from config if provided, otherwise build it
        target_table  = cfg.get("target_table") or \
                        f"{self.catalog}.{source_system}_{data_tier}.{dataset_name}"

        sql = f"""
        MERGE INTO {self._ingestion_config_table} AS target
        USING (
            SELECT
                '{config_id}'       AS config_id,
                '{dataset_id}'      AS dataset_id,
                '{environment}'     AS environment,
                '{ingestion_mode}'  AS ingestion_mode,
                '{load_type}'       AS load_type,
                '{source_type}'     AS source_type,
                '{volume_path}'     AS volume_path,
                '{s3_bucket}'       AS s3_bucket,
                '{s3_path}'         AS s3_path,
                '{file_pattern}'    AS file_pattern,
                '{fmt}'             AS format,
                '{delimiter}'       AS delimiter,
                '{encoding}'        AS encoding,
                {header}            AS header,
                {multiline}         AS multiline,
                '{schema_loc}'      AS schema_location,
                '{checkpoint_loc}'  AS checkpoint_location,
                '{primary_keys}'    AS primary_keys,
                '{watermark_col}'   AS watermark_column,
                '{watermark_delay}' AS watermark_delay,
                '{frequency}'       AS frequency,
                '{target_table}'    AS target_table,
                '{config_hash}'     AS config_hash,
                '{now}'             AS updated_at
        ) AS source
        ON target.dataset_id   = source.dataset_id
       AND target.environment  = source.environment
        WHEN MATCHED THEN UPDATE SET
            target.ingestion_mode       = source.ingestion_mode,
            target.load_type            = source.load_type,
            target.source_type          = source.source_type,
            target.volume_path          = source.volume_path,
            target.s3_bucket            = source.s3_bucket,
            target.s3_path              = source.s3_path,
            target.file_pattern         = source.file_pattern,
            target.format               = source.format,
            target.delimiter            = source.delimiter,
            target.encoding             = source.encoding,
            target.header               = source.header,
            target.multiline            = source.multiline,
            target.schema_location      = source.schema_location,
            target.checkpoint_location  = source.checkpoint_location,
            target.primary_keys         = source.primary_keys,
            target.watermark_column     = source.watermark_column,
            target.watermark_delay      = source.watermark_delay,
            target.frequency            = source.frequency,
            target.target_table         = source.target_table,
            target.config_hash          = source.config_hash,
            target.updated_at           = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            config_id, dataset_id, environment, ingestion_mode, load_type,
            source_type, volume_path, s3_bucket, s3_path, file_pattern,
            format, delimiter, encoding, header, multiline,
            schema_location, checkpoint_location, primary_keys,
            watermark_column, watermark_delay, frequency,
            target_table, config_hash, created_at, updated_at
        ) VALUES (
            source.config_id, source.dataset_id, source.environment,
            source.ingestion_mode, source.load_type,
            source.source_type, source.volume_path,
            source.s3_bucket, source.s3_path, source.file_pattern,
            source.format, source.delimiter, source.encoding,
            source.header, source.multiline,
            source.schema_location, source.checkpoint_location,
            source.primary_keys, source.watermark_column, source.watermark_delay,
            source.frequency, source.target_table, source.config_hash,
            '{now}', '{now}'
        )
        """
        self.spark.sql(sql)
        logger.debug("ingestion_config merged | dataset=%s", cfg["dataset_name"])

    # ------------------------------------------------------------------
    # MERGE: dq_sla_config
    # ------------------------------------------------------------------

    def _merge_dq_rules(self, cfg: dict[str, Any]) -> None:
        """
        Insert new DQ rules into dq_sla_config.
        Only inserts rules that don't already exist (by dataset_id + rule_name).
        Never overwrites manually curated thresholds.
        """
        rules: list[dict[str, Any]] = cfg.get("data_quality", [])
        dataset_id = cfg["dataset_id"]
        now = datetime.now(timezone.utc).isoformat()

        for rule in rules:
            rule_id         = str(uuid.uuid4())
            rule_name       = rule.get("rule_name", "")
            rule_type       = rule.get("rule_type", "")
            column_name     = rule.get("column_name", "")
            rule_expression = rule.get("rule_expression", "").replace("'", "''")
            threshold_pct   = rule.get("threshold_pct", 100.0)
            fail_on_breach  = str(rule.get("fail_on_breach", False)).lower()
            sla_type        = rule.get("sla_type", "accuracy")

            sql = f"""
            MERGE INTO {self._dq_table} AS target
            USING (
                SELECT
                    '{rule_id}'         AS dq_rule_id,
                    '{dataset_id}'      AS dataset_id,
                    '{rule_name}'       AS rule_name,
                    '{rule_type}'       AS rule_type,
                    '{column_name}'     AS column_name,
                    '{rule_expression}' AS rule_expression,
                    {threshold_pct}     AS threshold_pct,
                    {fail_on_breach}    AS fail_on_breach,
                    '{sla_type}'        AS sla_type,
                    true                AS active,
                    '{now}'             AS created_at
            ) AS source
            ON target.dataset_id = source.dataset_id
           AND target.rule_name  = source.rule_name
            WHEN NOT MATCHED THEN INSERT (
                dq_rule_id, dataset_id, rule_name, rule_type, column_name,
                rule_expression, threshold_pct, fail_on_breach,
                sla_type, active, created_at
            ) VALUES (
                source.dq_rule_id, source.dataset_id, source.rule_name,
                source.rule_type, source.column_name, source.rule_expression,
                source.threshold_pct, source.fail_on_breach,
                source.sla_type, source.active, source.created_at
            )
            """
            self.spark.sql(sql)

        logger.debug(
            "dq_sla_config merged | dataset=%s | rules=%d",
            cfg["dataset_name"], len(rules),
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_required_fields(self, cfg: dict[str, Any]) -> None:
        """Raise ValueError if any required config key is missing."""
        required = ["dataset_name", "version", "data_tier", "source_system", "active", "format"]
        missing = [k for k in required if cfg.get(k) is None]
        if missing:
            raise ValueError(
                f"Missing required fields {missing} in config for dataset "
                f"'{cfg.get('dataset_name', 'UNKNOWN')}'"
            )
