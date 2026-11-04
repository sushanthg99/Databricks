"""
metadata_loader.py
==================
YAML-to-Unity-Catalog metadata registration service.

Responsibilities
----------------
1. Read one or more YAML configuration files from the ``configs/`` directory.
2. Parse and validate each dataset configuration block.
3. Compute a SHA-256 hash of the configuration to detect changes.
4. Compare the hash against the existing row in ``ingestion_config``.
5. If unchanged → skip (avoid unnecessary Delta writes).
6. If changed or new → issue a ``MERGE INTO`` to update the following
   Unity Catalog metadata tables:
   - ``dataset_registry``   : dataset identity and classification
   - ``dataset_tags``       : key-value tags
   - ``ingestion_config``   : ingestion parameters and file locations
7. Optionally register initial DQ rules into ``dq_sla_config``.

Deduplication strategy
----------------------
Each dataset configuration is serialised to a canonical JSON string and
hashed with SHA-256. The hash is stored in ``ingestion_config.config_hash``.
On subsequent runs, only configurations whose hash differs are re-merged.

This avoids unnecessary Delta log entries and MERGE overhead when configs
haven't changed (e.g., daily metadata loader runs where most datasets
are unchanged).

Dataset ID generation
---------------------
``dataset_id`` is derived as a deterministic UUID (UUID v5, namespace DNS)
from ``{source_system}::{dataset_name}``. This ensures the same dataset
always gets the same ID across environments and re-runs, making the
registration idempotent. See OQ-001 in ``docs/open_questions.md``.

YAML file location
------------------
Configs are loaded from ``configs/domain_config.yml`` (file-based) or
``configs/api_config.yml`` (API sources). The loader accepts a list of
YAML file paths.

See also
--------
- docs/metadata_design.md
- docs/ingestion_workflow.md — Phase 0: Configuration Registration

TODO
----
- Implement ``_merge_dataset_registry()`` SQL MERGE.
- Implement ``_merge_ingestion_config()`` SQL MERGE.
- Implement ``_merge_dataset_tags()`` SQL MERGE (delete + insert approach).
- Implement ``_merge_dq_rules()`` SQL MERGE for initial DQ rule registration.
- Add Pydantic validation model for dataset config schema.
- Confirm dataset_id generation strategy (OQ-001).
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# UUID namespace for deterministic dataset_id generation (OQ-001)
_UUID_NAMESPACE = uuid.NAMESPACE_DNS


# ---------------------------------------------------------------------------
# MetadataLoader
# ---------------------------------------------------------------------------


class MetadataLoader:
    """
    Loads YAML dataset configurations and registers them into Unity Catalog
    metadata tables using SHA-256-based change detection.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    catalog : str
        Unity Catalog catalog name (e.g., ``dev_catalog``).
    metadata_schema : str
        Schema name for metadata tables (e.g., ``ingestion_metadata``).
    environment : str
        Target environment (``dev``, ``uat``, ``prod``).
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

        # Fully qualified table references
        self._meta_prefix = f"{catalog}.{metadata_schema}"
        self._registry_table = f"{self._meta_prefix}.dataset_registry"
        self._tags_table = f"{self._meta_prefix}.dataset_tags"
        self._ingestion_config_table = f"{self._meta_prefix}.ingestion_config"
        self._dq_table = f"{self._meta_prefix}.dq_sla_config"

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def load_from_file(self, yaml_path: str | Path) -> dict[str, Any]:
        """
        Load, validate, and register all dataset configurations from a
        single YAML file.

        Parameters
        ----------
        yaml_path : str or Path
            Absolute or relative path to the YAML configuration file.

        Returns
        -------
        dict[str, Any]
            Summary: ``{total, registered, skipped, failed}``.
        """
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
        """
        Register a list of dataset configuration dicts.

        Iterates over each dataset config, computes the hash, and conditionally
        merges into Unity Catalog metadata tables.

        Parameters
        ----------
        dataset_configs : list[dict[str, Any]]
            List of parsed dataset configuration dicts.
        source_system : str
            Source system name (injected if not already in each config).

        Returns
        -------
        dict[str, Any]
            Summary: ``{total, registered, skipped, failed}``.
        """
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
                logger.exception(
                    "Failed to register dataset '%s': %s", dataset_name, exc
                )
                failed += 1

        logger.info(
            "Metadata load complete | total=%d | registered=%d | skipped=%d | failed=%d",
            total, registered, skipped, failed,
        )
        return {
            "total": total,
            "registered": registered,
            "skipped": skipped,
            "failed": failed,
        }

    # ------------------------------------------------------------------
    # Single dataset processing
    # ------------------------------------------------------------------

    def _process_single(self, cfg: dict[str, Any]) -> str:
        """
        Process one dataset configuration entry.

        1. Validate required fields.
        2. Generate deterministic dataset_id.
        3. Compute SHA-256 hash.
        4. Check existing hash in ingestion_config.
        5. If changed/new → MERGE into metadata tables.
        6. Return ``"registered"`` or ``"skipped"``.

        Parameters
        ----------
        cfg : dict[str, Any]
            Single dataset configuration dict.

        Returns
        -------
        str
            ``"registered"`` if MERGE was executed, ``"skipped"`` if hash matched.
        """
        dataset_name: str = cfg["dataset_name"]
        self._validate_required_fields(cfg)

        dataset_id = self._generate_dataset_id(
            source_system=cfg["source_system"],
            dataset_name=dataset_name,
        )
        cfg["dataset_id"] = dataset_id

        new_hash = self._compute_hash(cfg)
        existing_hash = self._get_existing_hash(dataset_id)

        if existing_hash == new_hash:
            logger.debug("Config unchanged | dataset=%s | hash=%s", dataset_name, new_hash[:8])
            return "skipped"

        logger.info(
            "Config changed | dataset=%s | old_hash=%s | new_hash=%s",
            dataset_name,
            (existing_hash or "NONE")[:8],
            new_hash[:8],
        )

        cfg["config_hash"] = new_hash
        self._register(cfg)
        return "registered"

    def _register(self, cfg: dict[str, Any]) -> None:
        """
        Execute MERGE INTO for all relevant metadata tables for one dataset.

        Parameters
        ----------
        cfg : dict[str, Any]
            Dataset configuration with ``dataset_id`` and ``config_hash`` set.
        """
        self._merge_dataset_registry(cfg)
        self._merge_dataset_tags(cfg)
        self._merge_ingestion_config(cfg)

        if cfg.get("data_quality"):
            self._merge_dq_rules(cfg)

    # ------------------------------------------------------------------
    # Hash computation
    # ------------------------------------------------------------------

    def _compute_hash(self, cfg: dict[str, Any]) -> str:
        """
        Compute a stable SHA-256 hash of the dataset configuration.

        The configuration is serialised to a canonical JSON string (sorted keys)
        before hashing to ensure consistency regardless of dict insertion order.

        Parameters
        ----------
        cfg : dict[str, Any]
            Dataset configuration dict.

        Returns
        -------
        str
            Hex-encoded SHA-256 hash string.
        """
        # Exclude runtime-injected fields that should not affect the hash
        exclude_keys = {"dataset_id", "config_hash", "environment"}
        hashable = {k: v for k, v in cfg.items() if k not in exclude_keys}
        canonical = json.dumps(hashable, sort_keys=True, default=str)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    # ------------------------------------------------------------------
    # Dataset ID generation
    # ------------------------------------------------------------------

    @staticmethod
    def _generate_dataset_id(source_system: str, dataset_name: str) -> str:
        """
        Generate a deterministic UUID v5 for a dataset.

        Uses the combination ``{source_system}::{dataset_name}`` as the name
        within the DNS namespace. This ensures the same dataset always gets
        the same ID across environments and re-runs.

        Parameters
        ----------
        source_system : str
            Source system identifier.
        dataset_name : str
            Dataset name.

        Returns
        -------
        str
            UUID string (lowercase, hyphenated).
        """
        name = f"{source_system}::{dataset_name}"
        return str(uuid.uuid5(_UUID_NAMESPACE, name))

    # ------------------------------------------------------------------
    # Existing hash lookup
    # ------------------------------------------------------------------

    def _get_existing_hash(self, dataset_id: str) -> str | None:
        """
        Retrieve the stored ``config_hash`` for a dataset from
        ``ingestion_config``.

        Parameters
        ----------
        dataset_id : str
            UUID of the dataset.

        Returns
        -------
        str or None
            Existing hash, or ``None`` if the dataset is not yet registered.

        TODO
        ----
        Implement the SQL query:
            SELECT config_hash FROM {ingestion_config_table}
            WHERE dataset_id = '{dataset_id}'
            AND environment = '{environment}'
            LIMIT 1
        """
        # TODO: Implement
        # try:
        #     row = self.spark.sql(f"""
        #         SELECT config_hash
        #         FROM {self._ingestion_config_table}
        #         WHERE dataset_id = '{dataset_id}'
        #           AND environment = '{self.environment}'
        #         LIMIT 1
        #     """).first()
        #     return row["config_hash"] if row else None
        # except Exception:
        #     return None  # Table may not exist yet on first run
        logger.debug("_get_existing_hash() not yet implemented. Returning None.")
        return None  # Forces registration path during development

    # ------------------------------------------------------------------
    # MERGE operations — TODO stubs
    # ------------------------------------------------------------------

    def _merge_dataset_registry(self, cfg: dict[str, Any]) -> None:
        """
        MERGE INTO ``dataset_registry`` for the given dataset.

        Match condition: ``dataset_id``
        On match: update ``active``, ``version``, ``updated_at``
        On no match: insert full row

        TODO
        ----
        Implement using Spark SQL MERGE INTO:
            MERGE INTO {registry_table} AS target
            USING (SELECT ...) AS source
            ON target.dataset_id = source.dataset_id
            WHEN MATCHED THEN UPDATE SET ...
            WHEN NOT MATCHED THEN INSERT ...
        """
        logger.debug("_merge_dataset_registry() | dataset=%s", cfg["dataset_name"])
        # TODO: Implement MERGE INTO dataset_registry
        raise NotImplementedError(
            "_merge_dataset_registry() not yet implemented. Phase 1 implementation task."
        )

    def _merge_dataset_tags(self, cfg: dict[str, Any]) -> None:
        """
        Update ``dataset_tags`` for the given dataset.

        Strategy: delete existing tags for this ``dataset_id``, then insert
        all new tags from the YAML config. This handles tag additions,
        removals, and value changes cleanly.

        TODO
        ----
        Implement:
            DELETE FROM {tags_table} WHERE dataset_id = '{dataset_id}'
            INSERT INTO {tags_table} VALUES (...)
        Or use a MERGE with a composite key of (dataset_id, tag_key).
        """
        logger.debug("_merge_dataset_tags() | dataset=%s", cfg["dataset_name"])
        # TODO: Implement tag refresh
        pass  # Non-fatal if not yet implemented — tags are supplementary

    def _merge_ingestion_config(self, cfg: dict[str, Any]) -> None:
        """
        MERGE INTO ``ingestion_config`` for the given dataset + environment.

        Match condition: ``dataset_id`` + ``environment``
        On match: update all ingestion parameters and ``config_hash``
        On no match: insert full row

        TODO
        ----
        Implement the MERGE INTO statement. The ``config_hash`` column must be
        updated on every merge so the next run can detect changes.
        """
        logger.debug("_merge_ingestion_config() | dataset=%s", cfg["dataset_name"])
        # TODO: Implement MERGE INTO ingestion_config
        raise NotImplementedError(
            "_merge_ingestion_config() not yet implemented. Phase 1 implementation task."
        )

    def _merge_dq_rules(self, cfg: dict[str, Any]) -> None:
        """
        Register initial DQ rules into ``dq_sla_config`` for a new dataset.

        Only inserts rules that don't already exist (matched by
        ``dataset_id + rule_name``). Does not overwrite manually curated
        rule thresholds.

        TODO
        ----
        Implement MERGE INTO:
            ON target.dataset_id = source.dataset_id
            AND target.rule_name = source.rule_name
            WHEN NOT MATCHED THEN INSERT ...
        """
        logger.debug(
            "_merge_dq_rules() | dataset=%s | rules=%d",
            cfg["dataset_name"], len(cfg.get("data_quality", [])),
        )
        # TODO: Implement MERGE INTO dq_sla_config
        pass  # Non-fatal if not yet implemented

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_required_fields(self, cfg: dict[str, Any]) -> None:
        """
        Validate that required top-level fields are present in the config.

        Parameters
        ----------
        cfg : dict[str, Any]
            Dataset configuration dict.

        Raises
        ------
        ValueError
            If any required field is missing.

        TODO
        ----
        Replace with a Pydantic model for full type + constraint validation.
        """
        required = [
            "dataset_name", "version", "data_tier",
            "source_system", "active", "format",
        ]
        missing = [k for k in required if cfg.get(k) is None]
        if missing:
            raise ValueError(
                f"Missing required fields {missing} in config for dataset "
                f"'{cfg.get('dataset_name', 'UNKNOWN')}'"
            )
