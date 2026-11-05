"""
schema_registry.py
==================
Schema registration and versioning for the Dynamic Databricks Ingestion Framework.

Responsibilities
----------------
1. Compare the current schema of a Bronze Delta table against the latest
   registered schema version in the schema_registry metadata table.
2. Detect schema changes: added columns, dropped columns, type changes.
3. Register a new schema version row when a change is detected.
4. Mark previous versions as is_current = false atomically.
5. Support schema retrieval — return the current registered StructType.

Schema storage format
---------------------
Schemas are stored as JSON strings using Spark's StructType.json() format.
This ensures full compatibility with Auto Loader schema evolution.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)

# Change type constants — match values in schema_registry DDL
CHANGE_INITIAL      = "initial"
CHANGE_ADD_COLUMN   = "add_column"
CHANGE_DROP_COLUMN  = "drop_column"
CHANGE_TYPE_CHANGE  = "type_change"
CHANGE_MIXED        = "mixed"


class SchemaRegistry:
    """
    Manages schema versions for ingested datasets in Unity Catalog.

    Parameters
    ----------
    spark : SparkSession
    catalog : str
    metadata_schema : str
    """

    def __init__(
        self,
        spark:            "SparkSession",
        catalog:          str,
        metadata_schema:  str,
    ) -> None:
        self.spark          = spark
        self.catalog        = catalog
        self.metadata_schema= metadata_schema
        self._registry_table= f"{catalog}.{metadata_schema}.schema_registry"

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def check_and_register(
        self,
        dataset_id:     str,
        dataset_name:   str,
        current_schema: "StructType",
        registered_by:  str = "ingestion_framework",
    ) -> dict[str, Any]:
        """
        Compare current_schema against the registered schema.
        Register a new version if a change is detected.

        Parameters
        ----------
        dataset_id : str
            UUID from dataset_registry.
        dataset_name : str
            Human-readable name for logging.
        current_schema : StructType
            Schema obtained from the written Bronze Delta table.
        registered_by : str
            Identity of the caller.

        Returns
        -------
        dict[str, Any]
            ``{schema_changed, version_number, change_type}``
        """
        logger.info("Checking schema | dataset=%s", dataset_name)

        latest = self._get_latest_version(dataset_id)

        if latest is None:
            # First-time registration
            version_number  = 1
            change_type     = CHANGE_INITIAL
            changed_columns = [f.name for f in current_schema.fields]
            logger.info(
                "First schema registration | dataset=%s | columns=%d",
                dataset_name, len(changed_columns),
            )
        else:
            registered_schema = self._deserialise_schema(latest["schema_json"])
            diff = self._detect_changes(registered_schema, current_schema)

            if not diff["has_changes"]:
                logger.info(
                    "Schema unchanged | dataset=%s | version=%d",
                    dataset_name, latest["version_number"],
                )
                return {
                    "schema_changed": False,
                    "version_number": latest["version_number"],
                    "change_type":    None,
                }

            version_number  = latest["version_number"] + 1
            change_type     = diff["primary_change_type"]
            changed_columns = diff["changed_columns"]
            logger.info(
                "Schema change detected | dataset=%s | change=%s | new_version=%d",
                dataset_name, change_type, version_number,
            )

        self._register_version(
            dataset_id=dataset_id,
            version_number=version_number,
            schema=current_schema,
            change_type=change_type,
            changed_columns=changed_columns,
            registered_by=registered_by,
        )

        return {
            "schema_changed": True,
            "version_number": version_number,
            "change_type":    change_type,
        }

    def get_current_schema(self, dataset_id: str) -> "StructType | None":
        """
        Retrieve the current registered schema for a dataset.

        Returns None if no schema has been registered yet.
        """
        latest = self._get_latest_version(dataset_id)
        if latest is None:
            return None
        return self._deserialise_schema(latest["schema_json"])

    # ------------------------------------------------------------------
    # Schema comparison
    # ------------------------------------------------------------------

    def _detect_changes(
        self,
        old_schema: "StructType",
        new_schema: "StructType",
    ) -> dict[str, Any]:
        """
        Diff two StructTypes and classify what changed.

        Returns
        -------
        dict with keys:
            has_changes          : bool
            primary_change_type  : str | None
            changed_columns      : list[str]
            added                : list[str]
            dropped              : list[str]
            type_changed         : list[str]
        """
        old_fields = {f.name: str(f.dataType) for f in old_schema.fields}
        new_fields = {f.name: str(f.dataType) for f in new_schema.fields}

        added        = [n for n in new_fields if n not in old_fields]
        dropped      = [n for n in old_fields if n not in new_fields]
        type_changed = [
            n for n in old_fields
            if n in new_fields and old_fields[n] != new_fields[n]
        ]

        changed_columns = added + dropped + type_changed
        has_changes     = bool(changed_columns)

        if not has_changes:
            primary_change_type = None
        elif added and not dropped and not type_changed:
            primary_change_type = CHANGE_ADD_COLUMN
        elif dropped and not added and not type_changed:
            primary_change_type = CHANGE_DROP_COLUMN
        elif type_changed and not added and not dropped:
            primary_change_type = CHANGE_TYPE_CHANGE
        else:
            primary_change_type = CHANGE_MIXED

        return {
            "has_changes":          has_changes,
            "primary_change_type":  primary_change_type,
            "changed_columns":      changed_columns,
            "added":                added,
            "dropped":              dropped,
            "type_changed":         type_changed,
        }

    # ------------------------------------------------------------------
    # Persistence — get latest version
    # ------------------------------------------------------------------

    def _get_latest_version(self, dataset_id: str) -> dict[str, Any] | None:
        """
        Query schema_registry for the current (is_current = true) schema
        version of a dataset.

        Returns the row as a dict, or None if no entry exists yet.
        """
        try:
            row = self.spark.sql(f"""
                SELECT
                    schema_version_id,
                    version_number,
                    schema_json,
                    change_type,
                    is_current
                FROM {self._registry_table}
                WHERE dataset_id = '{dataset_id}'
                  AND is_current = true
                LIMIT 1
            """).first()

            return row.asDict() if row else None

        except Exception:
            # Table may not exist yet on first run — treat as no prior version
            return None

    # ------------------------------------------------------------------
    # Persistence — register new version
    # ------------------------------------------------------------------

    def _register_version(
        self,
        dataset_id:      str,
        version_number:  int,
        schema:          "StructType",
        change_type:     str,
        changed_columns: list[str],
        registered_by:   str,
    ) -> None:
        """
        Atomically:
        1. Mark all existing versions for this dataset as is_current = false.
        2. Insert new version row with is_current = true.

        Uses two SQL statements inside the same Spark context.
        Delta Lake's serializable isolation ensures consistency.
        """
        schema_version_id = str(uuid.uuid4())
        schema_json       = schema.json()
        now               = datetime.now(timezone.utc).isoformat()

        # Escape single quotes in changed_columns JSON
        changed_cols_json = json.dumps(changed_columns).replace("'", "''")

        logger.info(
            "Registering schema version | dataset_id=%s | version=%d | change=%s",
            dataset_id, version_number, change_type,
        )

        # Step 1 — retire all previous versions for this dataset
        self.spark.sql(f"""
            UPDATE {self._registry_table}
            SET is_current = false
            WHERE dataset_id = '{dataset_id}'
              AND is_current = true
        """)

        # Step 2 — insert the new current version
        self.spark.sql(f"""
            INSERT INTO {self._registry_table} (
                schema_version_id,
                dataset_id,
                version_number,
                schema_json,
                change_type,
                changed_columns,
                is_current,
                registered_at,
                registered_by
            ) VALUES (
                '{schema_version_id}',
                '{dataset_id}',
                {version_number},
                '{schema_json.replace(chr(39), chr(39)*2)}',
                '{change_type}',
                array({', '.join(f"'{c}'" for c in changed_columns)}),
                true,
                '{now}',
                '{registered_by}'
            )
        """)

        logger.info(
            "Schema version registered | dataset_id=%s | version=%d",
            dataset_id, version_number,
        )

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _deserialise_schema(schema_json: str) -> "StructType":
        """
        Deserialise a JSON string back to a Spark StructType.
        Uses Spark's native StructType.fromJson() for full fidelity.
        """
        from pyspark.sql.types import StructType
        return StructType.fromJson(json.loads(schema_json))
