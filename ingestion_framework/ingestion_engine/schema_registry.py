"""
schema_registry.py
==================
Schema registration and versioning for the Dynamic Databricks Ingestion Framework.

Responsibilities
----------------
1. Compare the current schema of a Bronze Delta table (or inferred Auto Loader
   schema) against the latest registered schema version in the Unity Catalog
   ``schema_registry`` metadata table.
2. Detect schema changes: added columns, dropped columns, renamed columns,
   and type changes.
3. Register a new schema version row in ``schema_registry`` when a change is
   detected, incrementing the version number and recording what changed.
4. Mark the new version as ``is_current = true`` and the previous version as
   ``is_current = false`` in an atomic MERGE operation.
5. Support schema retrieval — return the current registered schema for a
   dataset as a Spark ``StructType``.

Schema storage format
---------------------
Schemas are stored as JSON strings in the ``schema_json`` column of the
``schema_registry`` table using Spark's native ``StructType.json()`` format.
This ensures full compatibility with Auto Loader schema evolution.

See also: docs/metadata_design.md — schema_registry table
See also: OQ-007 in docs/open_questions.md

TODO
----
- Implement ``detect_changes()`` for column-level diff (add/drop/rename/type).
- Implement ``register_version()`` MERGE INTO logic.
- Implement ``get_current_schema()`` retrieval.
- Confirm ``schema_json`` format with the team (OQ-007).
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

# ---------------------------------------------------------------------------
# Change type constants — must match values in schema_registry DDL
# ---------------------------------------------------------------------------
CHANGE_INITIAL = "initial"
CHANGE_ADD_COLUMN = "add_column"
CHANGE_DROP_COLUMN = "drop_column"
CHANGE_RENAME_COLUMN = "rename_column"
CHANGE_TYPE_CHANGE = "type_change"


# ---------------------------------------------------------------------------
# SchemaRegistry
# ---------------------------------------------------------------------------


class SchemaRegistry:
    """
    Manages schema versions for ingested datasets.

    Each time a Bronze table's schema differs from the last registered version,
    a new row is inserted into the ``schema_registry`` metadata table.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    catalog : str
        Unity Catalog catalog name (e.g., ``dev_catalog``).
    metadata_schema : str
        Schema name for metadata tables (e.g., ``ingestion_metadata``).
    """

    def __init__(
        self,
        spark: "SparkSession",
        catalog: str,
        metadata_schema: str,
    ) -> None:
        self.spark = spark
        self.catalog = catalog
        self.metadata_schema = metadata_schema
        self._registry_table = f"{catalog}.{metadata_schema}.schema_registry"

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def check_and_register(
        self,
        dataset_id: str,
        dataset_name: str,
        current_schema: "StructType",
        registered_by: str = "ingestion_framework",
    ) -> dict[str, Any]:
        """
        Compare *current_schema* against the registered schema and register
        a new version if a change is detected.

        Parameters
        ----------
        dataset_id : str
            UUID of the dataset from ``dataset_registry``.
        dataset_name : str
            Human-readable dataset name (for logging).
        current_schema : StructType
            The Spark StructType obtained from the current Bronze table or
            Auto Loader schema inference.
        registered_by : str
            Identity of the caller (service principal or user).

        Returns
        -------
        dict[str, Any]
            ``{"schema_changed": bool, "version_number": int, "change_type": str}``
        """
        logger.info("Checking schema | dataset=%s", dataset_name)

        latest = self._get_latest_version(dataset_id)

        if latest is None:
            # First-time registration
            version_number = 1
            change_type = CHANGE_INITIAL
            changed_columns: list[str] = [f.name for f in current_schema.fields]
            logger.info(
                "First schema registration | dataset=%s | columns=%d",
                dataset_name, len(changed_columns),
            )
        else:
            # Compare and detect changes
            registered_schema = self._deserialise_schema(latest["schema_json"])
            diff = self._detect_changes(registered_schema, current_schema)

            if not diff["has_changes"]:
                logger.info(
                    "Schema unchanged | dataset=%s | version=%s",
                    dataset_name, latest["version_number"],
                )
                return {
                    "schema_changed": False,
                    "version_number": latest["version_number"],
                    "change_type": None,
                }

            version_number = latest["version_number"] + 1
            change_type = diff["primary_change_type"]
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
            "change_type": change_type,
        }

    def get_current_schema(self, dataset_id: str) -> "StructType | None":
        """
        Retrieve the current registered schema for a dataset.

        Parameters
        ----------
        dataset_id : str
            UUID of the dataset.

        Returns
        -------
        StructType or None
            The current Spark StructType, or ``None`` if no schema is registered.

        TODO
        ----
        Implement SQL query against ``schema_registry`` where ``is_current = true``.
        """
        # TODO: Implement
        # row = self.spark.sql(f"""
        #     SELECT schema_json FROM {self._registry_table}
        #     WHERE dataset_id = '{dataset_id}' AND is_current = true
        #     LIMIT 1
        # """).first()
        # if row:
        #     return self._deserialise_schema(row["schema_json"])
        raise NotImplementedError(
            "get_current_schema() not yet implemented. Phase 2 task."
        )

    # ------------------------------------------------------------------
    # Schema comparison
    # ------------------------------------------------------------------

    def _detect_changes(
        self,
        old_schema: "StructType",
        new_schema: "StructType",
    ) -> dict[str, Any]:
        """
        Compare two Spark StructTypes and identify what changed.

        Parameters
        ----------
        old_schema : StructType
            Previously registered schema.
        new_schema : StructType
            Current schema from the Bronze table or Auto Loader.

        Returns
        -------
        dict[str, Any]
            ``{has_changes, primary_change_type, changed_columns}``

        TODO
        ----
        Implement column-level diff logic:
        - Added columns: in new but not old
        - Dropped columns: in old but not new
        - Type changes: same name, different DataType
        - Renamed columns: heuristic — dropped + added with similar types (best-effort)
        """
        old_fields = {f.name: f.dataType for f in old_schema.fields}
        new_fields = {f.name: f.dataType for f in new_schema.fields}

        added = [name for name in new_fields if name not in old_fields]
        dropped = [name for name in old_fields if name not in new_fields]
        type_changed = [
            name for name in old_fields
            if name in new_fields and str(old_fields[name]) != str(new_fields[name])
        ]

        changed_columns = added + dropped + type_changed
        has_changes = bool(changed_columns)

        # Determine primary change type for the registry record
        if not has_changes:
            primary_change_type = None
        elif added and not dropped and not type_changed:
            primary_change_type = CHANGE_ADD_COLUMN
        elif dropped and not added and not type_changed:
            primary_change_type = CHANGE_DROP_COLUMN
        elif type_changed:
            primary_change_type = CHANGE_TYPE_CHANGE
        else:
            # Mixed changes — use most impactful type
            primary_change_type = CHANGE_TYPE_CHANGE

        return {
            "has_changes": has_changes,
            "primary_change_type": primary_change_type,
            "changed_columns": changed_columns,
            "added": added,
            "dropped": dropped,
            "type_changed": type_changed,
        }

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _get_latest_version(self, dataset_id: str) -> dict[str, Any] | None:
        """
        Query ``schema_registry`` for the current schema version of a dataset.

        Returns
        -------
        dict[str, Any] or None
            Latest version row, or ``None`` if no entry exists.

        TODO
        ----
        Implement SQL query:
            SELECT * FROM {registry_table}
            WHERE dataset_id = '{dataset_id}' AND is_current = true
            LIMIT 1
        """
        # TODO: Implement
        # row = self.spark.sql(f"""
        #     SELECT schema_version_id, version_number, schema_json, change_type
        #     FROM {self._registry_table}
        #     WHERE dataset_id = '{dataset_id}' AND is_current = true
        #     LIMIT 1
        # """).first()
        # return row.asDict() if row else None
        logger.debug(
            "_get_latest_version() not yet implemented. Returning None (first-run assumed)."
        )
        return None  # Placeholder — forces first-time registration path

    def _register_version(
        self,
        dataset_id: str,
        version_number: int,
        schema: "StructType",
        change_type: str,
        changed_columns: list[str],
        registered_by: str,
    ) -> None:
        """
        Insert a new schema version into ``schema_registry`` and mark all
        previous versions for this dataset as ``is_current = false``.

        Uses a two-step approach:
        1. UPDATE existing rows: ``SET is_current = false WHERE dataset_id = ?``
        2. INSERT new row with ``is_current = true``

        Or alternatively, a single MERGE INTO can handle both atomically.

        TODO
        ----
        Implement the MERGE / UPDATE + INSERT logic using Spark SQL.
        Confirm whether to use Delta MERGE or two separate SQL statements.
        """
        schema_version_id = str(uuid.uuid4())
        schema_json = schema.json()
        now = datetime.now(timezone.utc).isoformat()

        logger.info(
            "Registering schema version | dataset_id=%s | version=%d | change=%s",
            dataset_id, version_number, change_type,
        )

        # TODO: Implement
        # Step 1: Mark previous versions as not current
        # self.spark.sql(f"""
        #     UPDATE {self._registry_table}
        #     SET is_current = false
        #     WHERE dataset_id = '{dataset_id}'
        # """)
        #
        # Step 2: Insert new version
        # new_row = self.spark.createDataFrame([{
        #     "schema_version_id": schema_version_id,
        #     "dataset_id": dataset_id,
        #     "version_number": version_number,
        #     "schema_json": schema_json,
        #     "change_type": change_type,
        #     "changed_columns": changed_columns,
        #     "is_current": True,
        #     "registered_at": now,
        #     "registered_by": registered_by,
        # }])
        # new_row.write.format("delta").mode("append").saveAsTable(self._registry_table)

        raise NotImplementedError(
            "_register_version() not yet implemented. Phase 1 implementation task."
        )

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _deserialise_schema(schema_json: str) -> "StructType":
        """
        Deserialise a JSON string back to a Spark StructType.

        Parameters
        ----------
        schema_json : str
            JSON string produced by ``StructType.json()``.

        Returns
        -------
        StructType
            The reconstructed Spark StructType.
        """
        from pyspark.sql.types import StructType  # noqa: PLC0415 — deferred import

        return StructType.fromJson(json.loads(schema_json))
