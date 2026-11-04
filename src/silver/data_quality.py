"""
data_quality.py
===============
Data Quality (DQ) evaluation engine for the Silver layer.

Responsibilities
----------------
1. Load DQ rules for a dataset from the ``dq_sla_config`` metadata table.
2. Evaluate each active rule against the Silver DataFrame:
   - ``not_null``: column must not be null
   - ``unique``: column values must be unique
   - ``range``: column values must satisfy a numeric expression
   - ``regex``: column values must match a regular expression
   - ``referential``: (TODO) column values must exist in a reference table
3. Compute pass/fail row counts and pass percentage per rule.
4. Apply the configured ``threshold_pct`` — rows below threshold pass,
   above threshold optionally abort the Silver write (``fail_on_breach``).
5. Split the DataFrame into ``passing`` and ``failing`` partitions.
6. Persist DQ results (pass %, counts, run timestamp) back to
   ``dq_sla_config`` and/or a separate ``dq_results`` history table.
7. Return a result object consumed by ``SilverTransformer``.

See also
--------
- docs/metadata_design.md — dq_sla_config table
- docs/open_questions.md  — OQ-006 (DQ results storage strategy)

TODO (Phase 2)
--------------
- Implement all five rule type evaluators.
- Implement ``_load_rules()`` SQL query against dq_sla_config.
- Implement ``_persist_results()`` — confirm storage strategy (OQ-006).
- Add ``referential`` rule type (Phase 3).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

# DQ rule type constants — must match values in dq_sla_config
RULE_NOT_NULL = "not_null"
RULE_UNIQUE = "unique"
RULE_RANGE = "range"
RULE_REGEX = "regex"
RULE_REFERENTIAL = "referential"

SUPPORTED_RULE_TYPES = {RULE_NOT_NULL, RULE_UNIQUE, RULE_RANGE, RULE_REGEX}


class DataQualityEngine:
    """
    Evaluates DQ rules against a Silver DataFrame.

    Parameters
    ----------
    spark : SparkSession
    catalog : str
    metadata_schema : str
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
        self._dq_table = f"{catalog}.{metadata_schema}.dq_sla_config"

    def evaluate(
        self, df: "DataFrame", dataset_cfg: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Run all active DQ rules for a dataset against the given DataFrame.

        Parameters
        ----------
        df : DataFrame
            Silver DataFrame (post-transformation, pre-write).
        dataset_cfg : dict[str, Any]
            Must contain ``dataset_id``.

        Returns
        -------
        dict[str, Any]
            ``{passing: DataFrame, failing: DataFrame, results: list, aborted: bool}``

        TODO (Phase 2)
        --------------
        Implement rule loading and evaluation loop.
        """
        dataset_id = dataset_cfg.get("dataset_id", "UNKNOWN")
        dataset_name = dataset_cfg.get("dataset_name", "UNKNOWN")
        logger.info("DataQualityEngine.evaluate() | dataset=%s", dataset_name)

        # TODO: Implement
        # rules = self._load_rules(dataset_id)
        # if not rules:
        #     logger.info("No active DQ rules for dataset: %s", dataset_name)
        #     return {"passing": df, "failing": None, "results": [], "aborted": False}
        #
        # all_results = []
        # failing_condition = None
        # for rule in rules:
        #     rule_result = self._evaluate_rule(df, rule)
        #     all_results.append(rule_result)
        #     if rule_result["pass_pct"] < rule["threshold_pct"]:
        #         if rule["fail_on_breach"]:
        #             logger.error("DQ breach on fail_on_breach rule: %s", rule["rule_name"])
        #             self._persist_results(all_results, dataset_id)
        #             raise DQBreachError(f"DQ threshold breached: {rule['rule_name']}")
        # ...

        raise NotImplementedError(
            "DataQualityEngine.evaluate() not yet implemented. Phase 2 task."
        )

    # ------------------------------------------------------------------
    # Rule loading
    # ------------------------------------------------------------------

    def _load_rules(self, dataset_id: str) -> list[dict[str, Any]]:
        """
        Load active DQ rules for a dataset from ``dq_sla_config``.

        Returns
        -------
        list[dict[str, Any]]
            List of active DQ rule dicts.

        TODO (Phase 2)
        --------------
        SELECT * FROM {dq_table} WHERE dataset_id = '{dataset_id}' AND active = true
        """
        # TODO: Implement
        return []

    # ------------------------------------------------------------------
    # Rule evaluators
    # ------------------------------------------------------------------

    def _evaluate_rule(
        self, df: "DataFrame", rule: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Dispatch to the correct rule evaluator based on ``rule_type``.

        Returns
        -------
        dict[str, Any]
            ``{rule_name, rule_type, total_rows, passing_rows, failing_rows, pass_pct}``
        """
        rule_type = rule.get("rule_type", "")
        evaluators = {
            RULE_NOT_NULL: self._eval_not_null,
            RULE_UNIQUE: self._eval_unique,
            RULE_RANGE: self._eval_range,
            RULE_REGEX: self._eval_regex,
        }
        evaluator = evaluators.get(rule_type)
        if not evaluator:
            logger.warning("Unsupported rule type: %s — skipping", rule_type)
            return {"rule_name": rule["rule_name"], "skipped": True}

        return evaluator(df, rule)

    def _eval_not_null(
        self, df: "DataFrame", rule: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Evaluate: column must not contain null values.

        TODO (Phase 2)
        --------------
        total = df.count()
        passing = df.filter(F.col(rule["column_name"]).isNotNull()).count()
        """
        # TODO: Implement
        raise NotImplementedError("_eval_not_null() not yet implemented.")

    def _eval_unique(
        self, df: "DataFrame", rule: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Evaluate: column must contain only unique (non-duplicate) values.

        TODO (Phase 2)
        --------------
        total = df.count()
        distinct = df.select(rule["column_name"]).distinct().count()
        passing = distinct  # duplicates = total - distinct
        """
        # TODO: Implement
        raise NotImplementedError("_eval_unique() not yet implemented.")

    def _eval_range(
        self, df: "DataFrame", rule: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Evaluate: column values must satisfy a numeric expression.

        The ``rule_expression`` is a SQL WHERE-clause fragment, e.g.:
        ``"order_amount > 0"`` or ``"age BETWEEN 0 AND 120"``.

        TODO (Phase 2)
        --------------
        total = df.count()
        passing = df.filter(rule["rule_expression"]).count()
        """
        # TODO: Implement
        raise NotImplementedError("_eval_range() not yet implemented.")

    def _eval_regex(
        self, df: "DataFrame", rule: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Evaluate: column values must match a regular expression.

        TODO (Phase 2)
        --------------
        total = df.count()
        passing = df.filter(F.col(rule["column_name"]).rlike(rule["rule_expression"])).count()
        """
        # TODO: Implement
        raise NotImplementedError("_eval_regex() not yet implemented.")

    # ------------------------------------------------------------------
    # Results persistence
    # ------------------------------------------------------------------

    def _persist_results(
        self, results: list[dict[str, Any]], dataset_id: str
    ) -> None:
        """
        Persist DQ evaluation results.

        Strategy (see OQ-006):
        1. UPDATE dq_sla_config: set last_run_pass_pct, last_run_at per rule.
        2. INSERT INTO dq_results (history table): one row per rule per run.

        TODO (Phase 2)
        --------------
        Implement both persistence paths.
        Confirm whether dq_results table exists (OQ-006 — separate history table).
        """
        # TODO: Implement
        pass


# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------


class DQBreachError(Exception):
    """
    Raised when a DQ rule with ``fail_on_breach=True`` exceeds its threshold.

    This exception propagates up to ``SilverTransformer`` which marks
    the run as FAILED in ``ingestion_runtime_state``.
    """
