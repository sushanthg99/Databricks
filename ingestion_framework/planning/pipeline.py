"""
pipeline.py
===========
Orion Planning Financial cube pipeline on Databricks / Delta / Unity Catalog.

    intake -> bronze -> silver -> security -> gold (+ row filter / column mask)

Every stage is idempotent for a given ``run_id``:
  * Bronze  : replaceWhere on ``_run_id`` (history of every run is kept)
  * Silver  : snapshot of the latest run (cube extracts are full snapshots)
  * Gold    : rebuilt from Silver; security policies re-applied last

Usage (see notebooks/plan_fin_pipeline_notebook.py)::

    p = PlanningPipeline(spark, catalog="workspace", landing_dir="/Volumes/workspace/planning/landing/RUN001",
                    run_id="RUN001")
    p.run_all()
"""
from __future__ import annotations

import logging
import re
import shutil
from datetime import date, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ingestion_framework.planning import hierarchy as hier
from ingestion_framework.planning import manifest as mf
from ingestion_framework.planning import security as sec

logger = logging.getLogger(__name__)

# fact column for every dimension that security can restrict
SECURED_DIM_COLUMNS: dict[str, str] = {
    "Currency": "currency",
    "Legal Entity": "legal_entity",
    "Department": "department",
    "Molecule": "molecule",
    "Financial Account": "financial_account",
    "Version": "version",
    "Time Period": "time_period",
}
# every dimension on the fact (natural key = these 9 columns)
FACT_KEY_COLUMNS = [
    "currency", "legal_entity", "project", "department", "version",
    "time_period", "molecule", "financial_account", "measure_financial",
]
FACT_DIM_FOR_COLUMN = {
    "currency": "Currency", "legal_entity": "Legal Entity", "project": "Project",
    "department": "Department", "version": "Version", "time_period": "Time Period",
    "molecule": "Molecule", "financial_account": "Financial Account",
}

BRONZE_TABLE_FOR_PREFIX = {
    "financial_data": "bronze_fin_fact",
    "dimension_members": "bronze_dimension_members",
    "dimension_attribute": "bronze_dimension_attribute",
    "dimension_alias": "bronze_dimension_alias",
    "security_user_group": "bronze_security_user_group",
    "security_dimension_access": "bronze_security_dimension_access",
}
HIERARCHY_PREFIXES = {
    "dimension_primary_hierarchy": "primary",
    "dimension_alternate_hierarchy": "alternate",
}
_IDENT_RE = re.compile(r"^[A-Za-z0-9_.@+\-]+$")


def snake(name: str) -> str:
    return re.sub(r"[^0-9a-z]+", "_", name.strip().lower()).strip("_")


def stage_run(src_dir: str | Path, dest_dir: str | Path) -> None:
    """Copy a run folder (e.g. repo demo data) into a UC Volume landing path."""
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    for p in Path(src_dir).iterdir():
        if p.is_file():
            shutil.copy2(p, dest / p.name)


class DataQualityBreach(RuntimeError):
    pass


class PlanningPipeline:
    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        landing_dir: str,
        run_id: str,
        bronze_schema: str = "plan_bronze",
        silver_schema: str = "plan_silver",
        gold_schema: str = "plan_gold",
        meta_schema: str = "ingestion_metadata",
        admin_group: str = "plan_admins",
        text_reader_group: str = "plan_text_readers",
        dq_max_fail_pct: float = 5.0,
        identity_map: dict[str, str] | None = None,
    ) -> None:
        self.spark, self.catalog, self.landing_dir, self.run_id = spark, catalog, landing_dir, run_id
        self.bronze, self.silver, self.gold, self.meta = bronze_schema, silver_schema, gold_schema, meta_schema
        self.admin_group, self.text_reader_group = admin_group, text_reader_group
        self.dq_max_fail_pct = float(dq_max_fail_pct)
        self.identity_map = identity_map or {}
        self._manifest: dict[str, Any] | None = None

    # ------------------------------------------------------------------ utils
    def t(self, schema: str, name: str) -> str:
        return f"{self.catalog}.{schema}.{name}"

    def _sql(self, stmt: str) -> DataFrame:
        logger.debug("SQL: %s", stmt)
        return self.spark.sql(stmt)

    def _exists(self, fqn: str) -> bool:
        return self.spark.catalog.tableExists(fqn)

    def ensure_schemas(self) -> None:
        for s in (self.bronze, self.silver, self.gold, self.meta):
            self._sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{s}")

    def manifest(self) -> dict[str, Any]:
        if self._manifest is None:
            self._manifest = mf.load_manifest(self.landing_dir)
        return self._manifest

    def _log(self, stage: str, status: str, rows: int | None = None, message: str | None = None) -> None:
        schema = T.StructType([
            T.StructField("run_id", T.StringType()), T.StructField("stage", T.StringType()),
            T.StructField("status", T.StringType()), T.StructField("row_count", T.LongType()),
            T.StructField("message", T.StringType()), T.StructField("event_ts", T.TimestampType()),
        ])
        df = self.spark.createDataFrame([(self.run_id, stage, status, rows, message, datetime.utcnow())], schema)
        df.write.mode("append").option("mergeSchema", "true").saveAsTable(self.t(self.meta, "plan_run_log"))

    def _write_run(self, df: DataFrame, fqn: str) -> int:
        """Idempotent per-run write: replace only this run's partition of rows."""
        n = df.count()  # no cache(): PERSIST is not supported on serverless compute
        w = df.write.format("delta")
        if self._exists(fqn):
            w.mode("overwrite").option("replaceWhere", f"_run_id = '{self.run_id}'").saveAsTable(fqn)
        else:
            w.mode("overwrite").saveAsTable(fqn)
        return n

    def _overwrite(self, df: DataFrame, fqn: str) -> int:
        n = df.count()
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqn)
        return n

    # ----------------------------------------------------------------- intake
    def intake(self) -> mf.ValidationResult:
        self.ensure_schemas()
        res = mf.validate_run(self.landing_dir)
        for w in res.warnings:
            logger.warning("intake: %s", w)
        if not res.ok:
            self._log("intake", "FAILED", message="; ".join(res.errors)[:4000])
            raise RuntimeError("Intake validation failed: " + "; ".join(res.errors))
        self._log("intake", "OK", rows=self.manifest().get("total_row_count"))
        return res

    # ----------------------------------------------------------------- bronze
    def bronze_load(self) -> dict[str, int]:
        self.ensure_schemas()
        counts: dict[str, int] = {}
        edges: list[dict[str, Any]] = []
        for f in self.manifest()["files"]:
            name = f["file_name"]
            prefix = name.rsplit(f"_{self.run_id}", 1)[0]
            path = f"{self.landing_dir}/{name}"
            if prefix in BRONZE_TABLE_FOR_PREFIX:
                df = (self.spark.read.format("csv")
                      .option("header", "true").option("sep", "\t").option("encoding", "UTF-8")
                      .option("inferSchema", "false")
                      .option("quote", "").option("multiLine", "false")  # values contain bare " characters
                      .load(path))
                df = df.select(
                    *[F.col(f"`{c}`").alias(snake(c)) for c in df.columns],
                    F.lit(self.run_id).alias("_run_id"),
                    F.col("_metadata.file_path").alias("_source_file"),
                    F.current_timestamp().alias("_ingested_at"),
                )
                counts[BRONZE_TABLE_FOR_PREFIX[prefix]] = self._write_run(
                    df, self.t(self.bronze, BRONZE_TABLE_FOR_PREFIX[prefix]))
            elif prefix in HIERARCHY_PREFIXES:
                edges += hier.flatten_hierarchies(hier.load_hierarchies(Path(self.landing_dir) / name),
                                                  HIERARCHY_PREFIXES[prefix])
            else:
                logger.warning("bronze: no mapping for %s - skipped", name)

        edge_schema = T.StructType([
            T.StructField("dimension", T.StringType()), T.StructField("hierarchy_name", T.StringType()),
            T.StructField("hierarchy_kind", T.StringType()), T.StructField("member", T.StringType()),
            T.StructField("member_name", T.StringType()), T.StructField("parent", T.StringType()),
            T.StructField("element_type", T.StringType()), T.StructField("depth", T.IntegerType()),
            T.StructField("level", T.IntegerType()), T.StructField("weight", T.DoubleType()),
            T.StructField("element_index", T.IntegerType()),
        ])
        for e in edges:  # JSON weights may be ints; the schema column is double
            if e["weight"] is not None:
                e["weight"] = float(e["weight"])
        edf = (self.spark.createDataFrame(
                [tuple(e[f.name] for f in edge_schema.fields) for e in edges], edge_schema)
               .withColumn("_run_id", F.lit(self.run_id)).withColumn("_ingested_at", F.current_timestamp()))
        counts["bronze_hierarchy_edges"] = self._write_run(edf, self.t(self.bronze, "bronze_hierarchy_edges"))
        self.verify_bronze(counts)
        self._log("bronze", "OK", rows=sum(counts.values()), message=str(counts))
        return counts

    def verify_bronze(self, counts: dict[str, int] | None = None) -> list[dict[str, Any]]:
        """Reconcile Bronze row counts for this run against the manifest. Raises on any mismatch."""
        rows, bad = [], []
        for f in self.manifest()["files"]:
            name = f["file_name"]
            prefix = name.rsplit(f"_{self.run_id}", 1)[0]
            if prefix in BRONZE_TABLE_FOR_PREFIX:
                table = BRONZE_TABLE_FOR_PREFIX[prefix]
                actual = self._b(table).count()
            elif prefix in HIERARCHY_PREFIXES:
                table = "bronze_hierarchy_edges"
                actual = (self._b(table).where(F.col("hierarchy_kind") == HIERARCHY_PREFIXES[prefix])
                          .select("dimension", "hierarchy_name").distinct().count())
            else:
                continue
            ok = actual == f["row_count"]
            rows.append({"file": name, "table": table, "manifest_rows": f["row_count"],
                         "bronze_rows": actual, "match": ok})
            if not ok:
                bad.append(f"{name}: manifest {f['row_count']} vs bronze {actual}")
        if bad:
            self._log("bronze_verify", "FAILED", message="; ".join(bad)[:4000])
            raise RuntimeError("Bronze reconciliation failed: " + "; ".join(bad))
        self._log("bronze_verify", "OK", rows=sum(r["bronze_rows"] for r in rows))
        return rows

    def _b(self, name: str) -> DataFrame:
        return self.spark.table(self.t(self.bronze, name)).where(F.col("_run_id") == self.run_id)

    # ----------------------------------------------------------------- silver
    def silver_transform(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        counts.update(self._silver_dimensions())
        counts.update(self._silver_hierarchy())
        counts.update(self._silver_security())
        counts.update(self._silver_fact())
        self._log("silver", "OK", rows=sum(counts.values()), message=str(counts))
        return counts

    @staticmethod
    def _ts(c: str) -> F.Column:
        return F.to_timestamp(F.trim(F.col(c)))

    @staticmethod
    def _trim_strings(df: DataFrame) -> DataFrame:
        return df.select(*[F.trim(F.col(c)).alias(c) if t == "string" and not c.startswith("_") else F.col(c)
                           for c, t in df.dtypes])

    def _latest(self, df: DataFrame, keys: list[str], order_col: str) -> DataFrame:
        w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
        return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")

    def _silver_dimensions(self) -> dict[str, int]:
        m = self._trim_strings(self._b("bronze_dimension_members"))
        members = m.select(
            "dimension", "member_code", "member_name", "element_type", "parent",
            F.col("element_index").cast("int"), F.col("level").cast("int"), F.col("depth").cast("int"),
            F.col("weight").cast("double"), F.col("child_count").cast("int"),
            (F.upper(F.col("is_active")) == "Y").alias("is_active"),
            F.to_date("created_date").alias("created_date"),
            self._ts("last_modified_at").alias("last_modified_at"), "last_modified_by", "_run_id")
        members = self._latest(members, ["dimension", "member_code"], "last_modified_at")

        a = self._trim_strings(self._b("bronze_dimension_attribute"))
        attrs = a.select("dimension", "member", "attribute", "value", "attribute_type",
                         self._ts("last_updated_at").alias("last_updated_at"),
                         "updated_by", "update_source", "_run_id")
        attrs = self._latest(attrs, ["dimension", "member", "attribute"], "last_updated_at")

        al = self._trim_strings(self._b("bronze_dimension_alias"))
        aliases = al.select("dimension", "member", "alias_type", "alias_value", "language",
                            (F.upper(F.col("is_default_alias")) == "Y").alias("is_default_alias"),
                            self._ts("last_updated_at").alias("last_updated_at"), "_run_id")
        aliases = self._latest(aliases, ["dimension", "member", "alias_type"], "last_updated_at")

        return {
            "silver_dimension_members": self._overwrite(members, self.t(self.silver, "silver_dimension_members")),
            "silver_dimension_attribute": self._overwrite(attrs, self.t(self.silver, "silver_dimension_attribute")),
            "silver_dimension_alias": self._overwrite(aliases, self.t(self.silver, "silver_dimension_alias")),
        }

    def _silver_hierarchy(self) -> dict[str, int]:
        edges = self._b("bronze_hierarchy_edges").drop("_ingested_at")
        n_edges = self._overwrite(edges, self.t(self.silver, "silver_hierarchy_edges"))
        rows = hier.build_closure(r.asDict() for r in edges.collect())
        schema = T.StructType([
            T.StructField("dimension", T.StringType()), T.StructField("hierarchy_name", T.StringType()),
            T.StructField("ancestor", T.StringType()), T.StructField("member", T.StringType()),
            T.StructField("distance", T.IntegerType()),
        ])
        cdf = (self.spark.createDataFrame([tuple(r[f.name] for f in schema.fields) for r in rows], schema)
               .withColumn("_run_id", F.lit(self.run_id)))
        n_closure = self._overwrite(cdf, self.t(self.silver, "silver_hierarchy_closure"))
        return {"silver_hierarchy_edges": n_edges, "silver_hierarchy_closure": n_closure}

    def _silver_security(self) -> dict[str, int]:
        u = self._trim_strings(self._b("bronze_security_user_group"))
        users = u.select(
            "username", "group_name", "display_name", "email", "home_department", "account_type",
            (F.upper(F.col("is_active")) == "Y").alias("is_active"),
            self._ts("last_login_at").alias("last_login_at"),
            F.to_date("added_to_group_date").alias("added_to_group_date"), "added_by", "_run_id",
        ).dropDuplicates(["username", "group_name"])

        g = self._trim_strings(self._b("bronze_security_dimension_access"))
        grants = g.select(
            "group_name", "dimension", "element", F.upper(F.col("access")).alias("access"), "element_type",
            (F.upper(F.col("applies_to_descendants")) == "Y").alias("applies_to_descendants"),
            "granted_by", F.to_date("granted_date").alias("granted_date"),
            F.to_date("expiry_date").alias("expiry_date"), "comment", "_run_id",
        ).withColumn(
            "is_effective",
            (F.col("granted_date").isNull() | (F.col("granted_date") <= F.current_date()))
            & (F.col("expiry_date").isNull() | (F.col("expiry_date") >= F.current_date())),
        )
        return {
            "silver_security_user_group": self._overwrite(users, self.t(self.silver, "silver_security_user_group")),
            "silver_security_dimension_access": self._overwrite(grants, self.t(self.silver, "silver_security_dimension_access")),
        }

    # ---- fact + data quality
    def _fact_dq_rules(self) -> list[tuple[str, str, str]]:
        """(rule_name, SQL boolean 'passes' expression, severity)  severity: error -> quarantine, warn -> count only."""
        fin_entry = next(f for f in self.manifest()["files"] if f["content_type"] == "cube_data")
        versions = ", ".join(f"'{v}'" for v in fin_entry.get("versions", []))
        rules = [
            ("dimension_keys_not_null", " AND ".join(f"{c} IS NOT NULL" for c in FACT_KEY_COLUMNS), "error"),
            ("value_type_valid", "value_type IN ('Numeric','String')", "error"),
            ("numeric_has_number", "value_type <> 'Numeric' OR cell_value_numeric IS NOT NULL", "error"),
            ("string_has_text", "value_type <> 'String' OR cell_value_string IS NOT NULL", "error"),
            ("time_period_format", "time_period RLIKE '^(Adj|[A-Z][a-z]{2})-[0-9]{4}$'", "error"),
        ]
        if versions:
            rules.append(("version_known", f"version IN ({versions})", "error"))
        # referential checks against the member dimension (leaf, active) - warn only
        mem = self.spark.table(self.t(self.silver, "silver_dimension_members")).where(
            (F.col("element_type") == "Leaf") & F.col("is_active"))
        for col, dim in FACT_DIM_FOR_COLUMN.items():
            vals = [r[0] for r in mem.where(F.col("dimension") == dim).select("member_code").collect()]
            if vals:
                lst = ", ".join("'" + v.replace("'", "''") + "'" for v in vals)
                rules.append((f"{col}_valid_leaf", f"{col} IN ({lst})", "warn"))
        return rules

    def _silver_fact(self) -> dict[str, int]:
        b = self._trim_strings(self._b("bronze_fin_fact"))
        typed = b.select(
            *FACT_KEY_COLUMNS, "value_type",
            F.col("cell_value_numeric").cast("decimal(28,4)").alias("cell_value_numeric"),
            F.col("cell_value_string"),
            (F.upper(F.col("is_rule_derived")) == "Y").alias("is_rule_derived"),
            "source_process", self._ts("last_updated_at").alias("last_updated_at"),
            "last_updated_by", "_run_id",
        )
        typed = self._latest(typed, FACT_KEY_COLUMNS, "last_updated_at")

        # data quality ------------------------------------------------------
        rules = self._fact_dq_rules()
        total = typed.count()
        failed_arrays, results = [], []
        for name, expr, sev in rules:
            ok = F.coalesce(F.expr(expr), F.lit(False))
            failed_arrays.append((name, sev, ~ok))
        flagged = typed
        for name, sev, bad in failed_arrays:
            flagged = flagged.withColumn(f"_f_{name}", bad)
        for name, sev, _ in failed_arrays:
            n_bad = flagged.where(F.col(f"_f_{name}")).count()
            pct = (100.0 * n_bad / total) if total else 0.0
            results.append((self.run_id, "silver_fin_fact", name, sev, total, n_bad, pct,
                            self.dq_max_fail_pct, sev == "error" and pct > self.dq_max_fail_pct,
                            datetime.utcnow()))
        err_cols = [F.when(F.col(f"_f_{n}"), F.lit(n)) for n, s, _ in failed_arrays if s == "error"]
        flagged = flagged.withColumn(
            "_dq_failed_rules",
            F.filter(F.array(*err_cols), lambda x: x.isNotNull()) if err_cols
            else F.array().cast("array<string>"))
        drop_cols = [f"_f_{n}" for n, _, _ in failed_arrays]
        passing = flagged.where(F.size("_dq_failed_rules") == 0).drop("_dq_failed_rules", *drop_cols)
        failing = flagged.where(F.size("_dq_failed_rules") > 0).drop(*drop_cols)

        dq_schema = T.StructType([
            T.StructField("run_id", T.StringType()), T.StructField("table_name", T.StringType()),
            T.StructField("rule", T.StringType()), T.StructField("severity", T.StringType()),
            T.StructField("total_rows", T.LongType()), T.StructField("failed_rows", T.LongType()),
            T.StructField("fail_pct", T.DoubleType()), T.StructField("threshold_pct", T.DoubleType()),
            T.StructField("breached", T.BooleanType()), T.StructField("evaluated_at", T.TimestampType()),
        ])
        dq_fqn = self.t(self.meta, "plan_dq_results")
        self.ensure_schemas()
        dq_df = self.spark.createDataFrame(results, dq_schema)
        dq_df.write.mode("append").saveAsTable(dq_fqn)

        n_quarantine = self._overwrite(failing, self.t(self.silver, "silver_fin_fact_quarantine"))
        breached = [r for r in results if r[8]]
        n_ok = self._overwrite(passing, self.t(self.silver, "silver_fin_fact"))
        if breached:
            msg = ", ".join(f"{r[2]}={r[6]:.2f}%" for r in breached)
            self._log("silver_dq", "BREACH", rows=n_quarantine, message=msg)
            raise DataQualityBreach(f"DQ error-rule failure above {self.dq_max_fail_pct}%: {msg}")
        return {"silver_fin_fact": n_ok, "silver_fin_fact_quarantine": n_quarantine}

    # --------------------------------------------------------------- security
    def build_security_scope(self, today: date | None = None) -> dict[str, int]:
        """Resolve group grants -> plan_user_dim_scope (small; done on the driver)."""
        users = [{"Username": r.username, "Group_Name": r.group_name, "Email": r.email,
                  "Is_Active": "Y" if r.is_active else "N"}
                 for r in self.spark.table(self.t(self.silver, "silver_security_user_group")).collect()]
        grants = [{"Group_Name": r.group_name, "Dimension": r.dimension, "Element": r.element,
                   "Access": r.access, "Element_Type": r.element_type,
                   "Applies_To_Descendants": "Y" if r.applies_to_descendants else "N",
                   "Granted_Date": r.granted_date, "Expiry_Date": r.expiry_date}
                  for r in self.spark.table(self.t(self.silver, "silver_security_dimension_access")).collect()]
        leaf_map = hier.build_leaf_map(r.asDict() for r in self.spark.table(
            self.t(self.silver, "silver_hierarchy_edges")).collect())
        leaves_by_dim: dict[str, set[str]] = {}
        for r in self.spark.table(self.t(self.silver, "silver_dimension_members")).where(
                (F.col("element_type") == "Leaf") & F.col("is_active")).select("dimension", "member_code").collect():
            leaves_by_dim.setdefault(r.dimension, set()).add(r.member_code)

        rows, report = sec.resolve_scopes(users, grants, leaf_map, leaves_by_dim,
                                          SECURED_DIM_COLUMNS.keys(), today or date.today(), self.identity_map)
        logger.info("security: %d scope rows | expired=%d inactive=%s no-grants=%s unknown=%s",
                    len(rows), report.expired_grants, report.inactive_users,
                    report.users_without_grants, report.unknown_elements[:5])
        schema = T.StructType([T.StructField(c, T.StringType()) for c in
                               ("identity", "username", "dimension", "member", "access")])
        df = (self.spark.createDataFrame([tuple(r[c.name] for c in schema.fields) for r in rows], schema)
              .withColumn("resolved_at", F.current_timestamp()).withColumn("_run_id", F.lit(self.run_id)))
        n = self._overwrite(df, self.t(self.meta, "plan_user_dim_scope"))
        self._log("security_scope", "OK", rows=n,
                  message=f"expired={report.expired_grants} inactive={len(report.inactive_users)} "
                          f"no_grants={len(report.users_without_grants)} unknown_elements={len(report.unknown_elements)}")
        return {"plan_user_dim_scope": n}

    # ------------------------------------------------------------------- gold
    def gold_build(self) -> dict[str, int]:
        self.drop_security_policies()  # table is rewritten; policies re-applied last
        s = lambda n: self.spark.table(self.t(self.silver, n))  # noqa: E731

        attrs = (s("silver_dimension_attribute").groupBy("dimension", "member")
                 .agg(F.map_from_entries(F.collect_list(F.struct("attribute", "value"))).alias("attributes"))
                 .withColumnRenamed("dimension", "a_dim").withColumnRenamed("member", "a_member"))
        aliases = (s("silver_dimension_alias").groupBy("dimension", "member")
                   .agg(F.map_from_entries(F.collect_list(F.struct("alias_type", "alias_value"))).alias("aliases"))
                   .withColumnRenamed("dimension", "l_dim").withColumnRenamed("member", "l_member"))
        dim_member = (s("silver_dimension_members")
                      .join(attrs, (F.col("dimension") == F.col("a_dim")) & (F.col("member_code") == F.col("a_member")), "left")
                      .join(aliases, (F.col("dimension") == F.col("l_dim")) & (F.col("member_code") == F.col("l_member")), "left")
                      .drop("a_dim", "a_member", "l_dim", "l_member"))
        counts = {"dim_member": self._overwrite(dim_member, self.t(self.gold, "dim_member"))}
        counts["dim_hierarchy_closure"] = self._overwrite(
            s("silver_hierarchy_closure"), self.t(self.gold, "dim_hierarchy_closure"))

        fact = s("silver_fin_fact")
        period_ok = F.col("time_period").rlike("^[A-Z][a-z]{2}-[0-9]{4}$") & ~F.col("time_period").startswith("Adj")
        common = [
            "currency", "legal_entity", "project", "department", "version", "time_period", "molecule",
            "financial_account", F.col("measure_financial").alias("measure"),
            F.when(period_ok, F.to_date("time_period", "MMM-yyyy")).alias("period_start"),
            F.regexp_extract("time_period", "([0-9]{4})$", 1).cast("int").alias("fiscal_year"),
            F.col("time_period").startswith("Adj").alias("is_adjustment_period"),
            "is_rule_derived", "source_process", "last_updated_at", "last_updated_by", "_run_id",
        ]
        counts["fact_fin"] = self._overwrite(
            fact.where("value_type = 'Numeric'").select(*common, F.col("cell_value_numeric").alias("value")),
            self.t(self.gold, "fact_fin"))
        counts["fact_fin_text"] = self._overwrite(
            fact.where("value_type = 'String'").select(*common, F.col("cell_value_string").alias("text_value")),
            self.t(self.gold, "fact_fin_text"))

        self._sql(f"""
            CREATE OR REPLACE VIEW {self.t(self.gold, 'vw_fin_version_summary')} AS
            SELECT currency, legal_entity, department, financial_account, fiscal_year, time_period, period_start,
                   SUM(CASE WHEN version = 'Actual'   THEN value END) AS actual,
                   SUM(CASE WHEN version = 'Budget'   THEN value END) AS budget,
                   SUM(CASE WHEN version = 'Forecast' THEN value END) AS forecast,
                   SUM(CASE WHEN version = 'Actual'   THEN value END)
                     - SUM(CASE WHEN version = 'Budget' THEN value END) AS actual_vs_budget
            FROM {self.t(self.gold, 'fact_fin')}
            WHERE measure = 'Amount'
            GROUP BY currency, legal_entity, department, financial_account, fiscal_year, time_period, period_start
        """)
        self._log("gold", "OK", rows=sum(counts.values()), message=str(counts))
        return counts

    # ---------------------------------------------- row filter + column mask
    def _fn(self, name: str) -> str:
        return self.t(self.meta, name)

    def drop_security_policies(self) -> None:
        for tbl in ("fact_fin", "fact_fin_text"):
            fqn = self.t(self.gold, tbl)
            if self._exists(fqn):
                try:
                    self._sql(f"ALTER TABLE {fqn} DROP ROW FILTER")
                except Exception as exc:  # no filter set / unsupported - not fatal
                    logger.debug("drop row filter %s: %s", fqn, exc)
        text = self.t(self.gold, "fact_fin_text")
        if self._exists(text):
            try:
                self._sql(f"ALTER TABLE {text} ALTER COLUMN text_value DROP MASK")
            except Exception as exc:
                logger.debug("drop mask %s: %s", text, exc)

    def apply_security_policies(self) -> None:
        scope = self.t(self.meta, "plan_user_dim_scope")
        admin = self.admin_group.replace("'", "")
        bypass = f"is_account_group_member('{admin}') OR is_member('{admin}')"
        params = ", ".join(f"{c} STRING" for c in SECURED_DIM_COLUMNS.values())
        conds = " AND ".join(
            f"EXISTS (SELECT 1 FROM {scope} s WHERE s.identity = lower(current_user()) "
            f"AND s.dimension = '{dim}' AND s.member IN ('{sec.OPEN_MEMBER}', {col}))"
            for dim, col in SECURED_DIM_COLUMNS.items())
        self._sql(f"""
            CREATE OR REPLACE FUNCTION {self._fn('plan_fin_row_filter')}({params})
            RETURNS BOOLEAN
            RETURN {bypass} OR ({conds})
        """)
        reader = self.text_reader_group.replace("'", "")
        self._sql(f"""
            CREATE OR REPLACE FUNCTION {self._fn('plan_text_mask')}(v STRING)
            RETURNS STRING
            RETURN CASE WHEN {bypass} OR is_account_group_member('{reader}') OR is_member('{reader}')
                        THEN v ELSE '***MASKED***' END
        """)
        cols = ", ".join(SECURED_DIM_COLUMNS.values())
        for tbl in ("fact_fin", "fact_fin_text"):
            self._sql(f"ALTER TABLE {self.t(self.gold, tbl)} SET ROW FILTER {self._fn('plan_fin_row_filter')} ON ({cols})")
        self._sql(f"ALTER TABLE {self.t(self.gold, 'fact_fin_text')} "
                  f"ALTER COLUMN text_value SET MASK {self._fn('plan_text_mask')}")
        self._log("security_policies", "OK", message="row filter + mask applied to gold.fact_fin, fact_fin_text")

    def preview_access(self, identity: str, table: str = "fact_fin") -> DataFrame:
        """What would `identity` see? Same logic as the row filter, evaluated for any identity."""
        if not _IDENT_RE.match(identity):
            raise ValueError("identity may only contain letters, digits and _ . @ + -")
        scope = self.t(self.meta, "plan_user_dim_scope")
        fact = self.t(self.gold, table)
        conds = " AND ".join(
            f"EXISTS (SELECT 1 FROM {scope} s WHERE s.identity = '{identity.lower()}' "
            f"AND s.dimension = '{dim}' AND s.member IN ('{sec.OPEN_MEMBER}', f.{col}))"
            for dim, col in SECURED_DIM_COLUMNS.items())
        return self._sql(f"""
            SELECT '{identity}' AS identity,
                   (SELECT count(*) FROM {fact}) AS total_rows,
                   count(*) AS visible_rows
            FROM {fact} f WHERE {conds}
        """)

    # -------------------------------------------------------------------- all
    def run_all(self) -> dict[str, Any]:
        self.ensure_schemas()
        out: dict[str, Any] = {}
        self.intake()
        out["bronze"] = self.bronze_load()
        out["silver"] = self.silver_transform()
        out["security_scope"] = self.build_security_scope()
        out["gold"] = self.gold_build()
        self.apply_security_policies()
        return out
