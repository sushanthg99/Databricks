# Orion Planning Financial Pipeline — Acme

End-to-end ingestion of the *Financial* planning cube, with
role-based row and column security driven by the cube's own security export.

Mock data (not committed to the repo): a `RUN001/` folder of 9 files + `_READY_RUN001.json`.
Place it in the landing volume, or in `demo/plan_mock_data/RUN001/` and run with `stage_from_repo = true`.

## Flow

```
Orion Planning extract ──► UC Volume  /Volumes/<catalog>/plan_landing/landing/<RUN>/
                          (data files first, _READY_<RUN>.json LAST)
   │
   ▼ 1 INTAKE      manifest READY? every file present, SHA-256, row count, header columns match
   ▼ 2 BRONZE      raw TSV/JSON as strings, snake_case columns, + _run_id/_source_file/_ingested_at
   │               replaceWhere _run_id  -> re-running a run is a no-op, history of all runs kept
   ▼ 3 SILVER      typed + trimmed + deduped (latest last_updated_at per natural key)
   │               DQ rules -> silver_fin_fact_quarantine + ingestion_metadata.plan_dq_results
   │               hierarchy JSON -> edges + closure table; security files -> flagged/typed
   ▼ 4 SECURITY    group grants -> per-user scope table  ingestion_metadata.plan_user_dim_scope
   ▼ 5 GOLD        dim_member (attributes + aliases as maps), dim_hierarchy_closure,
                   fact_fin (numeric), fact_fin_text (comments etc.), vw_fin_version_summary
                   + ROW FILTER on both facts, COLUMN MASK on fact_fin_text.text_value
```

Run log: `ingestion_metadata.plan_run_log` (one row per stage per run).

Schemas: `plan_bronze`, `plan_silver`, `plan_gold` (+ `plan_landing` for the volume,
`ingestion_metadata` for logs/scope/policy functions).

## Running

Interactive: open `notebooks/plan_fin_pipeline_notebook`, `stage = all`.
Job: `databricks bundle deploy -t dev` then `databricks bundle run plan_fin_job -t dev`
(`resources/plan_fin_job.yml`: stage_landing → intake → bronze → silver → security → gold).
Production trigger: enable the commented `file_arrival` trigger on the landing volume.


## Security model

Planning export → Databricks mapping:

| Planning | Databricks |
|---|---|
| `security_user_group` (user → group) | `silver_security_user_group` |
| `security_dimension_access` (group → element, access) | `silver_security_dimension_access` (+ `is_effective`) |
| element security on 7 dimensions | `plan_user_dim_scope(identity, dimension, member, access)` |
| cell visibility | `ROW FILTER plan_fin_row_filter` on `fact_fin`, `fact_fin_text` |
| sensitive text | `COLUMN MASK plan_text_mask` on `fact_fin_text.text_value` |

Secured dimensions: Currency, Legal Entity, Department, Molecule,
Financial Account, Version, Time Period. A fact row is visible only if
it passes **every** secured dimension (AND).

### Resolution rules (`ingestion_framework/planning/security.py`)

1. Inactive users get nothing.
2. A grant is effective when `Granted_Date <= today` and it has not expired.
3. `READ`, `WRITE`, `LOCK` are positive (LOCK = read-only); `NONE` is an explicit denial.
4. Grants on Consolidated / Root / Alternate Consolidated elements (or
   `Applies_To_Descendants = Y`) expand to all leaves beneath them, across the
   primary **and** alternate hierarchies.
5. Per user and dimension: positive grants → `allowed = positives − denied`;
   only denials → every leaf minus denied; neither → the dimension is open (`'*'`).
   **NONE always wins.** Grants from all of a user's groups are unioned.
6. A user with no effective grants at all is denied everywhere (fail closed).

Row/column policies are bypassed for members of `plan_admins`; comment text is
readable by `plan_admins` and `plan_text_readers` (both parameters of the notebook).
Both are Databricks account groups — create them before expecting the bypass.

### Identity mapping
The scope table is keyed by `lower(coalesce(Email, Username))`, compared with
`lower(current_user())`. Users without an email in the Planning export only match if
their Databricks login equals their Planning username; pass `identity_map`
(`{"k.liu": "karen.liu@acme.com"}`) to `PlanningPipeline` to map them.

### Checking access
```python
pipe.preview_access("karen.liu@acme-mock.com").show()   # visible_rows vs total_rows
```
(uses the same predicate as the row filter, for any identity, as the table owner).

## Design notes / known limits

* Cube extracts are full snapshots: Silver and Gold reflect the **latest** run;
  Bronze keeps every run. Delta extracts (`previous_run_id`) are not modelled yet.
* `LOCK`/`WRITE` have no Databricks equivalent — row filters only control visibility.
* The pipeline rewrites Gold tables each run: it drops the policies, rebuilds,
  and re-applies them last. The principal running the job must be able to read
  everything (own the tables or be in `plan_admins`).
* No currency conversion: no FX-rate dataset is in the extract.
* `Adj-<year>` periods are kept; `period_start` is NULL and `is_adjustment_period` is true.
* DQ: *error* rules quarantine rows and fail the run if any exceeds
  `dq_max_fail_pct` (default 5%); *warn* rules (members not valid leaves, e.g.
  orphans like `UNMAPPED`) are only counted.
* **Not yet run on a live workspace.** The pure-Python modules have unit tests; the Spark pipeline
  was written without a Spark session available and needs a first run on Databricks to confirm.
