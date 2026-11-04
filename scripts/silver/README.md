# scripts/silver — Silver Layer Operational Scripts

This directory contains operational scripts for the Silver layer of the
Dynamic Databricks Ingestion Framework.

## Contents

Add scripts here for:

- **Silver table provisioning** — DDL scripts to pre-create Silver Delta tables
  with the correct schema before the first run.
- **Backfill scripts** — one-off scripts to re-process Bronze data into Silver
  for a specific date range or after a DQ rule change.
- **Quarantine table management** — scripts to inspect, remediate, or archive
  rows in Silver quarantine tables.
- **DQ rule management** — scripts to add, update, or disable DQ rules in
  `dq_sla_config` outside of the YAML-driven metadata loader.

## Naming Convention

Scripts should be named descriptively:
```
create_silver_tables_{source_system}.sql
backfill_silver_{dataset_name}_{YYYYMMDD}.py
inspect_quarantine_{dataset_name}.sql
add_dq_rule_{dataset_name}_{rule_name}.sql
```

## DQ Rule Management Example

```sql
-- Add a new DQ rule directly (bypass metadata loader for manual curation)
INSERT INTO {catalog}.ingestion_metadata.dq_sla_config
VALUES (
  uuid(),                      -- dq_rule_id
  '{dataset_id}',              -- dataset_id
  'customer_age_valid',        -- rule_name
  'range',                     -- rule_type
  'age',                       -- column_name
  'age BETWEEN 0 AND 120',     -- rule_expression
  99.0,                        -- threshold_pct
  false,                       -- fail_on_breach
  'accuracy',                  -- sla_type
  NULL,                        -- sla_deadline
  NULL,                        -- last_run_pass_pct
  NULL,                        -- last_run_at
  true,                        -- active
  current_timestamp()          -- created_at
);
```
