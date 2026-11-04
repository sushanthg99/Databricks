# Deployment Guide — Dynamic Databricks Ingestion Framework

## Prerequisites

Before deploying, ensure the following are in place:

| Requirement | Notes |
|---|---|
| Databricks CLI v0.18+ | `databricks --version` to verify |
| Databricks workspace | With Unity Catalog enabled |
| AWS S3 access | Via Unity Catalog External Location or IAM instance profile |
| Unity Catalog catalog | One per environment: `dev_catalog`, `uat_catalog`, `prod_catalog` |
| Service principal | Dedicated SP per environment for job execution |
| Databricks secret scope | For any credentials (API keys, etc.) |
| Python 3.10+ | For local bundle validation |

---

## Step 1: Configure Databricks CLI

```bash
# Configure a profile for each environment
databricks configure --profile dev
databricks configure --profile uat
databricks configure --profile prod

# Verify connectivity
databricks workspace list --profile dev
```

Update `databricks.yml` with the correct workspace hosts:
```yaml
targets:
  dev:
    workspace:
      host: https://your-dev-workspace.azuredatabricks.net
  uat:
    workspace:
      host: https://your-uat-workspace.azuredatabricks.net
  prod:
    workspace:
      host: https://your-prod-workspace.azuredatabricks.net
```

> **IMPORTANT:** Never hardcode tokens. Use `~/.databrickscfg` profiles or the
> `DATABRICKS_HOST` / `DATABRICKS_TOKEN` environment variables.

---

## Step 2: Provision Unity Catalog Resources

Run the following SQL in Databricks SQL or a bootstrap notebook for each environment.
Scripts are located in `scripts/unity_catalog/`.

```sql
-- 1. Create catalog (if not exists)
CREATE CATALOG IF NOT EXISTS dev_catalog;

-- 2. Create metadata schema
CREATE SCHEMA IF NOT EXISTS dev_catalog.ingestion_metadata;

-- 3. Create metadata tables (see sql/metadata_tables.sql)
-- Run the full content of sql/metadata_tables.sql

-- 4. Create Unity Catalog Volume for Auto Loader internals
CREATE VOLUME IF NOT EXISTS dev_catalog.ingestion_metadata.autoloader_volume
    COMMENT 'Stores Auto Loader schemas and checkpoints';

-- The volume paths used by the framework:
-- /Volumes/dev_catalog/ingestion_metadata/autoloader_volume/schemas/
-- /Volumes/dev_catalog/ingestion_metadata/autoloader_volume/checkpoints/

-- 5. Create External Location for S3 (if not already exists)
-- Requires MANAGE privilege on the metastore
-- CREATE EXTERNAL LOCATION IF NOT EXISTS s3_raw_landing
--     URL 's3://<PLACEHOLDER_BUCKET_NAME>/'
--     WITH (STORAGE CREDENTIAL <PLACEHOLDER_CREDENTIAL_NAME>);
```

---

## Step 3: Load Environment Configuration

```bash
# Review and fill in environment-specific values
cat configs/environments/dev.yml
```

Update the following placeholders:
- `catalog_name` — Unity Catalog catalog for this environment
- `s3_bucket` — Source S3 bucket
- `s3_base_path` — Base prefix in the S3 bucket
- `volume_base_path` — Unity Catalog volume path for schemas/checkpoints

---

## Step 4: Register Dataset Metadata

Before running ingestion, register dataset configs into Unity Catalog:

```bash
# Option A: Run the metadata loader notebook manually
databricks notebooks run --profile dev notebooks/bronze_ingestion_notebook.py \
    --params '{"run_mode": "metadata_only", "source_system": "ecommerce", "environment": "dev"}'

# Option B: Run via bundle job (after deploy)
databricks bundle run bronze_ingestion_job --target dev \
    --params '{"run_mode": "metadata_only"}'
```

---

## Step 5: Validate the Bundle

```bash
# From the project root
databricks bundle validate --target dev
```

Expected output: `Bundle validated successfully`

Common validation errors:
- Missing `<PLACEHOLDER_*>` values in `databricks.yml`
- Invalid resource YAML syntax in `resources/`
- Missing required variables

---

## Step 6: Deploy the Bundle

```bash
# Deploy to development
databricks bundle deploy --target dev

# Deploy to UAT (after dev validation)
databricks bundle deploy --target uat

# Deploy to production (requires explicit approval in your workflow)
databricks bundle deploy --target prod
```

The deploy command:
1. Syncs Python source files to the workspace file path.
2. Creates or updates Databricks job definitions.
3. Does **not** run any jobs — deployment is separate from execution.

---

## Step 7: Run the Bronze Ingestion Job

```bash
# Trigger a run for a specific source system
databricks bundle run bronze_ingestion_job --target dev \
    --params '{"source_system": "ecommerce", "environment": "dev", "run_mode": "incremental"}'

# Monitor the run
databricks runs get --run-id <RUN_ID> --profile dev
```

---

## Step 8: Verify the Ingestion

```sql
-- Verify Bronze table was created and has data
SELECT COUNT(*), MAX(_ingestion_timestamp)
FROM dev_catalog.ecommerce_bronze.orders
LIMIT 1;

-- Check runtime state
SELECT dataset_id, status, rows_ingested, error_message
FROM dev_catalog.ingestion_metadata.ingestion_runtime_state
ORDER BY created_at DESC
LIMIT 10;

-- Check schema registry
SELECT dataset_id, version_number, change_type, registered_at
FROM dev_catalog.ingestion_metadata.schema_registry
ORDER BY registered_at DESC
LIMIT 10;
```

---

## Environment Promotion Checklist

Before promoting from dev → UAT → prod:

- [ ] All unit tests pass: `pytest tests/unit/`
- [ ] `databricks bundle validate` succeeds for the target environment
- [ ] Metadata tables have been provisioned in the target catalog
- [ ] S3 External Location is configured for the target environment
- [ ] Service principal has required permissions in Unity Catalog
- [ ] At least one full dry-run completed in the lower environment
- [ ] `open_questions.md` reviewed — no blocking open questions
- [ ] Code review approved

---

## Rollback Procedure

### Roll back a bundle deployment

DAB does not have native rollback. To roll back:

```bash
# Checkout the previous version
git checkout <previous-commit-hash>

# Redeploy
databricks bundle deploy --target dev
```

### Roll back a Bronze table

```sql
-- Delta Lake time travel — restore to a previous version
RESTORE TABLE dev_catalog.ecommerce_bronze.orders
TO VERSION AS OF <VERSION_NUMBER>;

-- Or by timestamp
RESTORE TABLE dev_catalog.ecommerce_bronze.orders
TO TIMESTAMP AS OF '2026-01-01 00:00:00';
```

### Reset an Auto Loader checkpoint

> **WARNING:** Resetting a checkpoint will cause Auto Loader to re-process all files from
> the beginning. This will result in duplicate rows unless the Bronze table is also cleared.

```python
# Delete checkpoint directory (via dbutils in a notebook)
dbutils.fs.rm(
    "/Volumes/{catalog}/ingestion_metadata/autoloader_volume/checkpoints/{dataset_name}",
    recurse=True
)
```

---

## Secrets Management

Never store credentials in code or YAML files. Use Databricks secret scopes:

```bash
# Create a secret scope (one-time per environment)
databricks secrets create-scope --scope ingestion-framework --profile dev

# Store a secret
databricks secrets put-secret --scope ingestion-framework --key api-key --profile dev

# Reference in Python code (never log the value)
api_key = dbutils.secrets.get(scope="ingestion-framework", key="api-key")
```

---

## Troubleshooting

| Problem | Likely Cause | Resolution |
|---|---|---|
| `PERMISSION_DENIED` on S3 | IAM role not attached to cluster | Verify cluster IAM profile / UC External Location |
| Auto Loader not processing new files | Checkpoint corrupted or stale | Check checkpoint dir; consider reset with reload |
| Schema mismatch error | New column in source not in schema | Enable `mergeSchema` in Auto Loader config |
| Metadata MERGE fails | `ingestion_metadata` schema missing | Run Step 2 of this guide |
| `bundle validate` fails | Placeholder values in `databricks.yml` | Replace all `<PLACEHOLDER_*>` values |
| DQ check abort | Threshold breached | Check `dq_sla_config` and source data |
