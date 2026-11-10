# Developer Workflow Guide
# Dynamic Databricks Ingestion Framework

## Overview

This document covers the complete day-to-day workflow for developing,
deploying, and running the Dynamic Databricks Ingestion Framework.
Follow these steps every time you make a code change.

---

## Prerequisites (One-Time Setup)

These only need to be done once per machine.

| Prerequisite | Status | Command to verify |
|---|---|---|
| Git installed | ✅ Done | `git --version` |
| Databricks CLI installed | ✅ Done | `databricks --version` |
| CLI configured for dev | ✅ Done | `databricks auth describe --profile dev` |
| GitHub repo connected | ✅ Done | `git remote -v` |
| Databricks bundle deployed | ✅ Done | `databricks bundle validate --target dev --profile dev` |
| GitHub connected to Databricks | ✅ Done | `databricks git-credentials list --profile dev` |

---

## Workspace Locations

| Location | Purpose |
|---|---|
| `C:\Users\sushanth.gangadhara\OneDrive...\Databricks\dynamic-databricks-ingestion\` | Local project folder — where you edit code |
| `https://github.com/sushanthg99/Databricks` | GitHub remote repository |
| `/Workspace/Users/sushanth.gangadhara@blumetra.com/.bundle/dynamic-databricks-ingestion/dev/files/` | Databricks bundle files — latest deployed code |
| `/Repos/sushanth.gangadhara@blumetra.com/Databricks` | Databricks Repos UI — Git-connected view |

---

## Daily Development Workflow

Every time you make a code change, follow these 5 steps in order.

---

### Step 1 — Make Your Code Changes

Edit files in Kiro IDE or any editor.

Common files you will edit:

| File | What you change |
|---|---|
| `configs/domain_config.yml` | Add or modify dataset configurations |
| `configs/environments/dev.yml` | Update dev environment settings (S3 bucket, catalog, etc.) |
| `ingestion_framework/metadata_service/metadata_loader.py` | Metadata MERGE logic |
| `ingestion_framework/ingestion_engine/autoloader_engine.py` | Auto Loader / batch ingestion logic |
| `src/bronze/bronze_ingestion.py` | Bronze orchestration loop |
| `src/silver/silver_transformations.py` | Silver cleansing logic (Phase 2) |
| `src/gold/gold_transformations.py` | Gold aggregations (Phase 3) |
| `sql/metadata_tables.sql` | Metadata table DDL changes |
| `resources/databricks_job.yml` | Job task definitions |

---

### Step 2 — Push to GitHub

Open a **new PowerShell terminal** and run:

```powershell
# Navigate to the project folder
cd "C:\Users\sushanth.gangadhara\OneDrive - Acme Solutions (I) Private Limited\Desktop\Databricks\dynamic-databricks-ingestion"

# Check what files changed
git status

# Stage all changes
git add .

# Commit with a meaningful message
git commit -m "feat: describe what you changed"

# Push to GitHub main branch
git push
```

**Commit message examples:**
```
feat: add customers dataset to domain_config.yml
fix: handle null values in bronze ingestion loop
refactor: simplify schema registry version check
docs: update deployment guide with S3 setup steps
```

---

### Step 3 — Deploy Bundle to Databricks

```powershell
databricks bundle deploy --target dev --profile dev
```

**What this does:**
- Uploads your latest Python files to Databricks workspace
- Updates the job definition in Databricks Workflows
- Syncs `ingestion_framework/`, `src/`, `configs/`, `notebooks/`, `sql/`, `docs/`

**Expected output:**
```
Uploading bundle files to /Workspace/Users/...
Updated jobs.bronze_ingestion_job
Files: X uploaded, X deleted
Resources: 0 created, 1 changed, 0 deleted, 0 unchanged
```

---

### Step 4 — Sync Databricks Repo UI

This refreshes what you see under **Repos** in the Databricks browser UI.

```powershell
databricks repos update 1515843394154791 --branch main --profile dev
```

**Or do it from the Databricks UI:**
1. Open Databricks in browser
2. Left sidebar → **Repos**
3. Click **Databricks** folder
4. Click the **`main`** branch badge (top of the file tree)
5. Click **Pull**

---

### Step 5 — Run the Job (When Ready)

**Option A — from PowerShell:**
```powershell
databricks bundle run bronze_ingestion_job --target dev --profile dev
```

**Option B — from Databricks UI:**
1. Left sidebar → **Workflows**
2. Find **`[dev] Bronze Ingestion — Dynamic Framework`**
3. Click **Run now**
4. Optionally set parameters:
   - `source_system`: `ecommerce`
   - `environment`: `dev`
   - `run_mode`: `incremental` or `full` or `metadata_only`
   - `dataset_name`: leave empty for all datasets, or enter one name

---

## One-Liner — All Steps Together

Run this single command after making changes:

```powershell
cd "C:\Users\sushanth.gangadhara\OneDrive - Acme Solutions (I) Private Limited\Desktop\Databricks\dynamic-databricks-ingestion"; git add .; git commit -m "update"; git push; databricks bundle deploy --target dev --profile dev; databricks repos update 1515843394154791 --branch main --profile dev
```

---

## Run Modes Explained

| run_mode | What it does | When to use |
|---|---|---|
| `metadata_only` | Registers YAML configs into Unity Catalog metadata tables only. No ingestion. | First run, or after changing domain_config.yml |
| `incremental` | Auto Loader processes only new files since last checkpoint | Daily scheduled runs |
| `full` | Overwrites entire Bronze table with all current source files | Initial load, or full refresh needed |

---

## Monitoring — Check Run Results

After a job run, check the results in Databricks SQL:

```sql
-- See latest run results per dataset
SELECT
    dataset_id,
    status,
    rows_ingested,
    duration_seconds,
    error_message,
    created_at
FROM dev_catalog.ingestion_metadata.ingestion_runtime_state
WHERE layer = 'bronze'
ORDER BY created_at DESC
LIMIT 20;

-- Check which datasets are registered
SELECT
    dataset_name,
    source_system,
    active,
    data_tier,
    criticality
FROM dev_catalog.ingestion_metadata.dataset_registry
ORDER BY source_system, dataset_name;

-- Check ingestion config
SELECT
    dr.dataset_name,
    ic.load_type,
    ic.format,
    ic.s3_bucket,
    ic.s3_path,
    ic.target_table
FROM dev_catalog.ingestion_metadata.dataset_registry  dr
JOIN dev_catalog.ingestion_metadata.ingestion_config  ic
  ON dr.dataset_id = ic.dataset_id
WHERE ic.environment = 'dev';

-- Check schema versions
SELECT
    dataset_id,
    version_number,
    change_type,
    changed_columns,
    registered_at
FROM dev_catalog.ingestion_metadata.schema_registry
ORDER BY registered_at DESC
LIMIT 10;
```

---

## Before Running the Job — Checklist

Complete these once before the first job run:

- [ ] **Create metadata tables** — run `sql/metadata_tables.sql` in Databricks SQL Editor
  - Replace `{catalog}` with your catalog name (run `SHOW CATALOGS;` to find it)
  - Replace `{metadata_schema}` with `ingestion_metadata`

- [ ] **Create Unity Catalog Volume** — run in Databricks SQL:
  ```sql
  CREATE VOLUME IF NOT EXISTS {catalog}.ingestion_metadata.autoloader_volume
    COMMENT 'Auto Loader schemas and checkpoints';
  ```

- [ ] **Set S3 bucket name** — edit `configs/environments/dev.yml`:
  ```yaml
  s3:
    source_bucket: your-actual-s3-bucket-name
  ```

- [ ] **Run metadata_only first** — registers your YAML configs:
  ```powershell
  databricks bundle run bronze_ingestion_job --target dev --profile dev
  ```
  Set `run_mode = metadata_only` in the job parameters.

- [ ] **Place source files in S3** — put CSV/JSON/Parquet files in the S3 paths
  defined in `configs/domain_config.yml`.

- [ ] **Run incremental ingestion** — set `run_mode = incremental` and run the job.

---

## Deploying to UAT and Production

When dev is validated and ready to promote:

```powershell
# Deploy to UAT
databricks bundle deploy --target uat --profile uat

# Deploy to Production (requires explicit approval)
databricks bundle deploy --target prod --profile prod
```

Each environment uses its own:
- Databricks workspace
- Unity Catalog (`uat_catalog` / `prod_catalog`)
- S3 bucket
- Secret scope

---

## Troubleshooting Common Issues

| Problem | Likely Cause | Fix |
|---|---|---|
| `git push` fails with 403 | Wrong GitHub account cached | Run `cmdkey /delete:git:https://github.com` then push again |
| `bundle deploy` fails with 400 | Cluster config issue | Check `resources/databricks_job.yml` for invalid cluster settings |
| `bundle validate` shows errors | Missing placeholder values | Replace all `<PLACEHOLDER_*>` in `databricks.yml` |
| Repos UI shows old code | Repo not synced | Run `databricks repos update 1515843394154791 --branch main --profile dev` |
| Job fails — metadata tables missing | Tables not created | Run `sql/metadata_tables.sql` in Databricks SQL Editor |
| Job fails — no active datasets | Metadata not registered | Run job with `run_mode = metadata_only` first |
| Auto Loader not picking up files | Checkpoint stale or S3 path wrong | Check `s3_path` in `domain_config.yml` and verify files exist in S3 |
| Schema mismatch error | New column in source file | Auto Loader handles this with `addNewColumns` mode automatically |

---

## Key IDs and References

| Item | Value |
|---|---|
| Databricks workspace | `https://dbc-15fc2096-517b.cloud.databricks.com` |
| Databricks profile | `dev` |
| GitHub repo | `https://github.com/sushanthg99/Databricks` |
| Databricks Repo ID | `1515843394154791` |
| Bundle target | `dev` |
| Job name | `[dev] Bronze Ingestion — Dynamic Framework` |
| Bundle files path | `/Workspace/Users/sushanth.gangadhara@blumetra.com/.bundle/dynamic-databricks-ingestion/dev/files/` |
| Repos path | `/Repos/sushanth.gangadhara@blumetra.com/Databricks` |

---

## Implementation Phases — What's Done and What's Next

| Phase | Scope | Status |
|---|---|---|
| **Phase 1** | Metadata loader, Auto Loader Bronze ingestion, schema registry | ✅ Complete |
| **Phase 2** | Silver transformations, Data Quality engine | 🔲 Next |
| **Phase 3** | Gold aggregations, dataset dependencies | 🔲 Pending |
| **Phase 4** | REST API ingestion, SLA monitoring, alerts | 🔲 Pending |
| **Phase 5** | PII masking, data classification enforcement | 🔲 Pending |
