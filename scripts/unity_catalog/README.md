# scripts/unity_catalog — Unity Catalog Provisioning Scripts

This directory contains scripts for provisioning and managing Unity Catalog
resources for the Dynamic Databricks Ingestion Framework.

## Contents

Add scripts here for:

- **Catalog and schema creation** — bootstrap SQL for new environments.
- **External Location setup** — register S3 bucket paths as UC External Locations.
- **Storage Credential creation** — link IAM roles to Databricks for S3 access.
- **Volume provisioning** — create the Unity Catalog Volumes used by Auto Loader.
- **Grant management** — GRANT/REVOKE statements for schemas, tables, and volumes.

## Bootstrap Script Template

```sql
-- Step 1: Create catalog
CREATE CATALOG IF NOT EXISTS {catalog}
  COMMENT '{environment} environment for the ingestion framework';

-- Step 2: Create metadata schema
CREATE SCHEMA IF NOT EXISTS {catalog}.ingestion_metadata
  COMMENT 'Framework metadata tables — restricted write access';

-- Step 3: Create Bronze/Silver/Gold schemas per source system
CREATE SCHEMA IF NOT EXISTS {catalog}.ecommerce_bronze  COMMENT 'Raw ecommerce data';
CREATE SCHEMA IF NOT EXISTS {catalog}.ecommerce_silver  COMMENT 'Cleansed ecommerce data';
CREATE SCHEMA IF NOT EXISTS {catalog}.ecommerce_gold    COMMENT 'Business-ready ecommerce aggregates';

-- Step 4: Create Auto Loader volume
CREATE VOLUME IF NOT EXISTS {catalog}.ingestion_metadata.autoloader_volume
  COMMENT 'Auto Loader schema inference and checkpoint files';

-- Step 5: Create External Location (requires MANAGE METASTORE privilege)
-- CREATE EXTERNAL LOCATION IF NOT EXISTS s3_raw_{environment}
--   URL 's3://<PLACEHOLDER_BUCKET_NAME>/raw/'
--   WITH (STORAGE CREDENTIAL <PLACEHOLDER_CREDENTIAL_NAME>)
--   COMMENT 'S3 raw landing zone for {environment}';

-- Step 6: Grant framework service principal write access to metadata schema
-- GRANT SELECT, INSERT, UPDATE, DELETE
--   ON SCHEMA {catalog}.ingestion_metadata
--   TO `<PLACEHOLDER_SERVICE_PRINCIPAL>`;

-- Step 7: Grant data engineers read access to all schemas
-- GRANT SELECT ON SCHEMA {catalog}.ecommerce_bronze TO `<PLACEHOLDER_DE_GROUP>`;
-- GRANT SELECT ON SCHEMA {catalog}.ecommerce_silver TO `<PLACEHOLDER_DE_GROUP>`;
-- GRANT SELECT ON SCHEMA {catalog}.ecommerce_gold   TO `<PLACEHOLDER_DE_GROUP>`;
```

## Volume Path Reference

After running the bootstrap:

| Path | Purpose |
|---|---|
| `/Volumes/{catalog}/ingestion_metadata/autoloader_volume/schemas/` | Auto Loader schema files |
| `/Volumes/{catalog}/ingestion_metadata/autoloader_volume/checkpoints/` | Auto Loader checkpoints |

## Naming Conventions

| Resource | Convention | Example |
|---|---|---|
| Catalog | `{env}_catalog` | `dev_catalog` |
| Metadata schema | `ingestion_metadata` | Same across environments |
| Domain schemas | `{domain}_{tier}` | `ecommerce_bronze` |
| External Location | `s3_raw_{env}` | `s3_raw_dev` |
| Storage Credential | `s3_credential_{env}` | `s3_credential_dev` |
