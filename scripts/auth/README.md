# scripts/auth — Authentication & Credential Management

This directory contains scripts and instructions for setting up authentication
for the Dynamic Databricks Ingestion Framework.

## Contents

Add scripts here for:

- **Databricks secret scope creation** — create scopes per environment for storing
  API keys, tokens, and other sensitive credentials.
- **IAM role / instance profile setup** — configure AWS IAM roles for S3 access
  from Databricks clusters (replaces hardcoded credentials).
- **Service principal registration** — Databricks service principal setup for
  automated job execution.
- **Unity Catalog Storage Credential** — create the UC storage credential that
  maps to the IAM role for S3 External Location access.

## Secret Scope Setup (Databricks CLI)

```bash
# Create a secret scope per environment
databricks secrets create-scope --scope api-secrets-dev --profile dev
databricks secrets create-scope --scope api-secrets-uat --profile uat
databricks secrets create-scope --scope api-secrets-prod --profile prod

# Add a secret (you will be prompted for the value — never in command line)
databricks secrets put-secret --scope api-secrets-dev --key pricing-api-token --profile dev
```

## IAM Role Requirements

The Databricks cluster IAM role needs the following S3 permissions on the landing bucket:

```json
{
  "Effect": "Allow",
  "Action": ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"],
  "Resource": [
    "arn:aws:s3:::<PLACEHOLDER_S3_BUCKET_NAME>",
    "arn:aws:s3:::<PLACEHOLDER_S3_BUCKET_NAME>/*"
  ]
}
```

## Security Rules

- **Never** store credentials in scripts, YAML files, notebooks, or code.
- **Always** use Databricks secret scopes or AWS IAM roles for credential management.
- Service principals should follow the principle of least privilege.
- Rotate credentials on a schedule defined by your organisation's security policy.
