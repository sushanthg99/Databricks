# scripts/gold — Gold Layer Operational Scripts

This directory contains operational scripts for the Gold layer of the
Dynamic Databricks Ingestion Framework.

## Contents

Add scripts here for:

- **Gold table provisioning** — DDL scripts to pre-create Gold Delta tables
  with the correct schema and partitioning before the first run.
- **Full rebuild scripts** — one-off scripts to fully recompute a Gold table
  from all available Silver data (e.g., after business logic change).
- **Dependency registration** — scripts to manually register dataset dependencies
  in `dataset_dependencies` for Gold tables that join multiple Silver sources.
- **Aggregation validation** — spot-check queries to verify Gold table totals
  match expected values against Silver source tables.

## Naming Convention

```
create_gold_tables_{domain}.sql
rebuild_gold_{dataset_name}_{YYYYMMDD}.py
register_dependency_{downstream}_{upstream}.sql
validate_gold_{dataset_name}.sql
```

## Dependency Registration Example

```sql
-- Register that gold.daily_order_revenue depends on silver.orders and silver.products
INSERT INTO {catalog}.ingestion_metadata.dataset_dependencies
VALUES
  (uuid(), '{silver_orders_dataset_id}',   '{gold_revenue_dataset_id}', 'hard', current_timestamp()),
  (uuid(), '{silver_products_dataset_id}', '{gold_revenue_dataset_id}', 'hard', current_timestamp());
```

## Aggregation Validation Example

```sql
-- Verify Gold total matches Silver source
SELECT
  'gold'   AS layer, SUM(revenue)  AS total FROM {catalog}.ecommerce_gold.daily_order_revenue
UNION ALL
SELECT
  'silver' AS layer, SUM(order_amount) AS total FROM {catalog}.ecommerce_silver.orders
WHERE order_status = 'DELIVERED';
```
