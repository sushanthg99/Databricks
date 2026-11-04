# scripts/transformations — Reusable Transformation Snippets

This directory contains reusable SQL and Python transformation snippets
that can be referenced during Silver and Gold layer development.

## Contents

Add scripts here for:

- **SQL transformation templates** — common SQL patterns (deduplication,
  type casting, date parsing, NULL filling) that can be adapted per dataset.
- **Python helper functions** — PySpark utility functions for common
  transformations that don't belong in a specific module.
- **Column mapping scripts** — source-to-target column rename and type mapping
  definitions for specific source systems.

## Example Patterns

### Deduplication by Primary Key + Watermark (PySpark)

```python
from pyspark.sql import Window
from pyspark.sql import functions as F

def deduplicate(df, primary_keys: list, watermark_col: str):
    """Keep the latest record per primary key combination."""
    w = Window.partitionBy(*primary_keys).orderBy(F.desc(watermark_col))
    return (
        df.withColumn("_rank", F.row_number().over(w))
          .filter("_rank = 1")
          .drop("_rank")
    )
```

### MERGE INTO Template (Delta SQL)

```sql
MERGE INTO {target_table} AS target
USING (SELECT * FROM {source_view}) AS source
ON {join_condition}
WHEN MATCHED AND source.updated_at > target.updated_at THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
```

### Type Cast Template (PySpark)

```python
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, TimestampType

type_cast_map = {
    "order_id":    IntegerType(),
    "order_amount": DecimalType(18, 2),
    "order_ts":    TimestampType(),
}

for col_name, target_type in type_cast_map.items():
    df = df.withColumn(col_name, F.col(col_name).cast(target_type))
```
