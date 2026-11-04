"""
src.silver
==========
Silver layer transformation and data quality orchestration.

Reads from Delta Bronze tables, applies cleansing and validation,
and writes to Delta Silver tables via MERGE INTO.

Modules
-------
silver_transformations
    Type casting, null handling, deduplication, string normalisation,
    and derived column computation. Writes cleansed data to Silver Delta.

data_quality
    Evaluates DQ rules loaded from ``dq_sla_config`` against the Silver
    DataFrame. Records pass/fail metrics and optionally quarantines
    rows that fail critical rules.
"""
