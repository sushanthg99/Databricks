"""
src.gold
========
Gold layer transformation orchestration.

Reads from Delta Silver tables, applies business-level aggregations and joins,
and writes analytics-ready data to Delta Gold tables.

Modules
-------
gold_transformations
    Aggregations, dimensional joins, KPI calculations, and business-level
    transformations. Respects dataset dependencies defined in
    ``dataset_dependencies`` metadata table.
"""
