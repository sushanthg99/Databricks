"""
src
===
Layer-specific orchestration for the Dynamic Databricks Ingestion Framework.

This package contains the Bronze, Silver, Gold, and API ingestion modules.
Each sub-package is responsible for one layer of the medallion architecture
and orchestrates calls to ``ingestion_framework`` engine components.

Sub-packages
------------
bronze
    Entry point for raw ingestion from AWS S3 to Delta Bronze tables.
    Reads job parameters, filters active datasets, and drives the ingestion loop.

silver
    Cleansing, deduplication, type casting, and data quality evaluation.
    Reads from Bronze Delta tables and writes to Silver Delta tables.

gold
    Business-level aggregations and joins across Silver tables.
    Writes final, analytics-ready data to Gold Delta tables.

api
    REST API source ingestion. Fetches, paginates, and writes API responses
    directly to Bronze Delta tables.

Dependency rule
---------------
``src`` may import from ``ingestion_framework``.
``ingestion_framework`` must NEVER import from ``src``.
"""
