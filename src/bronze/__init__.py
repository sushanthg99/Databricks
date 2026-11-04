"""
src.bronze
==========
Bronze layer ingestion orchestration.

Reads raw data from AWS S3 into Delta Bronze tables using the
``AutoLoaderEngine`` from ``ingestion_framework``. Supports incremental
(Auto Loader) and full-load (batch overwrite) modes.

Modules
-------
bronze_ingestion
    Top-level entry point. Accepts job/notebook parameters, queries metadata
    tables for active datasets, drives the per-dataset ingestion loop, and
    updates ``ingestion_runtime_state``.

bronze_loader
    Delegates to ``AutoLoaderEngine`` and ``SchemaRegistry``. Handles the
    write to Delta Bronze and post-write metrics collection.

data_extraction
    Source file discovery helpers, S3 path utilities, and file listing
    for pre-ingestion validation.
"""
