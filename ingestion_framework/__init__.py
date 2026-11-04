"""
ingestion_framework
===================
Reusable core engine for the Dynamic Databricks Ingestion Framework.

This package provides platform-level components that are shared across all
medallion layers (Bronze, Silver, Gold). It contains no dataset-specific or
domain-specific business logic — those concerns belong in `src/`.

Sub-packages
------------
ingestion_engine
    Auto Loader-based and batch file reader; schema registry for versioned
    schema tracking.

metadata_service
    YAML configuration loader with SHA-256 hash-based deduplication and
    MERGE INTO-based registration into Unity Catalog metadata tables.

Usage
-----
Import directly from sub-packages:

    from ingestion_framework.ingestion_engine.autoloader_engine import AutoLoaderEngine
    from ingestion_framework.ingestion_engine.schema_registry import SchemaRegistry
    from ingestion_framework.metadata_service.metadata_loader import MetadataLoader

Design rule
-----------
This package must NEVER import from `src/`. Dependency flows one way:
    notebooks → src → ingestion_framework
"""
