"""
ingestion_framework.ingestion_engine
=====================================
Core file ingestion components.

Modules
-------
autoloader_engine
    Discovers source files on AWS S3, selects the appropriate reader based on
    file format, and writes data to Delta Bronze tables using either Databricks
    Auto Loader (incremental) or batch overwrite (full load).

schema_registry
    Manages schema registration and versioning in the Unity Catalog
    ``schema_registry`` metadata table. Detects schema changes and records
    new versions to support schema evolution auditing.
"""
