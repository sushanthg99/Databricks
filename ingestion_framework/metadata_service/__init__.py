"""
ingestion_framework.metadata_service
======================================
Metadata configuration loading and registration service.

Modules
-------
metadata_loader
    Reads YAML dataset configuration files, computes SHA-256 hashes to detect
    changes, and uses ``MERGE INTO`` to register or update configuration rows
    in Unity Catalog metadata tables. Unchanged configurations are skipped to
    avoid unnecessary writes.
"""
