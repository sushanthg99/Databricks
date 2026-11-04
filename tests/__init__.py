"""
tests
=====
Test suite for the Dynamic Databricks Ingestion Framework.

Structure
---------
unit/
    Pure Python unit tests. No Spark session or Databricks cluster required.
    All PySpark dependencies are mocked. Run with: ``pytest tests/unit/``

integration/
    Integration tests that require a live Databricks environment.
    Run separately in CI with a Databricks cluster.
    Tag: ``@pytest.mark.integration``

test_metadata_loader.py
    Starter unit tests for ``MetadataLoader`` in
    ``ingestion_framework/metadata_service/metadata_loader.py``.
"""
