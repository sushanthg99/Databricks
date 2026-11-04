"""
src.api
=======
REST API source ingestion for the Dynamic Databricks Ingestion Framework.

Modules
-------
api_fetcher
    Fetches data from REST API endpoints configured in ``configs/api_config.yml``.
    Handles authentication (bearer token, API key), pagination (cursor, offset,
    page number), retry logic, and conversion to Spark DataFrames for writing
    to Delta Bronze tables.
"""
