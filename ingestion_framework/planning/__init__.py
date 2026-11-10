"""
ingestion_framework.planning
=======================
Orion Planning cube ingestion: manifest intake gate, hierarchy
flattening, security resolution and the Bronze/Silver/Gold Financial pipeline.

Pure-Python modules (no Spark needed, unit-testable):
    manifest   - validate a landing run against its _READY manifest
    hierarchy  - flatten hierarchy JSON into edges / closure / leaf maps
    security   - resolve group grants into per-user dimension scopes

Spark module:
    pipeline   - PlanningPipeline (intake -> bronze -> silver -> security -> gold)
"""
