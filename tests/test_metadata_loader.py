"""
test_metadata_loader.py
=======================
Unit tests for ``ingestion_framework.metadata_service.metadata_loader``.

Tests the pure-Python logic in ``MetadataLoader`` that does NOT require
a live Spark session:
- SHA-256 hash computation and stability
- Deterministic dataset_id (UUID v5) generation
- YAML file loading and parsing
- Required field validation
- Hash-based change detection (same hash → skipped)

Spark-dependent methods (MERGE INTO, SQL queries) are mocked.
When those methods are implemented in Phase 1, extend the tests here
using ``pytest-mock`` to verify the correct SQL is generated.

Run:
    pytest tests/test_metadata_loader.py -v
    pytest tests/test_metadata_loader.py -v --cov=ingestion_framework
"""

from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_spark():
    """Return a MagicMock standing in for SparkSession."""
    return MagicMock(name="SparkSession")


@pytest.fixture
def loader(mock_spark):
    """Return a MetadataLoader with a mock SparkSession."""
    from ingestion_framework.metadata_service.metadata_loader import MetadataLoader

    return MetadataLoader(
        spark=mock_spark,
        catalog="test_catalog",
        metadata_schema="ingestion_metadata",
        environment="dev",
    )


@pytest.fixture
def sample_dataset_config():
    """Return a minimal valid dataset configuration dict."""
    return {
        "dataset_name": "orders",
        "version": "1.0",
        "dataset_type": "transactional",
        "data_tier": "bronze",
        "source_system": "ecommerce",
        "business_description": "Test orders dataset",
        "criticality": "high",
        "classification": "internal",
        "pii": False,
        "active": True,
        "format": "csv",
        "load_type": "incremental",
        "ingestion_mode": "auto_loader",
        "s3_bucket": "test-bucket",
        "s3_path": "raw/ecommerce/orders/",
        "delimiter": ",",
        "encoding": "UTF-8",
        "header": True,
        "primary_keys": ["order_id"],
        "watermark": {"column": "order_timestamp", "delay": "2 hours"},
        "checkpoint_location": "/Volumes/test_catalog/metadata/checkpoints/orders/",
        "schema_location": "/Volumes/test_catalog/metadata/schemas/orders/",
        "frequency": "daily",
        "tags": {"domain": "ecommerce", "owner": "data-team"},
        "data_quality": [],
    }


@pytest.fixture
def sample_yaml_file(sample_dataset_config):
    """Write a sample YAML config file to a temp directory and return its path."""
    config = {
        "source_system": "ecommerce",
        "environment": "dev",
        "datasets": [sample_dataset_config],
    }
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yml", delete=False, encoding="utf-8"
    ) as f:
        yaml.dump(config, f)
        return Path(f.name)


# ---------------------------------------------------------------------------
# Tests: dataset_id generation
# ---------------------------------------------------------------------------


class TestDatasetIdGeneration:
    """Tests for deterministic UUID v5 dataset_id generation."""

    def test_same_input_produces_same_id(self, loader):
        """Same source_system + dataset_name always produces the same UUID."""
        id1 = loader._generate_dataset_id("ecommerce", "orders")
        id2 = loader._generate_dataset_id("ecommerce", "orders")
        assert id1 == id2

    def test_different_dataset_name_produces_different_id(self, loader):
        """Different dataset names produce different UUIDs."""
        id_orders = loader._generate_dataset_id("ecommerce", "orders")
        id_customers = loader._generate_dataset_id("ecommerce", "customers")
        assert id_orders != id_customers

    def test_different_source_system_produces_different_id(self, loader):
        """Different source systems produce different UUIDs for the same dataset name."""
        id_ecommerce = loader._generate_dataset_id("ecommerce", "orders")
        id_erp = loader._generate_dataset_id("erp", "orders")
        assert id_ecommerce != id_erp

    def test_id_is_valid_uuid_format(self, loader):
        """Generated ID is a valid UUID string (lowercase hyphenated)."""
        import uuid
        dataset_id = loader._generate_dataset_id("ecommerce", "orders")
        # Should not raise
        parsed = uuid.UUID(dataset_id)
        assert str(parsed) == dataset_id

    def test_id_is_consistent_across_environments(self, loader):
        """Dataset ID is environment-independent — same in dev and prod."""
        from ingestion_framework.metadata_service.metadata_loader import MetadataLoader

        prod_loader = MetadataLoader(
            spark=MagicMock(),
            catalog="prod_catalog",
            metadata_schema="ingestion_metadata",
            environment="prod",
        )
        dev_id = loader._generate_dataset_id("ecommerce", "orders")
        prod_id = prod_loader._generate_dataset_id("ecommerce", "orders")
        assert dev_id == prod_id


# ---------------------------------------------------------------------------
# Tests: hash computation
# ---------------------------------------------------------------------------


class TestHashComputation:
    """Tests for SHA-256 config hash computation."""

    def test_same_config_produces_same_hash(self, loader, sample_dataset_config):
        """Identical configs produce the same hash."""
        h1 = loader._compute_hash(sample_dataset_config)
        h2 = loader._compute_hash(sample_dataset_config)
        assert h1 == h2

    def test_hash_is_sha256_hex(self, loader, sample_dataset_config):
        """Hash is a 64-character hex string (SHA-256)."""
        h = loader._compute_hash(sample_dataset_config)
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_changed_value_changes_hash(self, loader, sample_dataset_config):
        """Modifying any config value changes the hash."""
        h1 = loader._compute_hash(sample_dataset_config)
        modified = dict(sample_dataset_config)
        modified["s3_bucket"] = "different-bucket"
        h2 = loader._compute_hash(modified)
        assert h1 != h2

    def test_dict_key_order_does_not_affect_hash(self, loader, sample_dataset_config):
        """Hash is stable regardless of dict insertion order."""
        import copy
        cfg1 = sample_dataset_config
        # Reverse the key order
        cfg2 = dict(reversed(list(sample_dataset_config.items())))
        assert loader._compute_hash(cfg1) == loader._compute_hash(cfg2)

    def test_excluded_keys_do_not_affect_hash(self, loader, sample_dataset_config):
        """dataset_id, config_hash, and environment are excluded from hashing."""
        h1 = loader._compute_hash(sample_dataset_config)
        cfg_with_meta = dict(sample_dataset_config)
        cfg_with_meta["dataset_id"] = "some-uuid"
        cfg_with_meta["config_hash"] = "old-hash"
        cfg_with_meta["environment"] = "prod"  # Should be excluded
        h2 = loader._compute_hash(cfg_with_meta)
        assert h1 == h2


# ---------------------------------------------------------------------------
# Tests: required field validation
# ---------------------------------------------------------------------------


class TestValidation:
    """Tests for required field validation."""

    def test_valid_config_passes(self, loader, sample_dataset_config):
        """A valid config dict does not raise."""
        loader._validate_required_fields(sample_dataset_config)  # No exception

    def test_missing_dataset_name_raises(self, loader, sample_dataset_config):
        """Missing dataset_name raises ValueError."""
        cfg = dict(sample_dataset_config)
        del cfg["dataset_name"]
        with pytest.raises(ValueError, match="dataset_name"):
            loader._validate_required_fields(cfg)

    def test_missing_format_raises(self, loader, sample_dataset_config):
        """Missing format raises ValueError."""
        cfg = dict(sample_dataset_config)
        del cfg["format"]
        with pytest.raises(ValueError, match="format"):
            loader._validate_required_fields(cfg)

    def test_missing_data_tier_raises(self, loader, sample_dataset_config):
        """Missing data_tier raises ValueError."""
        cfg = dict(sample_dataset_config)
        del cfg["data_tier"]
        with pytest.raises(ValueError, match="data_tier"):
            loader._validate_required_fields(cfg)


# ---------------------------------------------------------------------------
# Tests: YAML file loading
# ---------------------------------------------------------------------------


class TestYamlFileLoading:
    """Tests for YAML file loading."""

    def test_load_from_nonexistent_file_raises(self, loader):
        """FileNotFoundError raised for missing config file."""
        with pytest.raises(FileNotFoundError):
            loader.load_from_file("/nonexistent/path/config.yml")

    def test_empty_datasets_list_returns_zero_counts(self, loader):
        """YAML with no datasets returns zero counts without error."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yml", delete=False, encoding="utf-8"
        ) as f:
            yaml.dump({"source_system": "test", "datasets": []}, f)
            path = f.name

        result = loader.load_from_file(path)
        assert result["total"] == 0
        assert result["registered"] == 0


# ---------------------------------------------------------------------------
# Tests: load_from_list — skipping unchanged configs
# ---------------------------------------------------------------------------


class TestSkipUnchanged:
    """Tests for hash-based skip logic."""

    def test_unchanged_config_is_skipped(self, loader, sample_dataset_config):
        """When existing hash matches, _process_single returns 'skipped'."""
        # Compute what the hash will be
        cfg = dict(sample_dataset_config)
        cfg["source_system"] = "ecommerce"
        cfg["environment"] = "dev"
        dataset_id = loader._generate_dataset_id("ecommerce", "orders")
        cfg["dataset_id"] = dataset_id
        expected_hash = loader._compute_hash(cfg)

        # Patch _get_existing_hash to return the same hash
        with patch.object(loader, "_get_existing_hash", return_value=expected_hash):
            result = loader._process_single(cfg)

        assert result == "skipped"

    def test_changed_config_triggers_registration(
        self, loader, sample_dataset_config
    ):
        """When hash differs, _process_single calls _register()."""
        cfg = dict(sample_dataset_config)
        cfg["source_system"] = "ecommerce"
        cfg["environment"] = "dev"

        with patch.object(loader, "_get_existing_hash", return_value="old-hash"):
            with patch.object(loader, "_register", side_effect=NotImplementedError):
                with pytest.raises(NotImplementedError):
                    loader._process_single(cfg)
            # If NotImplementedError is raised, _register was called — test passes
