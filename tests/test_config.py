"""
Tests for ConversionConfig class. (Generated using Claude.ai)
"""

import pytest
from pathlib import Path
from unittest.mock import patch
from main import ConversionConfig


class TestConversionConfig:
    """Test cases for ConversionConfig class."""

    def test_valid_config(self):
        """Test creating a valid configuration."""
        config = ConversionConfig(
            eventstore_uri="esdb://localhost:2113",
            db_path=Path("test.db")
        )
        
        assert config.eventstore_uri == "esdb://localhost:2113"
        assert config.db_path == Path("test.db")
        assert config.batch_size == 1000  # default value
        assert config.commit_frequency == 5  # default value
        assert config.validate_data is True  # default value
        assert config.create_indexes is True  # default value

    def test_custom_config(self):
        """Test creating configuration with custom values."""
        config = ConversionConfig(
            eventstore_uri="esdb://localhost:2113",
            db_path=Path("custom.db"),
            batch_size=500,
            commit_frequency=10,
            validate_data=False,
            create_indexes=False
        )
        
        assert config.batch_size == 500
        assert config.commit_frequency == 10
        assert config.validate_data is False
        assert config.create_indexes is False

    def test_empty_eventstore_uri_raises_error(self):
        """Test that empty EventStore URI raises ValueError."""
        with pytest.raises(ValueError, match="EventStore URI is required"):
            ConversionConfig(
                eventstore_uri="",
                db_path=Path("test.db")
            )

    def test_none_eventstore_uri_raises_error(self):
        """Test that None EventStore URI raises ValueError."""
        with pytest.raises(ValueError, match="EventStore URI is required"):
            ConversionConfig(
                eventstore_uri=None,  # type: ignore
                db_path=Path("test.db")
            )

    def test_negative_batch_size_raises_error(self):
        """Test that negative batch size raises ValueError."""
        with pytest.raises(ValueError, match="Batch size must be positive"):
            ConversionConfig(
                eventstore_uri="esdb://localhost:2113",
                db_path=Path("test.db"),
                batch_size=0
            )

    def test_negative_commit_frequency_raises_error(self):
        """Test that negative commit frequency raises ValueError."""
        with pytest.raises(ValueError, match="Commit frequency must be positive"):
            ConversionConfig(
                eventstore_uri="esdb://localhost:2113",
                db_path=Path("test.db"),
                commit_frequency=0
            )

    def test_parent_directory_creation(self, tmp_path):
        """Test that parent directory is created if it doesn't exist."""
        db_path = tmp_path / "nested" / "dir" / "test.db"
        
        config = ConversionConfig(
            eventstore_uri="esdb://localhost:2113",
            db_path=db_path
        )
        
        assert db_path.parent.exists()
        assert db_path.parent.is_dir()

    def test_string_representation(self):
        """Test string representation of config."""
        config = ConversionConfig(
            eventstore_uri="esdb://localhost:2113",
            db_path=Path("test.db")
        )
        
        config_str = str(config)
        assert "eventstore_uri='esdb://localhost:2113'" in config_str
        assert "db_path=PosixPath('test.db')" in config_str
        assert "batch_size=1000" in config_str
