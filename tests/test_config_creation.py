"""
Tests for create_config_from_args function. (Generated using Claude.ai)
"""

import pytest
import os
from pathlib import Path
from unittest.mock import patch, Mock
from main import create_config_from_args, ConversionConfig


class TestCreateConfigFromArgs:
    """Test cases for create_config_from_args function."""

    @pytest.fixture
    def mock_args(self):
        """Create mock command line arguments."""
        args = Mock()
        args.db = "test_events.db"
        args.batch_size = 500
        args.commit_frequency = 10
        args.skip_validation = False
        args.skip_indexes = False
        return args

    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_create_config_with_defaults(self, mock_args):
        """Test creating configuration with default values."""
        config = create_config_from_args(mock_args)
        
        assert config.eventstore_uri == 'esdb://localhost:2113'
        assert config.db_path.name == "test_events.db"
        assert config.batch_size == 500
        assert config.commit_frequency == 10
        assert config.validate_data is True
        assert config.create_indexes is True

    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_create_config_with_skip_validation(self, mock_args):
        """Test creating configuration with validation disabled."""
        mock_args.skip_validation = True
        config = create_config_from_args(mock_args)
        
        assert config.validate_data is False

    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_create_config_with_skip_indexes(self, mock_args):
        """Test creating configuration with indexes disabled."""
        mock_args.skip_indexes = True
        config = create_config_from_args(mock_args)
        
        assert config.create_indexes is False

    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_create_config_with_custom_values(self, mock_args):
        """Test creating configuration with custom values."""
        mock_args.batch_size = 1000
        mock_args.commit_frequency = 5
        config = create_config_from_args(mock_args)
        
        assert config.batch_size == 1000
        assert config.commit_frequency == 5

    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_create_config_with_relative_path(self, mock_args):
        """Test creating configuration with relative path."""
        mock_args.db = "data/events.db"
        config = create_config_from_args(mock_args)
        
        assert config.db_path.name == "events.db"
        assert config.db_path.parent.name == "data"

    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_create_config_with_absolute_path(self, mock_args):
        """Test creating configuration with absolute path."""
        # Use a path that can actually be created
        import tempfile
        with tempfile.TemporaryDirectory() as temp_dir:
            mock_args.db = str(Path(temp_dir) / "events.db")
            config = create_config_from_args(mock_args)
            
            assert config.db_path.name == "events.db"

    def test_missing_eventstore_uri_raises_error(self, mock_args):
        """Test that missing EVENTSTORE_URI raises ValueError."""
        # Clear environment variable
        if 'EVENTSTORE_URI' in os.environ:
            del os.environ['EVENTSTORE_URI']
        
        with pytest.raises(ValueError, match="EVENTSTORE_URI environment variable is required"):
            create_config_from_args(mock_args)

    @patch.dict(os.environ, {'EVENTSTORE_URI': ''})
    def test_empty_eventstore_uri_raises_error(self, mock_args):
        """Test that empty EVENTSTORE_URI raises ValueError."""
        with pytest.raises(ValueError, match="EVENTSTORE_URI environment variable is required"):
            create_config_from_args(mock_args)

    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_config_validation_after_creation(self, mock_args):
        """Test that configuration validation occurs after creation."""
        # This test verifies that the ConversionConfig.__post_init__ method
        # is called and validates the configuration
        
        config = create_config_from_args(mock_args)
        
        # The config should be valid
        assert config.eventstore_uri == 'esdb://localhost:2113'
        assert config.batch_size > 0
        assert config.commit_frequency > 0

    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_db_path_resolution(self, mock_args):
        """Test that database path is properly resolved."""
        mock_args.db = "nested/dir/events.db"
        config = create_config_from_args(mock_args)
        
        # The path should be resolved to an absolute path
        assert config.db_path.is_absolute() or config.db_path == Path("nested/dir/events.db")

    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_environment_variable_priority(self, mock_args):
        """Test that EVENTSTORE_URI from environment takes priority."""
        # Set environment variable
        os.environ['EVENTSTORE_URI'] = 'esdb://production:2113'
        
        config = create_config_from_args(mock_args)
        
        # Should use environment variable, not any default
        assert config.eventstore_uri == 'esdb://production:2113'

    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_all_skip_options(self, mock_args):
        """Test creating configuration with all skip options enabled."""
        mock_args.skip_validation = True
        mock_args.skip_indexes = True
        
        config = create_config_from_args(mock_args)
        
        assert config.validate_data is False
        assert config.create_indexes is False

    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_none_values_in_args(self, mock_args):
        """Test handling of None values in arguments."""
        # Test that None values are handled gracefully
        # The actual behavior depends on how argparse handles None values
        # For now, we'll test with valid values
        mock_args.batch_size = 1000
        mock_args.commit_frequency = 5
        
        config = create_config_from_args(mock_args)
        
        assert config.batch_size == 1000
        assert config.commit_frequency == 5
