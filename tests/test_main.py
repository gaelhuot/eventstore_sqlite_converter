"""
Tests for main function and command line interface. (Generated using Claude.ai)
"""

import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
from main import main, ConversionConfig


class TestMain:
    """Test cases for main function."""

    @pytest.fixture
    def mock_argv(self):
        """Create mock command line arguments."""
        return ['main.py', '--db', 'test.db']

    @pytest.fixture
    def mock_env(self):
        """Create mock environment variables."""
        return {'EVENTSTORE_URI': 'esdb://localhost:2113'}

    @patch('main.create_config_from_args')
    @patch('main.convert_events')
    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_successful_main_execution(self, mock_convert_events, mock_create_config, mock_argv):
        """Test successful main function execution."""
        # Mock configuration
        mock_config = Mock(spec=ConversionConfig)
        mock_config.db_path = Path("test.db")
        mock_create_config.return_value = mock_config
        
        # Mock conversion results
        mock_convert_events.return_value = {
            'total_events': 1000,
            'conversion_duration_seconds': 10.5,
            'events_per_second': 95.2,
            'database_size_mb': 2.5,
            'skipped_events': 0
        }
        
        # Mock sys.argv
        with patch.object(sys, 'argv', mock_argv):
            # Mock sys.exit to prevent actual exit
            with patch.object(sys, 'exit') as mock_exit:
                main()
                
                # Verify that conversion was called
                mock_convert_events.assert_called_once_with(mock_config)
                
                # Verify that exit was called with success code
                mock_exit.assert_called_once_with(0)

    @patch('main.create_config_from_args')
    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_main_with_configuration_error(self, mock_create_config, mock_argv):
        """Test main function with configuration error."""
        # Mock configuration error
        mock_create_config.side_effect = ValueError("Invalid configuration")
        
        # Mock sys.argv
        with patch.object(sys, 'argv', mock_argv):
            # Mock sys.exit to prevent actual exit
            with patch.object(sys, 'exit') as mock_exit:
                main()
                
                # Verify that exit was called with error code
                mock_exit.assert_called_once_with(1)

    @patch('main.create_config_from_args')
    @patch('main.convert_events')
    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_main_with_conversion_error(self, mock_convert_events, mock_create_config, mock_argv):
        """Test main function with conversion error."""
        # Mock configuration
        mock_config = Mock(spec=ConversionConfig)
        mock_config.db_path = Path("test.db")
        mock_create_config.return_value = mock_config
        
        # Mock conversion error
        mock_convert_events.side_effect = Exception("Conversion failed")
        
        # Mock sys.argv
        with patch.object(sys, 'argv', mock_argv):
            # Mock sys.exit to prevent actual exit
            with patch.object(sys, 'exit') as mock_exit:
                main()
                
                # Verify that exit was called with error code
                mock_exit.assert_called_once_with(1)

    @patch('main.create_config_from_args')
    @patch('main.convert_events')
    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_main_with_keyboard_interrupt(self, mock_convert_events, mock_create_config, mock_argv):
        """Test main function with keyboard interrupt."""
        # Mock configuration
        mock_config = Mock(spec=ConversionConfig)
        mock_config.db_path = Path("test.db")
        mock_create_config.return_value = mock_config
        
        # Mock keyboard interrupt
        mock_convert_events.side_effect = KeyboardInterrupt()
        
        # Mock sys.argv
        with patch.object(sys, 'argv', mock_argv):
            # Mock sys.exit to prevent actual exit
            with patch.object(sys, 'exit') as mock_exit:
                main()
                
                # Verify that exit was called with interrupt code
                mock_exit.assert_called_once_with(1)

    @patch('main.create_config_from_args')
    @patch('main.convert_events')
    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_main_output_formatting(self, mock_convert_events, mock_create_config, mock_argv, capsys):
        """Test main function output formatting."""
        # Mock configuration
        mock_config = Mock(spec=ConversionConfig)
        mock_config.db_path = Path("test.db")
        mock_create_config.return_value = mock_config
        
        # Mock conversion results
        mock_convert_events.return_value = {
            'total_events': 5000,
            'conversion_duration_seconds': 25.0,
            'events_per_second': 200.0,
            'database_size_mb': 10.5,
            'skipped_events': 5
        }
        
        # Mock sys.argv
        with patch.object(sys, 'argv', mock_argv):
            # Mock sys.exit to prevent actual exit
            with patch.object(sys, 'exit'):
                main()
                
                # Capture output
                captured = capsys.readouterr()
                
                # Verify output contains expected information
                assert "CONVERSION COMPLETED SUCCESSFULLY" in captured.out
                assert "Database file: test.db" in captured.out
                assert "Total events: 5,000" in captured.out
                assert "Duration: 25.00 seconds" in captured.out
                assert "Rate: 200.0 events/second" in captured.out
                assert "Database size: 10.50 MB" in captured.out
                assert "Skipped events: 5" in captured.out

    @patch('main.create_config_from_args')
    @patch('main.convert_events')
    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_main_output_without_skipped_events(self, mock_convert_events, mock_create_config, mock_argv, capsys):
        """Test main function output when no events are skipped."""
        # Mock configuration
        mock_config = Mock(spec=ConversionConfig)
        mock_config.db_path = Path("test.db")
        mock_create_config.return_value = mock_config
        
        # Mock conversion results with no skipped events
        mock_convert_events.return_value = {
            'total_events': 1000,
            'conversion_duration_seconds': 5.0,
            'events_per_second': 200.0,
            'database_size_mb': 2.0,
            'skipped_events': 0
        }
        
        # Mock sys.argv
        with patch.object(sys, 'argv', mock_argv):
            # Mock sys.exit to prevent actual exit
            with patch.object(sys, 'exit'):
                main()
                
                # Capture output
                captured = capsys.readouterr()
                
                # Verify output doesn't contain skipped events line
                assert "Skipped events:" not in captured.out

    @patch('main.create_config_from_args')
    @patch('main.convert_events')
    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_main_with_custom_arguments(self, mock_convert_events, mock_create_config, mock_argv):
        """Test main function with custom command line arguments."""
        # Mock configuration
        mock_config = Mock(spec=ConversionConfig)
        mock_config.db_path = Path("custom.db")
        mock_create_config.return_value = mock_config
        
        # Mock conversion results
        mock_convert_events.return_value = {
            'total_events': 100,
            'conversion_duration_seconds': 1.0,
            'events_per_second': 100.0,
            'database_size_mb': 0.5,
            'skipped_events': 0
        }
        
        # Mock sys.argv with custom arguments
        custom_argv = ['main.py', '--db', 'custom.db', '--batch-size', '500']
        with patch.object(sys, 'argv', custom_argv):
            # Mock sys.exit to prevent actual exit
            with patch.object(sys, 'exit') as mock_exit:
                main()
                
                # Verify that configuration was created with custom arguments
                mock_create_config.assert_called_once()
                
                # Verify that exit was called with success code
                mock_exit.assert_called_once_with(0)

    @patch('main.create_config_from_args')
    @patch('main.convert_events')
    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_main_with_version_argument(self, mock_convert_events, mock_create_config, mock_argv):
        """Test main function with version argument."""
        # Mock sys.argv with version argument
        version_argv = ['main.py', '--version']
        with patch.object(sys, 'argv', version_argv):
            # Mock sys.exit to prevent actual exit
            with patch.object(sys, 'exit') as mock_exit:
                # This should exit early due to version argument
                main()
                
                # Verify that exit was called (version argument causes early exit)
                mock_exit.assert_called()

    @patch('main.create_config_from_args')
    @patch('main.convert_events')
    @patch.dict(os.environ, {'EVENTSTORE_URI': 'esdb://localhost:2113'})
    def test_main_error_output(self, mock_convert_events, mock_create_config, mock_argv, capsys):
        """Test main function error output."""
        # Mock configuration
        mock_config = Mock(spec=ConversionConfig)
        mock_config.db_path = Path("test.db")
        mock_create_config.return_value = mock_config
        
        # Mock conversion error
        mock_convert_events.side_effect = Exception("Test error message")
        
        # Mock sys.argv
        with patch.object(sys, 'argv', mock_argv):
            # Mock sys.exit to prevent actual exit
            with patch.object(sys, 'exit'):
                main()
                
                # Capture output
                captured = capsys.readouterr()
                
                # Verify error output
                assert "Error: Test error message" in captured.out
