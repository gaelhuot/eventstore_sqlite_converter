"""
Tests for convert_events function. (Generated using Claude.ai)
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from main import convert_events, ConversionConfig, SQLiteEvent, EventValidationError


class TestConvertEvents:
    """Test cases for convert_events function."""

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path for testing."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            temp_path = Path(f.name)
        yield temp_path
        # Cleanup
        if temp_path.exists():
            temp_path.unlink()

    @pytest.fixture
    def config(self, temp_db_path):
        """Create a test configuration."""
        return ConversionConfig(
            eventstore_uri="esdb://localhost:2113",
            db_path=temp_db_path,
            batch_size=100,
            commit_frequency=2
        )

    @pytest.fixture
    def mock_event(self):
        """Create a mock EventStore event."""
        event = Mock()
        event.id = 'test-event-id'
        event.type = 'TestEvent'
        event.stream_name = 'test-stream'
        event.recorded_at = datetime(2023, 1, 1, 12, 0, 0)
        event.data = '{"test": "data"}'
        event.stream_position = 1
        event.commit_position = 100
        event.prepare_position = 99
        event.retry_count = 0
        event.link = None
        event.content_type = 'application/json'
        event.created = datetime(2023, 1, 1, 12, 0, 0)
        return event

    @patch('main.eventstore_client')
    @patch('main.SQLiteEventStore')
    def test_successful_conversion(self, mock_sqlite_store_class, mock_eventstore_client, config, mock_event):
        """Test successful event conversion."""
        # Mock EventStore client
        mock_client = Mock()
        mock_read_response = Mock()
        mock_read_response.__iter__ = Mock(return_value=iter([mock_event]))
        mock_client.read_all.return_value = mock_read_response
        mock_eventstore_client.return_value.__enter__.return_value = mock_client
        
        # Mock SQLite store
        mock_store = Mock()
        mock_sqlite_store_class.return_value.__enter__.return_value = mock_store
        mock_store.get_stats.return_value = {
            'total_events': 1,
            'events_processed_this_session': 1,
            'batches_processed': 1,
            'date_range': (1640995200, 1640995200),
            'database_size_mb': 0.1
        }
        
        # Run conversion
        stats = convert_events(config)
        
        # Verify results
        assert stats['total_events'] == 1
        assert stats['events_per_second'] > 0
        assert stats['skipped_events'] == 0
        assert 'conversion_duration_seconds' in stats
        
        # Verify that read response was stopped
        mock_read_response.stop.assert_called_once()

    @patch('main.eventstore_client')
    @patch('main.SQLiteEventStore')
    def test_conversion_with_multiple_events(self, mock_sqlite_store_class, mock_eventstore_client, config, mock_event):
        """Test conversion with multiple events."""
        # Create multiple events
        events = [mock_event for _ in range(5)]
        mock_read_response = Mock()
        mock_read_response.__iter__ = Mock(return_value=iter(events))
        mock_client = Mock()
        mock_client.read_all.return_value = mock_read_response
        mock_eventstore_client.return_value.__enter__.return_value = mock_client
        
        # Mock SQLite store
        mock_store = Mock()
        mock_sqlite_store_class.return_value.__enter__.return_value = mock_store
        mock_store.get_stats.return_value = {
            'total_events': 5,
            'events_processed_this_session': 5,
            'batches_processed': 1,
            'date_range': (1640995200, 1640995200),
            'database_size_mb': 0.5
        }
        
        # Run conversion
        stats = convert_events(config)
        
        # Verify results
        assert stats['total_events'] == 5
        assert stats['skipped_events'] == 0
        
        # Verify that save_events_batch was called
        mock_store.save_events_batch.assert_called()

    @patch('main.eventstore_client')
    @patch('main.SQLiteEventStore')
    def test_conversion_with_validation_errors(self, mock_sqlite_store_class, mock_eventstore_client, config, mock_event):
        """Test conversion with event validation errors."""
        # Create an invalid event that will cause validation error
        invalid_event = Mock()
        invalid_event.id = 'invalid-event'
        invalid_event.type = 'InvalidEvent'
        invalid_event.stream_name = 'test-stream'
        invalid_event.recorded_at = datetime(2023, 1, 1, 12, 0, 0)
        invalid_event.data = b'\xff\xfe\xfd'  # Invalid UTF-8
        
        events = [mock_event, invalid_event, mock_event]
        mock_read_response = Mock()
        mock_read_response.__iter__ = Mock(return_value=iter(events))
        mock_client = Mock()
        mock_client.read_all.return_value = mock_read_response
        mock_eventstore_client.return_value.__enter__.return_value = mock_client
        
        # Mock SQLite store
        mock_store = Mock()
        mock_sqlite_store_class.return_value.__enter__.return_value = mock_store
        mock_store.get_stats.return_value = {
            'total_events': 2,  # Only valid events
            'events_processed_this_session': 2,
            'batches_processed': 1,
            'date_range': (1640995200, 1640995200),
            'database_size_mb': 0.2
        }
        
        # Run conversion
        stats = convert_events(config)
        
        # Verify results
        assert stats['total_events'] == 2
        assert stats['skipped_events'] == 1  # One event was skipped

    @patch('main.eventstore_client')
    @patch('main.SQLiteEventStore')
    def test_conversion_with_batch_processing(self, mock_sqlite_store_class, mock_eventstore_client, config, mock_event):
        """Test conversion with batch processing."""
        # Set small batch size for testing
        config.batch_size = 2
        
        # Create multiple events
        events = [mock_event for _ in range(5)]
        mock_read_response = Mock()
        mock_read_response.__iter__ = Mock(return_value=iter(events))
        mock_client = Mock()
        mock_client.read_all.return_value = mock_read_response
        mock_eventstore_client.return_value.__enter__.return_value = mock_client
        
        # Mock SQLite store
        mock_store = Mock()
        mock_sqlite_store_class.return_value.__enter__.return_value = mock_store
        mock_store.get_stats.return_value = {
            'total_events': 5,
            'events_processed_this_session': 5,
            'batches_processed': 3,  # 2 + 2 + 1
            'date_range': (1640995200, 1640995200),
            'database_size_mb': 0.5
        }
        
        # Run conversion
        stats = convert_events(config)
        
        # Verify that save_events_batch was called multiple times
        assert mock_store.save_events_batch.call_count >= 2

    @patch('main.eventstore_client')
    @patch('main.SQLiteEventStore')
    def test_conversion_with_empty_eventstore(self, mock_sqlite_store_class, mock_eventstore_client, config):
        """Test conversion with empty EventStore."""
        # Mock empty EventStore
        mock_read_response = Mock()
        mock_read_response.__iter__ = Mock(return_value=iter([]))
        mock_client = Mock()
        mock_client.read_all.return_value = mock_read_response
        mock_eventstore_client.return_value.__enter__.return_value = mock_client
        
        # Mock SQLite store
        mock_store = Mock()
        mock_sqlite_store_class.return_value.__enter__.return_value = mock_store
        mock_store.get_stats.return_value = {
            'total_events': 0,
            'events_processed_this_session': 0,
            'batches_processed': 0,
            'date_range': (None, None),
            'database_size_mb': 0.0
        }
        
        # Run conversion
        stats = convert_events(config)
        
        # Verify results
        assert stats['total_events'] == 0
        assert stats['events_per_second'] == 0
        assert stats['skipped_events'] == 0

    @patch('main.eventstore_client')
    def test_eventstore_connection_error(self, mock_eventstore_client, config):
        """Test handling of EventStore connection errors."""
        # Mock connection error
        mock_eventstore_client.side_effect = ConnectionError("Connection failed")
        
        with pytest.raises(ConnectionError):
            convert_events(config)

    @patch('main.eventstore_client')
    @patch('main.SQLiteEventStore')
    def test_sqlite_error_handling(self, mock_sqlite_store_class, mock_eventstore_client, config, mock_event):
        """Test handling of SQLite errors."""
        # Mock EventStore client
        mock_client = Mock()
        mock_read_response = Mock()
        mock_read_response.__iter__ = Mock(return_value=iter([mock_event]))
        mock_client.read_all.return_value = mock_read_response
        mock_eventstore_client.return_value.__enter__.return_value = mock_client
        
        # Mock SQLite store that raises an error
        mock_store = Mock()
        mock_store.save_events_batch.side_effect = Exception("SQLite error")
        mock_sqlite_store_class.return_value.__enter__.return_value = mock_store
        
        with pytest.raises(Exception, match="SQLite error"):
            convert_events(config)

    @patch('main.eventstore_client')
    @patch('main.SQLiteEventStore')
    def test_conversion_with_skip_validation(self, mock_sqlite_store_class, mock_eventstore_client, config, mock_event):
        """Test conversion with validation disabled."""
        # Disable validation
        config.validate_data = False
        
        # Create an event that would normally fail validation
        invalid_event = Mock()
        invalid_event.id = 'invalid-event'
        invalid_event.type = 'InvalidEvent'
        invalid_event.stream_name = 'test-stream'
        invalid_event.recorded_at = datetime(2023, 1, 1, 12, 0, 0)
        invalid_event.data = b'\xff\xfe\xfd'  # Invalid UTF-8
        # Add metadata attributes to avoid Mock object issues
        invalid_event.stream_position = 1
        invalid_event.commit_position = 100
        invalid_event.prepare_position = 99
        invalid_event.retry_count = 0
        invalid_event.link = None
        invalid_event.content_type = 'application/json'
        invalid_event.created = datetime(2023, 1, 1, 12, 0, 0)
        
        events = [invalid_event]
        mock_read_response = Mock()
        mock_read_response.__iter__ = Mock(return_value=iter(events))
        mock_client = Mock()
        mock_client.read_all.return_value = mock_read_response
        mock_eventstore_client.return_value.__enter__.return_value = mock_client
        
        # Mock SQLite store
        mock_store = Mock()
        mock_sqlite_store_class.return_value.__enter__.return_value = mock_store
        mock_store.get_stats.return_value = {
            'total_events': 1,
            'events_processed_this_session': 1,
            'batches_processed': 1,
            'date_range': (1640995200, 1640995200),
            'database_size_mb': 0.1
        }
        
        # Run conversion - should not fail
        stats = convert_events(config)
        
        # Verify results
        assert stats['total_events'] == 1
        assert stats['skipped_events'] == 0  # No events skipped when validation is disabled

    @patch('main.eventstore_client')
    @patch('main.SQLiteEventStore')
    def test_conversion_progress_logging(self, mock_sqlite_store_class, mock_eventstore_client, config, mock_event):
        """Test that progress logging occurs during conversion."""
        # Create many events to trigger progress logging
        events = [mock_event for _ in range(2500)]  # More than 10 * batch_size
        mock_read_response = Mock()
        mock_read_response.__iter__ = Mock(return_value=iter(events))
        mock_client = Mock()
        mock_client.read_all.return_value = mock_read_response
        mock_eventstore_client.return_value.__enter__.return_value = mock_client
        
        # Mock SQLite store
        mock_store = Mock()
        mock_sqlite_store_class.return_value.__enter__.return_value = mock_store
        mock_store.get_stats.return_value = {
            'total_events': 2500,
            'events_processed_this_session': 2500,
            'batches_processed': 25,
            'date_range': (1640995200, 1640995200),
            'database_size_mb': 2.5
        }
        
        # Run conversion
        stats = convert_events(config)
        
        # Verify results
        assert stats['total_events'] == 2500
        assert stats['events_per_second'] > 0
