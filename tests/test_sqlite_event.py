"""
Tests for SQLiteEvent class. (Generated using Claude.ai)
"""

import pytest
import json
from unittest.mock import Mock, MagicMock
from datetime import datetime
from main import SQLiteEvent, EventValidationError


class TestSQLiteEvent:
    """Test cases for SQLiteEvent class."""

    def create_mock_event(self, **kwargs):
        """Helper to create a mock EventStore event."""
        mock_event = Mock()
        mock_event.id = kwargs.get('id', 'test-event-id')
        mock_event.type = kwargs.get('type', 'TestEvent')
        mock_event.stream_name = kwargs.get('stream_name', 'test-stream')
        mock_event.recorded_at = kwargs.get('recorded_at', datetime(2023, 1, 1, 12, 0, 0))
        mock_event.data = kwargs.get('data', '{"test": "data"}')
        mock_event.stream_position = kwargs.get('stream_position', 1)
        mock_event.commit_position = kwargs.get('commit_position', 100)
        mock_event.prepare_position = kwargs.get('prepare_position', 99)
        mock_event.retry_count = kwargs.get('retry_count', 0)
        mock_event.link = kwargs.get('link', None)
        mock_event.content_type = kwargs.get('content_type', 'application/json')
        mock_event.created = kwargs.get('created', datetime(2023, 1, 1, 12, 0, 0))
        return mock_event

    def test_valid_event_creation(self):
        """Test creating a valid SQLite event."""
        mock_event = self.create_mock_event()
        sqlite_event = SQLiteEvent(mock_event)
        
        assert sqlite_event.id == 'test-event-id'
        assert sqlite_event.type == 'TestEvent'
        assert sqlite_event.stream_name == 'test-stream'
        assert sqlite_event.recorded_at == int(datetime(2023, 1, 1, 12, 0, 0).timestamp())
        assert sqlite_event.data == '{"test": "data"}'
        assert 'stream_position' in sqlite_event.eventstore_metadata

    def test_event_with_none_data(self):
        """Test event with None data."""
        mock_event = self.create_mock_event(data=None)
        sqlite_event = SQLiteEvent(mock_event)
        
        assert sqlite_event.data is None

    def test_event_with_bytes_data(self):
        """Test event with bytes data."""
        mock_event = self.create_mock_event(data=b'{"test": "bytes"}')
        sqlite_event = SQLiteEvent(mock_event)
        
        assert sqlite_event.data == '{"test": "bytes"}'

    def test_event_with_dict_data(self):
        """Test event with dictionary data."""
        data_dict = {"key": "value", "number": 42}
        mock_event = self.create_mock_event(data=data_dict)
        sqlite_event = SQLiteEvent(mock_event)
        
        assert json.loads(sqlite_event.data) == data_dict

    def test_event_with_list_data(self):
        """Test event with list data."""
        data_list = [1, 2, 3, "test"]
        mock_event = self.create_mock_event(data=data_list)
        sqlite_event = SQLiteEvent(mock_event)
        
        assert json.loads(sqlite_event.data) == data_list

    def test_event_with_invalid_utf8_bytes_raises_error(self):
        """Test that invalid UTF-8 bytes raise EventValidationError."""
        invalid_bytes = b'\xff\xfe\xfd'
        mock_event = self.create_mock_event(data=invalid_bytes)
        
        with pytest.raises(EventValidationError, match="Invalid UTF-8 data"):
            SQLiteEvent(mock_event, validate=True)

    def test_event_with_invalid_utf8_bytes_skipped_when_no_validation(self):
        """Test that invalid UTF-8 bytes are skipped when validation is disabled."""
        invalid_bytes = b'\xff\xfe\xfd'
        mock_event = self.create_mock_event(data=invalid_bytes)
        
        sqlite_event = SQLiteEvent(mock_event, validate=False)
        assert sqlite_event.data is None

    def test_event_with_non_serializable_data_raises_error(self):
        """Test that non-serializable data raises EventValidationError."""
        # Create an object that can't be serialized to JSON
        class NonSerializable:
            pass
        
        mock_event = self.create_mock_event(data=NonSerializable())
        
        with pytest.raises(EventValidationError, match="Cannot serialize data"):
            SQLiteEvent(mock_event, validate=True)

    def test_event_with_non_serializable_data_skipped_when_no_validation(self):
        """Test that non-serializable data is skipped when validation is disabled."""
        class NonSerializable:
            pass
        
        mock_event = self.create_mock_event(data=NonSerializable())
        
        sqlite_event = SQLiteEvent(mock_event, validate=False)
        assert sqlite_event.data is None

    def test_event_data_size_limit(self):
        """Test that event data size limit is enforced."""
        large_data = "x" * (SQLiteEvent.MAX_DATA_SIZE + 1)
        mock_event = self.create_mock_event(data=large_data)
        
        with pytest.raises(EventValidationError, match="Event data too large"):
            SQLiteEvent(mock_event, validate=True)

    def test_event_data_size_limit_bypassed_when_no_validation(self):
        """Test that data size limit is bypassed when validation is disabled."""
        large_data = "x" * (SQLiteEvent.MAX_DATA_SIZE + 1)
        mock_event = self.create_mock_event(data=large_data)
        
        sqlite_event = SQLiteEvent(mock_event, validate=False)
        assert sqlite_event.data == large_data

    def test_event_with_missing_attributes(self):
        """Test event with missing optional attributes."""
        mock_event = Mock()
        mock_event.id = 'test-id'
        mock_event.type = None
        mock_event.stream_name = None
        mock_event.recorded_at = datetime(2023, 1, 1, 12, 0, 0)
        mock_event.data = 'test data'
        # Mock the metadata attributes to avoid Mock object issues
        mock_event.stream_position = 1
        mock_event.commit_position = 100
        mock_event.prepare_position = 99
        mock_event.retry_count = 0
        mock_event.link = None
        mock_event.content_type = 'application/json'
        mock_event.created = datetime(2023, 1, 1, 12, 0, 0)
        
        sqlite_event = SQLiteEvent(mock_event)
        
        assert sqlite_event.type == 'unknown'
        assert sqlite_event.stream_name == 'unknown'

    def test_metadata_serialization(self):
        """Test that metadata is properly serialized to JSON."""
        mock_event = self.create_mock_event()
        sqlite_event = SQLiteEvent(mock_event)
        
        metadata = json.loads(sqlite_event.eventstore_metadata)
        assert metadata['stream_position'] == 1
        assert metadata['commit_position'] == 100
        assert metadata['prepare_position'] == 99
        assert metadata['retry_count'] == 0
        assert metadata['content_type'] == 'application/json'

    def test_metadata_with_none_values(self):
        """Test that None values are excluded from metadata."""
        mock_event = self.create_mock_event(
            stream_position=None,
            commit_position=None,
            link=None
        )
        sqlite_event = SQLiteEvent(mock_event)
        
        metadata = json.loads(sqlite_event.eventstore_metadata)
        assert 'stream_position' not in metadata
        assert 'commit_position' not in metadata
        assert 'link' not in metadata

    def test_metadata_timestamp_conversion(self):
        """Test that datetime objects in metadata are converted to timestamps."""
        created_time = datetime(2023, 1, 1, 12, 0, 0)
        mock_event = self.create_mock_event(created=created_time)
        sqlite_event = SQLiteEvent(mock_event)
        
        metadata = json.loads(sqlite_event.eventstore_metadata)
        assert metadata['created'] == int(created_time.timestamp())
