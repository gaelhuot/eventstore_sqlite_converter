"""
Integration tests for the EventStore to SQLite converter. (Generated using Claude.ai)
"""

import pytest
import tempfile
import sqlite3
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from main import (
    ConversionConfig, SQLiteEvent, SQLiteEventStore, 
    convert_events, eventstore_client
)


class TestIntegration:
    """Integration test cases."""

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
    def sample_events(self):
        """Create sample EventStore events for testing."""
        events = []
        for i in range(10):
            event = Mock()
            event.id = f'event-{i}'
            event.type = f'TestEvent{i}'
            event.stream_name = f'stream-{i % 3}'
            event.recorded_at = datetime(2023, 1, 1, 12, i, 0)
            event.data = f'{{"index": {i}, "message": "test event {i}"}}'
            event.stream_position = i
            event.commit_position = 100 + i
            event.prepare_position = 99 + i
            event.retry_count = 0
            event.link = None
            event.content_type = 'application/json'
            event.created = datetime(2023, 1, 1, 12, i, 0)
            events.append(event)
        return events

    def test_full_conversion_workflow(self, temp_db_path, sample_events):
        """Test the complete conversion workflow from EventStore to SQLite."""
        # Create configuration
        config = ConversionConfig(
            eventstore_uri="esdb://localhost:2113",
            db_path=temp_db_path,
            batch_size=3,
            commit_frequency=2
        )

        # Mock EventStore client
        mock_client = Mock()
        mock_read_response = Mock()
        mock_read_response.__iter__ = Mock(return_value=iter(sample_events))
        mock_client.read_all.return_value = mock_read_response

        # Mock the eventstore_client context manager
        with patch('main.eventstore_client') as mock_eventstore_client:
            mock_eventstore_client.return_value.__enter__.return_value = mock_client
            
            # Run conversion
            stats = convert_events(config)
            
            # Verify results
            assert stats['total_events'] == 10
            assert stats['skipped_events'] == 0
            assert stats['events_per_second'] > 0

        # Verify database was created and contains data
        assert temp_db_path.exists()
        
        # Check database contents
        with sqlite3.connect(temp_db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM events")
            count = cursor.fetchone()[0]
            assert count == 10
            
            # Check a specific event
            cursor = conn.execute(
                "SELECT id, event_type, stream_name, data FROM events WHERE id = ?",
                ('event-5',)
            )
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == 'event-5'
            assert row[1] == 'TestEvent5'
            assert row[2] == 'stream-2'
            assert 'index": 5' in row[3]

    def test_database_schema_creation(self, temp_db_path):
        """Test that database schema is properly created."""
        config = ConversionConfig(
            eventstore_uri="esdb://localhost:2113",
            db_path=temp_db_path,
            create_indexes=True
        )

        # Create store and connect
        with SQLiteEventStore(temp_db_path, config) as store:
            # Verify tables exist
            cursor = store.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            table_names = [row[0] for row in cursor.fetchall()]
            
            assert 'events' in table_names
            assert 'conversion_metadata' in table_names
            
            # Verify events table structure
            cursor = store.connection.execute("PRAGMA table_info(events)")
            columns = {row[1] for row in cursor.fetchall()}
            
            expected_columns = {
                'id', 'recorded_at', 'event_type', 'stream_name',
                'data', 'eventstore_metadata', 'processed_at'
            }
            assert expected_columns.issubset(columns)
            
            # Verify indexes were created
            cursor = store.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
            index_names = [row[0] for row in cursor.fetchall()]
            
            expected_indexes = [
                'idx_events_recorded_at',
                'idx_events_type',
                'idx_events_stream',
                'idx_events_processed_at'
            ]
            
            for expected_index in expected_indexes:
                assert expected_index in index_names

    def test_batch_processing_and_commits(self, temp_db_path, sample_events):
        """Test that batch processing and commits work correctly."""
        config = ConversionConfig(
            eventstore_uri="esdb://localhost:2113",
            db_path=temp_db_path,
            batch_size=3,
            commit_frequency=2
        )

        # Mock EventStore client
        mock_client = Mock()
        mock_read_response = Mock()
        mock_read_response.__iter__ = Mock(return_value=iter(sample_events))
        mock_client.read_all.return_value = mock_read_response

        with patch('main.eventstore_client') as mock_eventstore_client:
            mock_eventstore_client.return_value.__enter__.return_value = mock_client
            
            # Run conversion
            stats = convert_events(config)
            
            # Verify batch processing
            assert stats['total_events'] == 10
            # With batch size 3 and 10 events, we should have 4 batches (3+3+3+1)
            assert stats['batches_processed'] == 4

        # Verify database contents
        with sqlite3.connect(temp_db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM events")
            count = cursor.fetchone()[0]
            assert count == 10

    def test_event_validation_and_filtering(self, temp_db_path):
        """Test that event validation and filtering works correctly."""
        # Create events with some invalid data
        valid_event = Mock()
        valid_event.id = 'valid-event'
        valid_event.type = 'ValidEvent'
        valid_event.stream_name = 'test-stream'
        valid_event.recorded_at = datetime(2023, 1, 1, 12, 0, 0)
        valid_event.data = '{"valid": "data"}'
        valid_event.stream_position = 1
        valid_event.commit_position = 100
        valid_event.prepare_position = 99
        valid_event.retry_count = 0
        valid_event.link = None
        valid_event.content_type = 'application/json'
        valid_event.created = datetime(2023, 1, 1, 12, 0, 0)

        invalid_event = Mock()
        invalid_event.id = 'invalid-event'
        invalid_event.type = 'InvalidEvent'
        invalid_event.stream_name = 'test-stream'
        invalid_event.recorded_at = datetime(2023, 1, 1, 12, 0, 0)
        invalid_event.data = b'\xff\xfe\xfd'  # Invalid UTF-8
        invalid_event.stream_position = 2
        invalid_event.commit_position = 101
        invalid_event.prepare_position = 100
        invalid_event.retry_count = 0
        invalid_event.link = None
        invalid_event.content_type = 'application/json'
        invalid_event.created = datetime(2023, 1, 1, 12, 0, 0)

        # Create a second valid event with different ID
        valid_event2 = Mock()
        valid_event2.id = 'valid-event-2'
        valid_event2.type = 'ValidEvent'
        valid_event2.stream_name = 'test-stream'
        valid_event2.recorded_at = datetime(2023, 1, 1, 12, 0, 0)
        valid_event2.data = '{"valid": "data2"}'
        valid_event2.stream_position = 3
        valid_event2.commit_position = 102
        valid_event2.prepare_position = 101
        valid_event2.retry_count = 0
        valid_event2.link = None
        valid_event2.content_type = 'application/json'
        valid_event2.created = datetime(2023, 1, 1, 12, 0, 0)
        
        events = [valid_event, invalid_event, valid_event2]
        
        config = ConversionConfig(
            eventstore_uri="esdb://localhost:2113",
            db_path=temp_db_path,
            batch_size=2,
            validate_data=True
        )

        # Mock EventStore client
        mock_client = Mock()
        mock_read_response = Mock()
        mock_read_response.__iter__ = Mock(return_value=iter(events))
        mock_client.read_all.return_value = mock_read_response

        with patch('main.eventstore_client') as mock_eventstore_client:
            mock_eventstore_client.return_value.__enter__.return_value = mock_client
            
            # Run conversion
            stats = convert_events(config)
            
            # Verify that invalid event was skipped
            # Note: The total_events count comes from the database, not the session
            assert stats['events_processed_this_session'] == 2
            assert stats['skipped_events'] == 1

        # Verify only valid events are in database
        with sqlite3.connect(temp_db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM events")
            count = cursor.fetchone()[0]
            # We expect 2 valid events (the same valid event appears twice in the test)
            assert count == 2
            
            # Verify invalid event is not present
            cursor = conn.execute(
                "SELECT COUNT(*) FROM events WHERE id = ?",
                ('invalid-event',)
            )
            count = cursor.fetchone()[0]
            assert count == 0

    def test_conversion_metadata_tracking(self, temp_db_path, sample_events):
        """Test that conversion metadata is properly tracked."""
        config = ConversionConfig(
            eventstore_uri="esdb://localhost:2113",
            db_path=temp_db_path,
            batch_size=5
        )

        # Mock EventStore client
        mock_client = Mock()
        mock_read_response = Mock()
        mock_read_response.__iter__ = Mock(return_value=iter(sample_events))
        mock_client.read_all.return_value = mock_read_response

        with patch('main.eventstore_client') as mock_eventstore_client:
            mock_eventstore_client.return_value.__enter__.return_value = mock_client
            
            # Run conversion
            stats = convert_events(config)

        # Verify metadata was recorded
        with sqlite3.connect(temp_db_path) as conn:
            cursor = conn.execute(
                "SELECT key, value FROM conversion_metadata"
            )
            metadata = dict(cursor.fetchall())
            
            assert 'last_conversion' in metadata
            assert 'total_events' in metadata
            assert int(metadata['total_events']) == 10

    def test_performance_optimization_settings(self, temp_db_path):
        """Test that SQLite performance optimizations are applied."""
        config = ConversionConfig(
            eventstore_uri="esdb://localhost:2113",
            db_path=temp_db_path
        )

        with SQLiteEventStore(temp_db_path, config) as store:
            # Verify PRAGMA settings
            cursor = store.connection.execute("PRAGMA journal_mode")
            journal_mode = cursor.fetchone()[0]
            assert journal_mode == "wal"
            
            cursor = store.connection.execute("PRAGMA synchronous")
            synchronous = cursor.fetchone()[0]
            assert synchronous == 1  # NORMAL
            
            cursor = store.connection.execute("PRAGMA cache_size")
            cache_size = cursor.fetchone()[0]
            assert cache_size == 10000
            
            cursor = store.connection.execute("PRAGMA temp_store")
            temp_store = cursor.fetchone()[0]
            assert temp_store == 2  # MEMORY

    def test_error_handling_and_recovery(self, temp_db_path):
        """Test error handling and recovery scenarios."""
        config = ConversionConfig(
            eventstore_uri="esdb://localhost:2113",
            db_path=temp_db_path,
            batch_size=2
        )

        # Test with invalid database path
        # Use a path that can't be created due to permissions, not filesystem structure
        import tempfile
        temp_dir = tempfile.mkdtemp()
        try:
            # Make the temp directory read-only
            import os
            os.chmod(temp_dir, 0o444)  # Read-only
            
            invalid_config = ConversionConfig(
                eventstore_uri="esdb://localhost:2113",
                db_path=Path(temp_dir) / "test.db"
            )

            # Should raise an error when trying to connect
            with pytest.raises((sqlite3.Error, OSError)):
                with SQLiteEventStore(invalid_config.db_path, invalid_config):
                    pass
        finally:
            # Restore permissions and cleanup
            os.chmod(temp_dir, 0o755)
            import shutil
            shutil.rmtree(temp_dir)

        # Valid path should work
        with SQLiteEventStore(temp_db_path, config) as store:
            assert store.connection is not None
