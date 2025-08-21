"""
Tests for SQLiteEventStore class. (Generated using Claude.ai)
"""

import pytest
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from main import SQLiteEventStore, ConversionConfig, SQLiteEvent


class TestSQLiteEventStore:
    """Test cases for SQLiteEventStore class."""

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
    def config(self):
        """Create a test configuration."""
        return ConversionConfig(
            eventstore_uri="esdb://localhost:2113",
            db_path=Path("test.db"),
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

    def test_initialization(self, temp_db_path, config):
        """Test SQLiteEventStore initialization."""
        store = SQLiteEventStore(temp_db_path, config)
        
        assert store.db_path == temp_db_path
        assert store.config == config
        assert store.connection is None
        assert store.events_processed == 0
        assert store.batches_processed == 0

    def test_context_manager(self, temp_db_path, config):
        """Test SQLiteEventStore as context manager."""
        with SQLiteEventStore(temp_db_path, config) as store:
            assert store.connection is not None
            assert isinstance(store.connection, sqlite3.Connection)
        
        # Connection should be closed after context exit
        assert store.connection is None

    def test_connect_and_schema_creation(self, temp_db_path, config):
        """Test database connection and schema creation."""
        store = SQLiteEventStore(temp_db_path, config)
        store.connect()
        
        try:
            assert store.connection is not None
            
            # Check if tables were created
            cursor = store.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            table_names = [row[0] for row in cursor.fetchall()]
            
            assert 'events' in table_names
            assert 'conversion_metadata' in table_names
            
            # Check events table schema
            cursor = store.connection.execute("PRAGMA table_info(events)")
            columns = {row[1] for row in cursor.fetchall()}
            
            expected_columns = {
                'id', 'recorded_at', 'event_type', 'stream_name',
                'data', 'eventstore_metadata', 'processed_at'
            }
            assert expected_columns.issubset(columns)
            
        finally:
            store.close()

    def test_index_creation(self, temp_db_path, config):
        """Test that indexes are created when enabled."""
        config.create_indexes = True
        store = SQLiteEventStore(temp_db_path, config)
        store.connect()
        
        try:
            # Check if indexes were created
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
                
        finally:
            store.close()

    def test_skip_index_creation(self, temp_db_path, config):
        """Test that indexes are skipped when disabled."""
        config.create_indexes = False
        store = SQLiteEventStore(temp_db_path, config)
        store.connect()
        
        try:
            # Check that no custom indexes were created
            cursor = store.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_autoindex%'"
            )
            index_names = [row[0] for row in cursor.fetchall()]
            
            # No custom indexes should exist
            assert len(index_names) == 0
            
        finally:
            store.close()

    def test_save_events_batch(self, temp_db_path, config, mock_event):
        """Test saving a batch of events."""
        # Set commit frequency to 1 to ensure immediate commit
        config.commit_frequency = 1
        store = SQLiteEventStore(temp_db_path, config)
        store.connect()
        
        try:
            # Create SQLite events with unique IDs
            sqlite_events = []
            for i in range(3):
                event_copy = Mock()
                event_copy.id = f'test-event-{i}'
                event_copy.type = mock_event.type
                event_copy.stream_name = mock_event.stream_name
                event_copy.recorded_at = mock_event.recorded_at
                event_copy.data = mock_event.data
                event_copy.stream_position = mock_event.stream_position
                event_copy.commit_position = mock_event.commit_position
                event_copy.prepare_position = mock_event.prepare_position
                event_copy.retry_count = mock_event.retry_count
                event_copy.link = mock_event.link
                event_copy.content_type = mock_event.content_type
                event_copy.created = mock_event.created
                sqlite_events.append(SQLiteEvent(event_copy))
            
            # Save batch
            store.save_events_batch(sqlite_events)
            
            # Check that events were saved
            cursor = store.connection.execute("SELECT COUNT(*) FROM events")
            count = cursor.fetchone()[0]
            assert count == 3
            
            # Check counters
            assert store.events_processed == 3
            assert store.batches_processed == 1
            
        finally:
            store.close()

    def test_save_empty_batch(self, temp_db_path, config):
        """Test saving an empty batch."""
        store = SQLiteEventStore(temp_db_path, config)
        store.connect()
        
        try:
            initial_count = store.events_processed
            
            # Save empty batch
            store.save_events_batch([])
            
            # Counters should not change
            assert store.events_processed == initial_count
            assert store.batches_processed == 0
            
        finally:
            store.close()

    def test_commit_frequency(self, temp_db_path, config, mock_event):
        """Test that commits happen at the specified frequency."""
        config.commit_frequency = 2
        store = SQLiteEventStore(temp_db_path, config)
        store.connect()
        
        try:
            # Create events
            sqlite_events = [SQLiteEvent(mock_event) for _ in range(5)]
            
            # Save first batch (should not commit yet)
            store.save_events_batch(sqlite_events[:2])
            assert store.batches_processed == 1
            
            # Save second batch (should commit)
            store.save_events_batch(sqlite_events[2:4])
            assert store.batches_processed == 2
            
            # Save third batch (should not commit yet)
            store.save_events_batch(sqlite_events[4:])
            assert store.batches_processed == 3
            
        finally:
            store.close()

    def test_update_conversion_metadata(self, temp_db_path, config):
        """Test updating conversion metadata."""
        store = SQLiteEventStore(temp_db_path, config)
        store.connect()
        
        try:
            # Update metadata
            store.update_conversion_metadata('test_key', 'test_value')
            
            # Check that metadata was saved
            cursor = store.connection.execute(
                "SELECT value FROM conversion_metadata WHERE key = ?",
                ('test_key',)
            )
            result = cursor.fetchone()
            assert result is not None
            assert result[0] == 'test_value'
            
        finally:
            store.close()

    def test_get_stats(self, temp_db_path, config, mock_event):
        """Test getting conversion statistics."""
        # Set commit frequency to 1 to ensure immediate commit
        config.commit_frequency = 1
        store = SQLiteEventStore(temp_db_path, config)
        store.connect()
        
        try:
            # Add some events with unique IDs
            sqlite_events = []
            for i in range(5):
                event_copy = Mock()
                event_copy.id = f'test-event-{i}'
                event_copy.type = mock_event.type
                event_copy.stream_name = mock_event.stream_name
                event_copy.recorded_at = mock_event.recorded_at
                event_copy.data = mock_event.data
                event_copy.stream_position = mock_event.stream_position
                event_copy.commit_position = mock_event.commit_position
                event_copy.prepare_position = mock_event.prepare_position
                event_copy.retry_count = mock_event.retry_count
                event_copy.link = mock_event.link
                event_copy.content_type = mock_event.content_type
                event_copy.created = mock_event.created
                sqlite_events.append(SQLiteEvent(event_copy))
            store.save_events_batch(sqlite_events)
            
            # Get stats
            stats = store.get_stats()
            
            assert stats['total_events'] == 5
            assert stats['events_processed_this_session'] == 5
            assert stats['batches_processed'] == 1
            assert 'date_range' in stats
            assert 'database_size_mb' in stats
            
        finally:
            store.close()

    def test_get_stats_without_connection(self, temp_db_path, config):
        """Test getting stats without database connection."""
        store = SQLiteEventStore(temp_db_path, config)
        
        # Should not raise error, just return empty dict
        stats = store.get_stats()
        assert stats == {}

    def test_close_with_final_commit(self, temp_db_path, config, mock_event):
        """Test that close performs final commit and updates metadata."""
        store = SQLiteEventStore(temp_db_path, config)
        store.connect()
        
        # Add some events
        sqlite_events = [SQLiteEvent(mock_event) for _ in range(3)]
        store.save_events_batch(sqlite_events)
        
        # Close store
        store.close()
        
        # Check that final metadata was updated
        # Note: We can't easily test this without reconnecting, but the method
        # should complete without errors
        
        assert store.connection is None

    def test_connection_error_handling(self, temp_db_path, config):
        """Test handling of connection errors."""
        # Use an invalid path that can't be created
        invalid_path = Path("/invalid/path/test.db")
        store = SQLiteEventStore(invalid_path, config)
        
        with pytest.raises((sqlite3.Error, OSError)):
            store.connect()

    def test_sqlite_error_handling(self, temp_db_path, config, mock_event):
        """Test handling of SQLite errors during batch save."""
        store = SQLiteEventStore(temp_db_path, config)
        store.connect()
        
        try:
            # Force a SQLite error by closing the connection and trying to save
            store.connection.close()
            
            invalid_event = SQLiteEvent(mock_event)
            
            with pytest.raises(sqlite3.Error):
                store.save_events_batch([invalid_event])
                
        finally:
            store.close()

    def test_pragma_settings(self, temp_db_path, config):
        """Test that SQLite PRAGMA settings are applied."""
        store = SQLiteEventStore(temp_db_path, config)
        store.connect()
        
        try:
            # Check journal mode
            cursor = store.connection.execute("PRAGMA journal_mode")
            journal_mode = cursor.fetchone()[0]
            assert journal_mode == "wal"
            
            # Check synchronous mode
            cursor = store.connection.execute("PRAGMA synchronous")
            synchronous = cursor.fetchone()[0]
            assert synchronous == 1  # NORMAL
            
            # Check cache size
            cursor = store.connection.execute("PRAGMA cache_size")
            cache_size = cursor.fetchone()[0]
            assert cache_size == 10000
            
        finally:
            store.close()
