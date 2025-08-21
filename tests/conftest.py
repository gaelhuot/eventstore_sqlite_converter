"""
Common test fixtures and configuration. (Generated using Claude.ai)
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock
from datetime import datetime
from main import ConversionConfig


@pytest.fixture
def temp_db_path():
    """Create a temporary database path for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        temp_path = Path(f.name)
    yield temp_path
    # Cleanup
    if temp_path.exists():
        temp_path.unlink()


@pytest.fixture
def sample_config(temp_db_path):
    """Create a sample configuration for testing."""
    return ConversionConfig(
        eventstore_uri="esdb://localhost:2113",
        db_path=temp_db_path,
        batch_size=100,
        commit_frequency=5
    )


@pytest.fixture
def mock_event():
    """Create a mock EventStore event for testing."""
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


@pytest.fixture
def mock_eventstore_client():
    """Create a mock EventStore client for testing."""
    client = Mock()
    client.read_all.return_value = Mock()
    return client


@pytest.fixture
def mock_sqlite_store():
    """Create a mock SQLite store for testing."""
    store = Mock()
    store.get_stats.return_value = {
        'total_events': 0,
        'events_processed_this_session': 0,
        'batches_processed': 0,
        'date_range': (None, None),
        'database_size_mb': 0.0
    }
    return store
