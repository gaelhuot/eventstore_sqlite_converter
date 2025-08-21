#!/usr/bin/env python3
"""
EventStore to SQLite Converter

Tool for collecting EventStore events and storing them in a SQLite database
with proper error handling, logging, configuration management, and resource cleanup.

Author: @gaelhuot
Version: 1.0.0
"""

import json
import logging
import os
import sqlite3
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, List, Optional, Dict, Any

import argparse
from dotenv import load_dotenv

from esdbclient import EventStoreDBClient
from esdbclient.events import RecordedEvent

# Load environment variables
load_dotenv()

# Constants
DEFAULT_BATCH_SIZE = 1000
DEFAULT_DB_PATH = "eventstore.db"
DEFAULT_LOG_LEVEL = "INFO"

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("eventstore_converter.log"),
    ],
)

logger = logging.getLogger(__name__)


@dataclass
class ConversionConfig:
    """Configuration for the EventStore to SQLite conversion process."""

    eventstore_uri: str
    db_path: Path
    batch_size: int = DEFAULT_BATCH_SIZE
    commit_frequency: int = 5  # Commit every N batches
    validate_data: bool = True
    create_indexes: bool = True

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if not self.eventstore_uri:
            raise ValueError("EventStore URI is required")

        if self.batch_size <= 0:
            raise ValueError("Batch size must be positive")

        if self.commit_frequency <= 0:
            raise ValueError("Commit frequency must be positive")

        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Configuration validated: {self}")


class EventValidationError(Exception):
    """Raised when event data validation fails."""

    pass


class SQLiteEvent:
    """
    EventStore event adapted for SQLite storage with validation.

    This class transforms EventStore events into a SQLite-compatible format
    with comprehensive validation and error handling.
    """

    MAX_DATA_SIZE = 1024 * 1024  # 1MB limit for event data

    def __init__(self, event: RecordedEvent, validate: bool = True) -> None:
        """
        Initialize SQLite event from EventStore event.

        Args:
            event: The original EventStore event
            validate: Whether to validate event data

        Raises:
            EventValidationError: If validation fails
        """
        self.id = str(event.id)
        self.type = event.type or "unknown"
        self.stream_name = event.stream_name or "unknown"
        self.recorded_at = int(event.recorded_at.timestamp())

        # Validate and process event data
        self.data = self._process_event_data(event.data, validate)

        # Convert EventStore metadata to JSON
        self.eventstore_metadata = self._serialize_metadata(event)

    def _process_event_data(self, data: Any, validate: bool) -> Optional[str]:
        """
        Process and validate event data.

        Args:
            data: Raw event data
            validate: Whether to perform validation

        Returns:
            Processed data as string or None

        Raises:
            EventValidationError: If validation fails
        """
        if data is None:
            return None

        # Convert bytes to string if needed
        if isinstance(data, bytes):
            try:
                data_str = data.decode("utf-8")
            except UnicodeDecodeError as e:
                if validate:
                    raise EventValidationError(f"Invalid UTF-8 data: {e}")
                logger.warning(f"Skipping invalid UTF-8 data for event {self.id}")
                return None
        elif isinstance(data, str):
            data_str = data
        else:
            # Convert other types to JSON string
            try:
                data_str = json.dumps(data)
            except (TypeError, ValueError) as e:
                if validate:
                    raise EventValidationError(f"Cannot serialize data: {e}")
                logger.warning(f"Skipping non-serializable data for event {self.id}")
                return None

        # Validate data size
        if validate and len(data_str.encode("utf-8")) > self.MAX_DATA_SIZE:
            raise EventValidationError(
                f"Event data too large: {len(data_str)} bytes > {self.MAX_DATA_SIZE}"
            )

        return data_str

    def _serialize_metadata(self, event: RecordedEvent) -> str:
        """
        Serialize EventStore metadata to JSON.

        Args:
            event: The original EventStore event

        Returns:
            JSON string of metadata
        """
        metadata = {
            "stream_position": getattr(event, "stream_position", None),
            "commit_position": getattr(event, "commit_position", None),
            "prepare_position": getattr(event, "prepare_position", None),
            "retry_count": getattr(event, "retry_count", 0),
            "link": getattr(event, "link", None),
            "content_type": getattr(event, "content_type", None),
            "created": getattr(event, "created", None),
        }

        # Remove None values and convert timestamps
        cleaned_metadata = {}
        for key, value in metadata.items():
            if value is not None:
                if hasattr(value, "timestamp"):  # Handle datetime objects
                    cleaned_metadata[key] = int(value.timestamp())
                else:
                    cleaned_metadata[key] = value

        return json.dumps(cleaned_metadata)


class SQLiteEventStore:
    """
    SQLite database manager for event storage with enhanced features.

    This class manages SQLite connections, schema creation, batch operations,
    and provides transaction management with proper resource cleanup.
    """

    def __init__(self, db_path: Path, config: ConversionConfig) -> None:
        """
        Initialize SQLite event store.

        Args:
            db_path: Path to SQLite database file
            config: Conversion configuration
        """
        self.db_path = db_path
        self.config = config
        self.connection: Optional[sqlite3.Connection] = None
        self.events_processed = 0
        self.batches_processed = 0

        logger.info(f"Initializing SQLite store at {db_path}")

    def __enter__(self) -> "SQLiteEventStore":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit with proper cleanup."""
        self.close()

    def connect(self) -> None:
        """Establish database connection and setup schema."""
        try:
            self.connection = sqlite3.connect(
                str(self.db_path),
                timeout=30.0,
                isolation_level="DEFERRED",  # Better performance for batch operations
            )

            # Configure SQLite for better performance
            self.connection.execute("PRAGMA journal_mode=WAL")
            self.connection.execute("PRAGMA synchronous=NORMAL")
            self.connection.execute("PRAGMA cache_size=10000")
            self.connection.execute("PRAGMA temp_store=MEMORY")

            self._create_schema()

            if self.config.create_indexes:
                self._create_indexes()

            logger.info("Database connection established and configured")

        except sqlite3.Error as e:
            logger.error(f"Failed to connect to database {self.db_path}: {e}")
            raise

    def _create_schema(self) -> None:
        """Create database schema if it doesn't exist."""
        if not self.connection:
            raise RuntimeError("Database connection not established")

        # Check if table exists
        cursor = self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='events'"
        )

        if cursor.fetchone() is not None:
            logger.info("Events table already exists")
            return

        # Create events table with better schema
        self.connection.execute(
            """
            CREATE TABLE events (
                id TEXT PRIMARY KEY,
                recorded_at INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                stream_name TEXT NOT NULL,
                data TEXT,
                eventstore_metadata TEXT,
                processed_at INTEGER DEFAULT (strftime('%s', 'now')),
                
                -- Add constraints
                CHECK (recorded_at > 0),
                CHECK (length(event_type) > 0),
                CHECK (length(stream_name) > 0)
            )
        """
        )

        # Create metadata table for conversion tracking
        self.connection.execute(
            """
            CREATE TABLE conversion_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at INTEGER DEFAULT (strftime('%s', 'now'))
            )
        """
        )

        self.connection.commit()
        logger.info("Database schema created successfully")

    def _create_indexes(self) -> None:
        """Create performance indexes."""
        if not self.connection:
            raise RuntimeError("Database connection not established")

        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_events_recorded_at ON events(recorded_at)",
            "CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type)",
            "CREATE INDEX IF NOT EXISTS idx_events_stream ON events(stream_name)",
            "CREATE INDEX IF NOT EXISTS idx_events_processed_at ON events(processed_at)",
        ]

        for index_sql in indexes:
            self.connection.execute(index_sql)

        self.connection.commit()
        logger.info("Database indexes created successfully")

    def save_events_batch(self, events: List[SQLiteEvent]) -> None:
        """
        Save a batch of events to the database.

        Args:
            events: List of events to save
        """
        if not events or not self.connection:
            return

        values = [
            (
                event.id,
                event.recorded_at,
                event.type,
                event.stream_name,
                event.data,
                event.eventstore_metadata,
            )
            for event in events
        ]

        try:
            self.connection.executemany(
                """
                INSERT OR REPLACE INTO events 
                (id, recorded_at, event_type, stream_name, data, eventstore_metadata) 
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                values,
            )

            self.events_processed += len(events)
            self.batches_processed += 1

            # Commit based on frequency setting
            if self.batches_processed % self.config.commit_frequency == 0:
                self.connection.commit()
                logger.debug(f"Committed batch {self.batches_processed}")

        except sqlite3.Error as e:
            logger.error(f"Failed to save events batch: {e}")
            raise

    def update_conversion_metadata(self, key: str, value: str) -> None:
        """Update conversion metadata for tracking progress."""
        if not self.connection:
            return

        self.connection.execute(
            """
            INSERT OR REPLACE INTO conversion_metadata (key, value)
            VALUES (?, ?)
        """,
            (key, value),
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get conversion statistics."""
        if not self.connection:
            return {}

        cursor = self.connection.execute("SELECT COUNT(*) FROM events")
        total_events = cursor.fetchone()[0]

        cursor = self.connection.execute(
            """
            SELECT MIN(recorded_at), MAX(recorded_at) FROM events
        """
        )
        date_range = cursor.fetchone()

        return {
            "total_events": total_events,
            "events_processed_this_session": self.events_processed,
            "batches_processed": self.batches_processed,
            "date_range": date_range,
            "database_size_mb": self.db_path.stat().st_size / (1024 * 1024),
        }

    def close(self) -> None:
        """Close database connection with final commit."""
        if self.connection:
            try:
                # Final commit
                self.connection.commit()

                # Update final statistics
                self.update_conversion_metadata(
                    "last_conversion", str(int(time.time()))
                )
                self.update_conversion_metadata(
                    "total_events", str(self.events_processed)
                )
                self.connection.commit()

                self.connection.close()
                logger.info("Database connection closed")

            except sqlite3.Error as e:
                logger.error(f"Error closing database: {e}")
            finally:
                self.connection = None


@contextmanager
def eventstore_client(uri: str) -> Iterator[EventStoreDBClient]:
    """
    Context manager for EventStore client with proper cleanup.

    Args:
        uri: EventStore connection URI

    Yields:
        EventStoreDBClient instance

    Raises:
        ConnectionError: If connection fails
        ValueError: If URI is invalid
    """
    client = None
    try:
        logger.info(f"Connecting to EventStore at {uri}")
        client = EventStoreDBClient(uri=uri)

        # Test connection by attempting to read from a non-existent stream
        # This will validate the connection without side effects
        try:
            list(client.get_stream("__connection_test__", limit=1))
        except Exception:
            # Expected - stream doesn't exist, but connection is working
            pass

        yield client

    except (ConnectionError, OSError, ValueError) as e:
        logger.error(f"EventStore connection failed: {e}")
        raise ConnectionError(f"Failed to connect to EventStore: {e}")
    except Exception as e:
        logger.error(f"Unexpected error with EventStore client: {e}")
        raise
    finally:
        if client:
            try:
                client.close()
                logger.info("EventStore client closed")
            except Exception as e:
                logger.warning(f"Error closing EventStore client: {e}")


def convert_events(config: ConversionConfig) -> Dict[str, Any]:
    """
    Convert events from EventStore to SQLite.

    Args:
        config: Conversion configuration

    Returns:
        Dictionary with conversion statistics

    Raises:
        ConnectionError: If EventStore connection fails
        sqlite3.Error: If SQLite operations fail
    """
    start_time = time.time()
    logger.info(f"Starting event conversion with config: {config}")

    with eventstore_client(config.eventstore_uri) as client:
        with SQLiteEventStore(config.db_path, config) as db:

            try:
                # Read all events from EventStore
                logger.info("Reading events from EventStore...")
                read_response = client.read_all()

                events_batch = []
                total_processed = 0
                skipped_events = 0

                try:
                    for event in read_response:
                        try:
                            sqlite_event = SQLiteEvent(event, config.validate_data)
                            events_batch.append(sqlite_event)

                            # Process batch when full
                            if len(events_batch) >= config.batch_size:
                                db.save_events_batch(events_batch)
                                total_processed += len(events_batch)

                                if total_processed % (config.batch_size * 10) == 0:
                                    logger.info(
                                        f"Processed {total_processed} events..."
                                    )

                                events_batch = []

                        except EventValidationError as e:
                            logger.warning(f"Skipping invalid event {event.id}: {e}")
                            skipped_events += 1
                            continue

                    # Process remaining events in the last batch
                    if events_batch:
                        db.save_events_batch(events_batch)
                        total_processed += len(events_batch)

                finally:
                    read_response.stop()

                # Get final statistics
                stats = db.get_stats()
                duration = time.time() - start_time

                stats.update(
                    {
                        "conversion_duration_seconds": duration,
                        "events_per_second": (
                            total_processed / duration if duration > 0 else 0
                        ),
                        "skipped_events": skipped_events,
                    }
                )

                logger.info(f"Conversion completed successfully: {stats}")
                return stats

            except Exception as e:
                logger.error(f"Error during event conversion: {e}")
                raise


def create_config_from_args(args: argparse.Namespace) -> ConversionConfig:
    """
    Create configuration from command line arguments.

    Args:
        args: Parsed command line arguments

    Returns:
        Validated configuration object

    Raises:
        ValueError: If configuration is invalid
    """
    eventstore_uri = os.getenv("EVENTSTORE_URI")
    if not eventstore_uri:
        raise ValueError(
            "EVENTSTORE_URI environment variable is required. "
            "Please set it in your .env file or environment."
        )

    db_path = Path(args.db).resolve()

    return ConversionConfig(
        eventstore_uri=eventstore_uri,
        db_path=db_path,
        batch_size=args.batch_size,
        commit_frequency=args.commit_frequency,
        validate_data=not args.skip_validation,
        create_indexes=not args.skip_indexes,
    )


def main() -> None:
    """Main entry point with comprehensive argument parsing and error handling."""
    parser = argparse.ArgumentParser(
        description="Convert EventStore events to SQLite database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --db events.db
  %(prog)s --db events.db --batch-size 500 --commit-frequency 10
  %(prog)s --db events.db --skip-validation --skip-indexes

Environment Variables:
  EVENTSTORE_URI    EventStore connection string (required)
  LOG_LEVEL         Logging level (DEBUG, INFO, WARNING, ERROR)
        """,
    )

    parser.add_argument(
        "--db",
        type=str,
        default=DEFAULT_DB_PATH,
        help=f"Path to SQLite database file (default: {DEFAULT_DB_PATH})",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Number of events to process in each batch (default: {DEFAULT_BATCH_SIZE})",
    )

    parser.add_argument(
        "--commit-frequency",
        type=int,
        default=5,
        help="Commit to database every N batches (default: 5)",
    )

    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip event data validation (faster but less safe)",
    )

    parser.add_argument(
        "--skip-indexes",
        action="store_true",
        help="Skip creating database indexes (faster initial import)",
    )

    parser.add_argument(
        "--version", action="version", version="EventStore SQLite Converter 1.0.0"
    )

    args = parser.parse_args()

    try:
        # Create and validate configuration
        config = create_config_from_args(args)

        # Run conversion
        stats = convert_events(config)

        # Print summary
        print("\n" + "=" * 50)
        print("CONVERSION COMPLETED SUCCESSFULLY")
        print("=" * 50)
        print(f"Database file: {config.db_path}")
        print(f"Total events: {stats['total_events']:,}")
        print(f"Duration: {stats['conversion_duration_seconds']:.2f} seconds")
        print(f"Rate: {stats['events_per_second']:.1f} events/second")
        print(f"Database size: {stats['database_size_mb']:.2f} MB")

        if stats["skipped_events"] > 0:
            print(f"Skipped events: {stats['skipped_events']:,}")

        sys.exit(0)

    except KeyboardInterrupt:
        logger.info("Conversion interrupted by user")
        print("\nConversion interrupted by user")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
