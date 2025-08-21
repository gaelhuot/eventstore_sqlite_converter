"""
EventStore Sample Data Generator

This module provides functionality to generate sample event data for testing and development
purposes in EventStore applications. It creates randomized events with realistic timestamps,
event types, and associated data to simulate real-world event streaming scenarios.

The module is designed to be configurable and extensible, allowing developers to:
- Customize the date range for event generation
- Define custom event types and data patterns
- Control the volume of sample data generated via command-line arguments
- Integrate with EventStore databases for data population

Features:
- Command-line interface with configurable sample size
- Randomized event generation with proper timestamp distribution
- Extensible event type and data configuration
- Professional-grade documentation and examples
- Direct EventStore database integration via esdbclient

Author: @gaelhuot
Version: 1.0.0

Usage:
    python scripts/sample_data.py --size 500
    python scripts/sample_data.py --size 2000
    python scripts/sample_data.py  # Default: 1000 events

Requirements:
    - EventStore database running (via Docker or local installation)
    - esdbclient package installed
    - EventStore accessible at localhost:1113
"""

import argparse
import random
import datetime
import uuid

from esdbclient import EventStoreDBClient, NewEvent, StreamState


class SampleEvent:
    """
    A sample event entity representing a single event in an event store.

    This class encapsulates the structure and generation logic for sample events,
    including event type, timestamp, and associated data. It provides a clean
    interface for creating randomized event instances that can be used for
    testing, development, and demonstration purposes.

    The class uses class-level configuration for date ranges, event types, and
    event data, making it easy to customize for different use cases.

    Attributes:
        event_type (str): The type/category of the event (e.g., 'event_type_1')
        event_date (datetime.datetime): The timestamp when the event occurred
        event_data (str): Additional data associated with the event

    Class Attributes:
        _start_date (datetime.datetime): Start of the date range for event generation
        _end_date (datetime.datetime): End of the date range for event generation
        _events_types (list): Available event types for random selection
        _events_data (list): Available event data values for random selection

    Example:
        >>> event = SampleEvent()
        >>> print(f"Event: {event.event_type} at {event.event_date}")
        Event: event_type_2 at 2025-01-15 14:30:22
    """

    # Class-level configuration for event generation
    # Date range for event generation (January 2025)
    _start_date = datetime.datetime(2025, 1, 1)
    _end_date = datetime.datetime(2025, 1, 31)

    # Available event types for random selection
    # These can be customized to match your domain-specific event types
    _events_types = [
        "event_type_1",
        "event_type_2",
        "event_type_3",
    ]

    # Available event data values for random selection
    # These can be customized to include relevant business data
    _events_data = [
        "event_data_1",
        "event_data_2",
        "event_data_3",
    ]

    def _get_random_timestamp(self):
        """
        Generate a random timestamp within the configured date range.

        This method converts the configured start and end dates to Unix timestamps,
        generates a random timestamp between them, and converts it back to a
        datetime object. This approach ensures proper distribution across the
        entire date range including time components.

        Returns:
            datetime.datetime: A random datetime object within the configured range

        Note:
            The method uses Unix timestamps (seconds since epoch) for precise
            random selection across the entire datetime range.
        """
        start_timestamp = int(self._start_date.timestamp())
        end_timestamp = int(self._end_date.timestamp())
        random_timestamp = random.randint(start_timestamp, end_timestamp)
        return datetime.datetime.fromtimestamp(random_timestamp)

    def __init__(self):
        """
        Initialize a new SampleEvent instance with randomized attributes.

        Upon instantiation, the event is populated with:
        - A randomly selected event type from the predefined list
        - A random timestamp within the configured date range
        - Randomly selected event data from the available options

        The randomization ensures each event instance is unique and provides
        realistic variety for testing scenarios.
        """
        self.event_type = random.choice(self._events_types)  # Random event type
        self.event_date = self._get_random_timestamp()  # Random event date
        self.event_data = random.choice(self._events_data)  # Random event data

    def insert(self, client: EventStoreDBClient):
        """
        Insert the event into the EventStore database.

        This method creates a new stream for each event and appends the event
        to that stream. Each event gets its own unique stream identifier,
        which is useful for testing and demonstration purposes.

        Args:
            client (EventStoreDBClient): The EventStore client instance

        Note:
            - Each event creates a new stream with a unique UUID
            - Event data is encoded as bytes before insertion
            - Uses StreamState.NO_STREAM for new stream creation
        """
        # Generate a unique stream name for this event
        stream_name = f"test_stream_{uuid.uuid4()}"

        # Append the event to the new stream
        # The event data is encoded as bytes and the event type is set
        client.append_to_stream(
            stream_name=stream_name,
            current_version=StreamState.NO_STREAM,
            events=[NewEvent(type=self.event_type, data=self.event_data.encode())],
        )


def main(sample_size):
    """
    Main execution function for generating sample event data.

    This function serves as the primary entry point for the script, generating
    the specified number of sample events and inserting them into the EventStore
    database. It demonstrates the usage of the SampleEvent class and provides
    a simple way to generate bulk sample data for development and testing purposes.

    Args:
        sample_size (int): The number of sample events to generate and insert

    Note:
        - Connects to EventStore at localhost:1113 (non-TLS)
        - Each event is inserted into its own stream
        - Progress is not displayed during insertion (consider adding for large datasets)

    Example:
        >>> main(100)  # Generate and insert 100 sample events
    """
    # Initialize EventStore client connection
    # Configuration: localhost, non-TLS, with keep-alive settings
    client = EventStoreDBClient(
        uri="esdb://localhost:2113?tls=false&keepAliveInterval=10000&keepAliveTimeout=10000",
        root_certificates=None,
    )

    # Generate and insert the specified number of sample events
    for i in range(sample_size):
        event = SampleEvent()
        event.insert(client)


if __name__ == "__main__":
    # Command-line argument parsing for configurable sample size
    parser = argparse.ArgumentParser(
        description="Generate sample event data for EventStore database",
        epilog="Example: python scripts/sample_data.py --size 500",
    )
    parser.add_argument(
        "--size",
        type=int,
        default=1000,
        help="Number of sample events to generate and insert (default: 1000)",
    )
    args = parser.parse_args()

    # Execute the main function with the specified sample size
    main(args.size)
