# Tests for EventStore SQLite Converter (Generated using Claude.ai)

This directory contains the complete suite of unit tests and integration tests for the EventStore to SQLite converter.

## Test Structure

### Unit Tests

- **`test_config.py`** - Tests for the `ConversionConfig` class
- **`test_sqlite_event.py`** - Tests for the `SQLiteEvent` class
- **`test_sqlite_eventstore.py`** - Tests for the `SQLiteEventStore` class
- **`test_eventstore_client.py`** - Tests for the EventStore context manager
- **`test_conversion.py`** - Tests for the `convert_events` function
- **`test_config_creation.py`** - Tests for configuration creation from CLI arguments
- **`test_main.py`** - Tests for the main function and CLI interface

### Integration Tests

- **`test_integration.py`** - End-to-end integration tests

### Configuration and Utilities

- **`conftest.py`** - Common fixtures and test configuration
- **`pytest.ini`** - Pytest configuration with code coverage

## Running Tests

### Run all tests
```bash
pytest
```

### Run with code coverage
```bash
pytest --cov=main --cov-report=html
```

### Run only unit tests
```bash
pytest -m "not integration"
```

### Run only integration tests
```bash
pytest -m integration
```

### Run with more details
```bash
pytest -v -s
```

## Code Coverage

The tests aim for complete code coverage and include:

- **Functional tests** : Verification that each function does what it should do
- **Error tests** : Verification of error handling and edge cases
- **Validation tests** : Verification of constraints and data validation
- **Performance tests** : Verification of SQLite optimizations
- **Integration tests** : Verification of the complete workflow

## Common Fixtures

The `conftest.py` file provides reusable fixtures:

- `temp_db_path` : Temporary path for test databases
- `sample_config` : Standard test configuration
- `mock_event` : Mocked EventStore event
- `mock_eventstore_client` : Mocked EventStore client
- `mock_sqlite_store` : Mocked SQLite store

## Mocking

The tests extensively use mocking to:

- Simulate EventStore connections
- Isolate components for unit testing
- Control test data
- Avoid external dependencies

## Best Practices

- Each test tests a single functionality
- Tests are independent and can be run in any order
- Fixtures are used for code reuse
- Tests include clear and descriptive assertions
- Code coverage is maintained at a high level