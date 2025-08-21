# EventStore to SQLite Converter

Simple python tool for converting EventStore events to SQLite database.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Database Schema](#database-schema)
- [Performance Guidelines](#performance-guidelines)
- [Development](#development)
- [Troubleshooting](#troubleshooting)
- [Tests](#tests)
- [License](#license)

## Features

## Prerequisites

- **Python 3.10+** (Tested with 3.12)
- **EventStoreDB** running (Docker Compose provided)
- **SQLite 3.x** (Built into Python)

## Installation

### 1. Clone and Setup

```bash
git clone <repository-url>
cd eventstore_sqlite_converter

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit .env file
EVENTSTORE_URI=esdb://localhost:2113?tls=false
LOG_LEVEL=INFO
```

### 4. Start EventStoreDB (Optional)

```bash
cd docker && docker compose up -d
```

## Usage

### Basic Usage

```bash
# Basic conversion
python main.py

# With custom database path
python main.py --db my_events.db

# Advanced configuration
python main.py --db events.db --batch-size 2000 --commit-frequency 3
```

### Command Line Options

```bash
python main.py [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--db` | `eventstore.db` | Path to SQLite database file |
| `--batch-size` | `1000` | Events per batch (memory management) |
| `--commit-frequency` | `5` | Commit every N batches (performance) |
| `--skip-validation` | `False` | Skip data validation (faster, less safe) |
| `--skip-indexes` | `False` | Skip index creation (faster import) |
| `--version` | - | Show version information |
| `--help` | - | Show help message |

### Advanced Examples

```bash
# Production conversion with optimized settings
python main.py --db production_events.db --batch-size 5000 --commit-frequency 10

# Fast import without validation (use with caution)
python main.py --db fast_import.db --skip-validation --skip-indexes

# Debug mode with detailed logging
LOG_LEVEL=DEBUG python main.py --db debug_events.db --batch-size 100
```

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `EVENTSTORE_URI` | Yes | - | EventStore connection string |
| `LOG_LEVEL` | No | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |

#### EventStore URI Examples

```bash
# Local insecure connection
EVENTSTORE_URI=esdb://localhost:2113?tls=false

# Secure connection with authentication
EVENTSTORE_URI=esdb://admin:changeit@localhost:2113

# Cluster connection
EVENTSTORE_URI=esdb+discover://admin:changeit@cluster.example.com:2113
```

## Database Schema

The tool creates a SQLite database with the following optimized schema:

```sql
-- Main events table
CREATE TABLE events (
    id TEXT PRIMARY KEY,                    -- EventStore event ID
    recorded_at INTEGER NOT NULL,          -- Unix timestamp
    event_type TEXT NOT NULL,              -- Event type/category
    stream_name TEXT NOT NULL,             -- EventStore stream name
    data TEXT,                             -- Event data (JSON/text)
    eventstore_metadata TEXT,              -- Serialized EventStore metadata
    processed_at INTEGER DEFAULT (strftime('%s', 'now')),  -- Processing timestamp
    
    -- Data integrity constraints
    CHECK (recorded_at > 0),
    CHECK (length(event_type) > 0),
    CHECK (length(stream_name) > 0)
);

-- Conversion tracking table
CREATE TABLE conversion_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at INTEGER DEFAULT (strftime('%s', 'now'))
);

-- Performance indexes
CREATE INDEX idx_events_recorded_at ON events(recorded_at);
CREATE INDEX idx_events_type ON events(event_type);
CREATE INDEX idx_events_stream ON events(stream_name);
CREATE INDEX idx_events_processed_at ON events(processed_at);
```

### Query Examples

```sql
-- Events by date range
SELECT * FROM events 
WHERE recorded_at BETWEEN 1640995200 AND 1672531200
ORDER BY recorded_at;

-- Events by type
SELECT event_type, COUNT(*) as count 
FROM events 
GROUP BY event_type 
ORDER BY count DESC;

-- Stream activity
SELECT stream_name, COUNT(*) as event_count,
       datetime(MIN(recorded_at), 'unixepoch') as first_event,
       datetime(MAX(recorded_at), 'unixepoch') as last_event
FROM events 
GROUP BY stream_name 
ORDER BY event_count DESC;

-- Conversion statistics
SELECT * FROM conversion_metadata;
```

## Performance Guidelines

### Recommended Settings

| Scenario | Batch Size | Commit Frequency | Notes |
|----------|------------|------------------|-------|
| **Development** | 100-500 | 1-2 | Frequent commits for debugging |
| **Small datasets** | 1000 | 5 | Default settings |
| **Large datasets** | 5000-10000 | 10-20 | Optimized for throughput |
| **Memory constrained** | 500-1000 | 2-5 | Lower memory usage |

### Performance Tips

1. **Use WAL mode**: Automatically enabled for better concurrent access
2. **Tune batch size**: Larger batches = better performance, more memory
3. **Adjust commit frequency**: Less frequent commits = better performance
4. **Skip validation**: Use `--skip-validation` for trusted data sources
5. **Defer indexes**: Use `--skip-indexes` for initial bulk import

### Typical Performance

- **Small datasets** (< 10K events): 2000-5000 events/second
- **Medium datasets** (10K-100K events): 1000-3000 events/second  
- **Large datasets** (> 100K events): 500-2000 events/second

*Performance varies based on hardware, EventStore setup, and data complexity.*

## Development

### Sample Data Generation

Generate test data for development and testing:

```bash
# Generate 1000 sample events (default)
python scripts/sample_data.py

# Generate custom amount
python scripts/sample_data.py --size 5000
```

### Docker Environment

Complete EventStoreDB environment with health checks:

```bash
# Start EventStoreDB
cd docker && docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f eventstore

# Stop environment
docker compose down
```

Access EventStoreDB Web UI: http://localhost:2113

## Troubleshooting

### Common Issues

#### Connection Errors
```bash
# Check EventStore status
curl -f http://localhost:2113/health/live

# Verify connection string
echo $EVENTSTORE_URI

# Test with verbose logging
LOG_LEVEL=DEBUG python main.py --db test.db
```

#### Memory Issues
```bash
# Reduce batch size
python main.py --batch-size 500

# Increase commit frequency
python main.py --commit-frequency 2
```

#### Performance Issues
```bash
# Skip validation for trusted data
python main.py --skip-validation

# Defer index creation
python main.py --skip-indexes

# Increase batch size
python main.py --batch-size 5000
```

### Log Analysis

Check logs for detailed error information:

```bash
# View recent logs
tail -f eventstore_converter.log

# Search for errors
grep -i error eventstore_converter.log

# Performance analysis
grep -i "events/second" eventstore_converter.log
```

### Development Setup
```bash
# Install development dependencies
pip install pytest pytest-cov black isort mypy flake8

# Run tests
pytest

# Format code
black main.py scripts/

# Type checking
mypy main.py

# Linting
flake8 main.py
```

## Tests (Generated using Claude.ai)

[Tests](tests/README.md) documentation can be found in the tests directory.

Run tests:
```bash
pytest
```

Run tests with code coverage:
```bash
pytest --cov=main --cov-report=html
```


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.