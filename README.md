# IoT Data Pipeline

[![CI](https://github.com/samuel-jinadu/iot-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/samuel-jinadu/iot-data-pipeline/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.12+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

A production-ready PySpark data pipeline for processing IoT device telemetry data. Built with a medallion architecture (bronze/silver/gold), comprehensive data quality validation, and Delta Lake storage.

## Features

- **Medallion Architecture**: Bronze → Silver → Gold data layers using Delta Lake
- **Data Quality Validation**: Configurable rules for strings, integers, timestamps, and null handling
- **Delta Lake Storage**: ACID transactions, time travel, and schema evolution
- **Containerized**: Docker-based development and production images
- **Config-Driven**: YAML configuration for environment-specific settings
- **Comprehensive Testing**: Unit and integration tests with 80% coverage requirement
- **Type-Safe**: Python 3.12+ with frozen configuration classes
- **Fast Dependency Management**: Uses `uv` for lightning-fast installs

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Raw JSON  │────▶│   Bronze    │────▶│   Silver    │────▶│    Gold     │
│    Data     │     │   (Delta)   │     │   (Delta)   │     │   (Delta)   │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                           │                   │                   │
                           │                   │                   │
                    ┌──────▼──────┐     ┌──────▼──────┐     ┌──────▼──────┐
                    │   Extract   │     │   Quality   │     │  Aggregate  │
                    │   Schema    │     │    Rules    │     │   by Country│
                    └─────────────┘     └─────────────┘     └─────────────┘
```

## Tech Stack

- **Apache Spark 4.2.0** with PySpark
- **Delta Lake 4.0** for ACID storage
- **Python 3.12+**
- **Docker** with multi-stage builds
- **uv** for dependency management
- **pytest** + **chispa** for testing
- **ruff** + **black** for linting/formatting

## Project Structure

```
.
├── iot_pipeline/
│   ├── __init__.py
│   ├── __main__.py          # Entry point
│   ├── main.py              # CLI argument handling
│   ├── config.py            # Configuration classes & validation
│   ├── session.py           # SparkSession builder
│   ├── pipeline.py          # ETL orchestration
│   ├── etl.py               # Extract, clean, save operations
│   ├── quality.py           # Data quality validations
│   ├── analysis.py          # Business logic (aggregations)
│   ├── logs.py              # Logging setup
│   └── exceptions.py        # Custom exceptions
├── tests/
│   ├── conftest.py          # Pytest fixtures
│   ├── test_*.py            # Test modules
│   └── fixtures/            # Test data & configs
├── yaml/
│   ├── dev.yaml             # Development config
│   └── prod.yaml            # Production config
├── scripts/
│   └── run.sh               # Local run script
├── pyproject.toml           # Project metadata & dependencies
├── Dockerfile               # Multi-stage Docker build
├── compose.yaml             # Docker Compose orchestration
└── .github/workflows/ci.yml # CI pipeline
```

## Prerequisites

- **Docker** and **Docker Compose** (recommended)
- **Python 3.12+** with **uv** (for local development)
- **Java 21** (if running Spark locally)

## Quick Start

### Using Docker (Recommended)

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd iot-pipeline
   ```

2. **Prepare your data**
   ```bash
   # Place your IoT data in data/iot_devices.json
   mkdir -p data logs
   cp /path/to/your/iot_devices.json data/
   ```

3. **Run the pipeline**
   ```bash
   docker compose up --build
   ```

### Local Development

1. **Install dependencies**
   ```bash
   uv sync --locked
   ```

2. **Run the pipeline**
   ```bash
   uv run iot_pipeline yaml/dev.yaml
   ```

## Configuration

Configuration is managed via YAML files in the `yaml/` directory. Key settings include:

### Core Settings

| Setting | Description | Default |
|---------|-------------|---------|
| `LOGLEVEL` | Logging level (DEBUG/INFO/WARNING/ERROR/CRITICAL) | `WARNING` |
| `LOGFILE` | Path to log file | `logs/pipeline.log` |
| `APPNAME` | Spark application name | `IoT Pipeline` |
| `INPUTDATAPATH` | Path to input JSON/JSONL/CSV file | `data/iot_devices.json` |
| `OUTPUTFOLDERPATH` | Output directory for Delta tables | `data/output` |
| `EXTRACT_SCHEMA` | Spark DDL schema string | **Required** |

### Data Quality Rules

| Setting | Description |
|---------|-------------|
| `DIRTY_DATA_THRESHOLD_PCT` | Max % of dirty rows allowed (0-100) |
| `THREE_CHAR_STR` | Columns that must be exactly 3 characters |
| `TWO_CHAR_STR` | Columns that must be exactly 2 characters |
| `POSITIVE_INTEGER` | Columns that must be non-negative |
| `MILLI_TIMESTAMP` | Columns that must be valid millisecond timestamps |
| `DROP_ON_NULL` | Columns to drop rows with nulls |

### Spark Optimizations

| Setting | Description | Default |
|---------|-------------|---------|
| `SPARK_MASTER` | Spark master URL | `local[*]` |
| `SPARK_SQL_SHUFFLE_PARTITIONS` | Number of shuffle partitions | `8` |
| `SPARK_SQL_SESSION_TIMEZONE` | Session timezone | `UTC` |
| `SPARK_MEMORY_FRACTION` | Memory fraction (0.5-0.7) | `0.6` |
| `SPARK_DRIVER_MEMORY` | Driver memory (e.g., `2g`) | `1g` |
| `CHECK_EXECUTION_PLAN` | Log query plan instead of saving gold | `False` |

### Example Configuration

```yaml
# yaml/dev.yaml
LOGLEVEL: "INFO"
LOGFILE: "logs/pipeline.log"

INPUTDATAPATH: "data/iot_devices.json"
OUTPUTFOLDERPATH: "data/output"

DIRTY_DATA_THRESHOLD_PCT: 10
THREE_CHAR_STR: ["cca3"]
TWO_CHAR_STR: ["cca2"]
POSITIVE_INTEGER: ["c02_level", "battery_level", "humidity", "temp", "device_id"]
MILLI_TIMESTAMP: ["timestamp"]
DROP_ON_NULL: ["c02_level", "battery_level", "humidity", "temp", "device_id", "timestamp"]

EXTRACT_SCHEMA: "device_id INT, device_name STRING, ip STRING, cca2 STRING, cca3 STRING, cn STRING, latitude DECIMAL(7,3), longitude DECIMAL(7,3), scale STRING, temp INT, humidity INT, battery_level INT, c02_level INT, lcd STRING, timestamp LONG"

APPNAME: "IoT Pipeline"
SPARK_MASTER: "local[2]"
SPARK_SQL_SHUFFLE_PARTITIONS: 4
SPARK_MEMORY_FRACTION: 0.5
SPARK_DRIVER_MEMORY: 2g
SPARK_SQL_SESSION_TIMEZONE: UTC
SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE: dynamic
SPARK_SERIALIZER: org.apache.spark.serializer.KryoSerializer
SPARK_SQL_PARQUET_COMPRESSION_CODEC: zstd
SPARK_SQL_COLUMN_NAME_OF_CORRUPT_RECORD: _rescued_data
CHECK_EXECUTION_PLAN: True
```

## Data Flow

1. **Extract**: Reads JSON/JSONL/CSV with specified schema
2. **Bronze**: Raw data written to `data/output/bronze/<filename>.delta`
3. **Quality Check**: Applies validation rules, calculates dirty data percentage
4. **Clean**: Drops rows with nulls in specified columns
5. **Silver**: Cleaned data written to `data/output/silver/<filename>.delta`
6. **Analyze**: Aggregates devices per country
7. **Gold**: Analysis results written to `data/output/gold/<analysis>.delta`

## Testing

### Run all tests

```bash
uv run pytest
```

### Run with coverage report

```bash
uv run pytest --cov=iot_pipeline --cov-report=html
```

### Run specific test categories

```bash
# Unit tests only
uv run pytest -m "not integration"

# Integration tests only
uv run pytest -m integration
```

### Test structure

- **Unit tests**: Mocked Spark operations, fast execution
- **Integration tests**: Real SparkSession, Delta Lake, file I/O
- **Fixtures**: Shared test data in `tests/fixtures/`

## Development

### Code Quality

```bash
# Format code
uv run black .

# Lint
uv run ruff check

# Fix lint issues
uv run ruff check --fix
```

### Docker Build

```bash
# Build test image
docker build --target test -t iot-pipeline:test .

# Build production image
docker build --target prod -t iot-pipeline:prod .
```

### CI Pipeline

The GitHub Actions workflow (`.github/workflows/ci.yml`) runs:
1. Build test Docker image
2. Black formatting check
3. Ruff linting
4. Pytest with coverage
5. Build production image

## Output

### Delta Tables

- **Bronze**: `data/output/bronze/<input_filename>.delta`
- **Silver**: `data/output/silver/<input_filename>.delta`
- **Gold**: `data/output/gold/devices_per_country.delta`

### Logs

- Pipeline logs: `logs/pipeline.log`
- Rescued data backup: `data/output/_rescued_data.csv`

## Error Handling

The pipeline includes custom exceptions:

| Exception | Description |
|-----------|-------------|
| `ConfigurationValidationException` | Invalid config value |
| `DataQualityException` | Too many dirty rows |
| `ZeroRowsDataFrameException` | Empty DataFrame |
| `ConfigurationImmutabilityRule` | Attempt to modify frozen config |
| `MissingConfigurationKeyNoReasonableDefaultException` | Required config missing |

## Performance Considerations

- **Caching**: DataFrames cached during pipeline execution
- **Partitioning**: Configurable shuffle partitions
- **Compression**: Zstd codec for Parquet files
- **Serialization**: KryoSerializer for faster serialization
- **Delta Lake**: Optimized storage with ACID transactions

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

Test data derived from [Learning Spark: Lightning-Fast Data Analytics [2nd Edition]](https://github.com/databricks/LearningSparkV2) under Apache License 2.0. See [NOTICE](NOTICE) for details.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Support

For issues and questions, please open an issue on the GitHub repository.