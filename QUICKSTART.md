# Quick Start Guide

Get up and running with Parquet S3 Blocks Writer in under 5 minutes!

## Prerequisites

- Python 3.13+
- Poetry
- Docker (for LocalStack)

## Quick Setup

### Option 1: Using Make (Recommended)

```bash
# Install dependencies
make install

# Setup environment file
make setup

# Start LocalStack
make localstack-start

# Run the application
make run
```

### Option 2: Manual Setup

```bash
# 1. Install dependencies
poetry install

# 2. Copy environment file
cp .env.example .env

# 3. Start LocalStack
docker-compose up -d

# 4. Run the application
poetry run parquet-s3-writer
```

## Verify Installation

Check that LocalStack is running:

```bash
make localstack-status
```

Or manually:

```bash
curl http://localhost:4566/_localstack/health
```

## Expected Output

You should see output similar to:

```
2024-01-15 10:30:45,123 - parquet_s3_blocks_writer.main - INFO - Starting Parquet S3 Blocks Writer
2024-01-15 10:30:45,124 - parquet_s3_blocks_writer.main - INFO - ================================================================================
...
================================================================================
EXECUTION SUMMARY
================================================================================

Data Generation:
  Time taken: 2.34 seconds
  Number of rows: 100,000
  Number of columns: 18

Parquet File Writing:
  Time taken: 1.45 seconds
  File size: 11.23 MB
  Number of blocks (row groups): 12
  ...

S3 Upload:
  Time taken: 0.89 seconds
  Upload method: multipart
  Number of parts: 3
  ...
```

## Verify the Upload

Check that the file was uploaded to LocalStack:

```bash
# List buckets
aws --endpoint-url=http://localhost:4566 s3 ls

# List files in bucket
aws --endpoint-url=http://localhost:4566 s3 ls s3://parquet-data-bucket/

# Download the file
aws --endpoint-url=http://localhost:4566 s3 cp s3://parquet-data-bucket/data.parquet ./test-download.parquet
```

## Customization

Edit your `.env` file to customize:

```env
# Generate more records
NUM_RECORDS=500000

# Use larger blocks
BLOCK_SIZE_MB=2

# Change bucket name
S3_BUCKET_NAME=my-data-bucket
```

## Common Commands

```bash
# Show all available commands
make help

# Run tests
make test

# Format code
make format

# Clean generated files
make clean

# Stop LocalStack
make localstack-stop

# View LocalStack logs
make localstack-logs
```

## Troubleshooting

### Port Already in Use

If port 4566 is already in use:

```bash
# Find and stop the process
lsof -ti:4566 | xargs kill -9

# Or stop existing LocalStack
docker stop localstack-parquet
```

### Poetry Not Found

Install Poetry:

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

### Docker Not Running

Start Docker Desktop or your Docker daemon before running LocalStack.

## Next Steps

- Read the full [README.md](README.md) for detailed documentation
- Modify the data schema in `src/parquet_s3_blocks_writer/data_generator.py`
- Add custom processing logic in `src/parquet_s3_blocks_writer/main.py`
- Configure for AWS S3 instead of LocalStack (see README)

## Getting Help

- Check the logs: `make localstack-logs`
- Run tests: `make test`
- Review configuration: `cat .env`
