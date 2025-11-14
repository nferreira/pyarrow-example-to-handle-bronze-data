# Parquet S3 Blocks Writer

A Python application that generates fake data, writes it to Parquet files in configurable blocks, and uploads to S3 (supporting both AWS S3 and LocalStack).

## Features

- Generate realistic fake data using the Faker library
- Write Parquet files in configurable block sizes (default 1MB)
- Upload to S3 using multipart uploads for large files
- Support for both LocalStack (local development) and AWS S3
- Comprehensive logging and progress tracking
- Summary statistics for data generation, writing, and uploading
- Two main entry points:
  - **Standard Writer** (`main.py`): Production-ready Parquet writer with S3 upload
  - **Generator Example** (`generator/main.py`): Educational example demonstrating Python generators with CSV verification

## Documentation

This project includes comprehensive documentation:

- **[README.md](README.md)** - This file, main project overview
- **[QUICKSTART.md](QUICKSTART.md)** - Quick start guide to get running in 5 minutes
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Detailed architecture and component documentation
- **[WRITERS_COMPARISON.md](WRITERS_COMPARISON.md)** - Comparison of different Parquet writer implementations
- **[docs/parquet-writer-s3-output-stream.md](docs/parquet-writer-s3-output-stream.md)** - Technical deep dive into how ParquetWriter works with S3
- **[src/parquet_s3_blocks_writer/generator/docs/crash-course.md](src/parquet_s3_blocks_writer/generator/docs/crash-course.md)** - Comprehensive Python generators tutorial

## Requirements

- Python 3.13 or higher
- Poetry for dependency management
- LocalStack (optional, for local S3 testing)

## Installation

### 1. Install Poetry

If you don't have Poetry installed:

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

### 2. Clone and Install Dependencies

```bash
# Navigate to the project directory
cd parquet-s3-blocks-writer

# Install dependencies
poetry install
```

## Configuration

### Environment Variables

Copy the example environment file and customize it:

```bash
cp .env.example .env
```

Edit `.env` with your configuration:

```env
# LocalStack Configuration (default)
S3_ENDPOINT_URL=http://localhost:4566
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
S3_BUCKET_NAME=parquet-data-bucket

# Application Settings
TARGET_DATA_SIZE_MB=10
BLOCK_SIZE_MB=1
NUM_RECORDS=100000
```

### Configuration Options

| Variable | Description | Default |
|----------|-------------|---------|
| `S3_ENDPOINT_URL` | S3 endpoint (use for LocalStack, leave empty for AWS) | `http://localhost:4566` |
| `AWS_REGION` | AWS region | `us-east-1` |
| `AWS_ACCESS_KEY_ID` | AWS access key | `test` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | `test` |
| `S3_BUCKET_NAME` | S3 bucket name | `parquet-data-bucket` |
| `TARGET_DATA_SIZE_MB` | Target data size in MB | `10` |
| `BLOCK_SIZE_MB` | Block size for Parquet writing | `1` |
| `NUM_RECORDS` | Number of records to generate | `100000` |

## Running with LocalStack

### 1. Start LocalStack

Using Docker:

```bash
docker run --rm -d \
  -p 4566:4566 \
  -p 4510-4559:4510-4559 \
  --name localstack \
  localstack/localstack
```

Or using Docker Compose, create a `docker-compose.yml`:

```yaml
version: '3.8'

services:
  localstack:
    image: localstack/localstack
    ports:
      - "4566:4566"
      - "4510-4559:4510-4559"
    environment:
      - SERVICES=s3
      - DEBUG=1
      - DATA_DIR=/tmp/localstack/data
    volumes:
      - "./localstack-data:/tmp/localstack"
```

Then run:

```bash
docker-compose up -d
```

### 2. Verify LocalStack is Running

```bash
curl http://localhost:4566/_localstack/health
```

### 3. Create S3 Bucket (Optional)

The application **automatically creates the S3 bucket** if it doesn't exist. However, you can also create it manually:

#### Option A: Automatic Creation (Recommended)

The bucket is created automatically when you run the application. Both `S3Uploader` and `S3StreamingParquetWriter` include methods that check for bucket existence and create it if needed:
- `S3Uploader.create_bucket_if_not_exists()` - Called automatically during upload
- `S3StreamingParquetWriter._ensure_bucket_exists()` - Called automatically when writing starts

#### Option B: Manual Creation with AWS CLI

```bash
# For LocalStack
aws --endpoint-url=http://localhost:4566 s3 mb s3://parquet-data-bucket

# For AWS S3 (replace with your bucket name)
aws s3 mb s3://parquet-data-bucket --region us-east-1
```

#### Option C: Manual Creation with Python

```python
import boto3
from parquet_s3_blocks_writer.config import get_s3_config

config = get_s3_config()
s3_client = boto3.client(
    's3',
    endpoint_url=config.endpoint_url,
    region_name=config.region_name,
    aws_access_key_id=config.aws_access_key_id,
    aws_secret_access_key=config.aws_secret_access_key
)

# Create bucket if it doesn't exist
try:
    s3_client.head_bucket(Bucket=config.bucket_name)
    print(f"Bucket '{config.bucket_name}' already exists")
except:
    if config.region_name == "us-east-1":
        s3_client.create_bucket(Bucket=config.bucket_name)
    else:
        s3_client.create_bucket(
            Bucket=config.bucket_name,
            CreateBucketConfiguration={'LocationConstraint': config.region_name}
        )
    print(f"Created bucket '{config.bucket_name}'")
```

### 4. Run the Application

#### Option A: Standard Parquet Writer (Production)

```bash
# Using Poetry entry point script
poetry run parquet-s3-writer

# Or using Python module syntax (recommended)
poetry run python -m parquet_s3_blocks_writer.main

# Or activate the virtual environment first
poetry shell
python -m parquet_s3_blocks_writer.main
```

#### Option B: Generator Example (Educational)

```bash
# Using Poetry with Python module syntax (recommended)
poetry run python -m parquet_s3_blocks_writer.generator.main

# Or activate the virtual environment first
poetry shell
python -m parquet_s3_blocks_writer.generator.main
```

**Note**: The generator example includes:
- CSV verification (compares original data with decoded Parquet data)
- Memory usage comparison between generator and non-generator approaches
- Comprehensive memory allocation tracking

### 5. Verify Upload to LocalStack

```bash
# Install AWS CLI if not already installed
pip install awscli

# List buckets
aws --endpoint-url=http://localhost:4566 s3 ls

# List objects in the bucket
aws --endpoint-url=http://localhost:4566 s3 ls s3://parquet-data-bucket/

# Download the file to verify
aws --endpoint-url=http://localhost:4566 s3 cp s3://parquet-data-bucket/data.parquet ./downloaded.parquet
```

## Running with AWS S3

To use real AWS S3 instead of LocalStack:

1. Edit your `.env` file:

```env
# Comment out or remove S3_ENDPOINT_URL
# S3_ENDPOINT_URL=

# Set your AWS credentials
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_actual_access_key
AWS_SECRET_ACCESS_KEY=your_actual_secret_key
S3_BUCKET_NAME=your-actual-bucket-name
```

2. Run the application:

```bash
poetry run parquet-s3-writer
```

## Project Structure

```
parquet-s3-blocks-writer/
├── src/
│   └── parquet_s3_blocks_writer/
│       ├── __init__.py
│       ├── main.py                      # Main entry point (standard writer)
│       ├── config.py                    # Configuration management
│       ├── data_generator.py            # Fake data generation
│       ├── parquet_writer.py            # Parquet file writing
│       ├── parquet_writer_interface.py  # Abstract interface for writers
│       ├── s3_streaming_parquet_writer.py # Direct S3 streaming writer
│       ├── s3_uploader.py               # S3 upload logic
│       └── generator/
│           ├── main.py                  # Generator example entry point
│           └── docs/
│               └── crash-course.md      # Python generators tutorial
├── docs/
│   └── parquet-writer-s3-output-stream.md  # Technical deep dive
├── tests/                                # Test files
├── output/                               # Output directory for Parquet files
├── pyproject.toml                        # Poetry configuration
├── .env.example                          # Example environment variables
├── ARCHITECTURE.md                       # Architecture documentation
├── QUICKSTART.md                         # Quick start guide
├── WRITERS_COMPARISON.md                 # Writer implementations comparison
├── .gitignore                            # Git ignore rules
└── README.md                             # This file
```

## Usage Examples

### Standard Writer (Production)

```bash
# Using Poetry entry point script
poetry run parquet-s3-writer

# Or using Python module syntax (recommended)
poetry run python -m parquet_s3_blocks_writer.main
```

### Generator Example (Educational)

```bash
# Run the generator example with CSV verification
poetry run python -m parquet_s3_blocks_writer.generator.main
```

### Customize via Environment Variables

```bash
# Generate 1 million records with 2MB blocks
NUM_RECORDS=1000000 BLOCK_SIZE_MB=2 poetry run parquet-s3-writer

# Use a different bucket
S3_BUCKET_NAME=my-custom-bucket poetry run parquet-s3-writer

# Use LocalStack (default)
USE_LOCALSTACK=true LOCALSTACK_ENDPOINT_URL=http://localhost:4566 poetry run python -m parquet_s3_blocks_writer.generator.main
```

For more examples and detailed usage, see:
- **[QUICKSTART.md](QUICKSTART.md)** - Quick start examples
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Architecture and usage patterns
- **[WRITERS_COMPARISON.md](WRITERS_COMPARISON.md)** - Writer implementation details

## Development

### Running Tests

```bash
poetry run pytest
```

### Code Formatting

```bash
poetry run black src/
```

### Adding Dependencies

```bash
# Add a runtime dependency
poetry add package-name

# Add a development dependency
poetry add --group dev package-name
```

## How It Works

### 1. Data Generation

The application uses the Faker library to generate realistic fake data with the following columns:

- `id`: Sequential identifier
- `name`: Random person name
- `email`: Random email address
- `address`: Random street address
- `city`: Random city name
- `state`: Random state
- `zip_code`: Random ZIP code
- `country`: Random country
- `phone_number`: Random phone number
- `date_of_birth`: Random date of birth
- `job`: Random job title
- `company`: Random company name
- `ssn`: Random SSN
- `credit_card_number`: Random credit card number
- `iban`: Random IBAN
- `ipv4`: Random IPv4 address
- `user_agent`: Random user agent string
- `text`: Random text (up to 200 characters)

### 2. Parquet Writing

The ParquetBlockWriter:
- Estimates the average row size from a sample
- Calculates the number of rows per block based on the target block size
- Writes the data in row groups (blocks) to optimize storage and reading
- Uses Snappy compression by default

### 3. S3 Upload

The S3Uploader:
- Creates the bucket if it doesn't exist
- Uses simple upload for small files
- Uses multipart upload for large files (>5MB)
- Uploads in configurable chunk sizes (default 5MB per part)
- Provides comprehensive error handling and logging

## Output

The application provides detailed logging and a summary at the end:

```
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
  Compression: snappy
  Average row size: 982.45 bytes
  File path: ./output/data.parquet

S3 Upload:
  Time taken: 0.89 seconds
  Upload method: multipart
  Number of parts: 3
  Bucket: parquet-data-bucket
  S3 Key: data.parquet
  S3 URI: s3://parquet-data-bucket/data.parquet

Total Execution:
  Total time: 4.68 seconds
  Average throughput: 2.40 MB/s

================================================================================
```

## Troubleshooting

### LocalStack Connection Issues

If you get connection errors:

1. Verify LocalStack is running:
   ```bash
   docker ps | grep localstack
   ```

2. Check LocalStack logs:
   ```bash
   docker logs localstack
   ```

3. Ensure the endpoint URL is correct in your `.env` file

### Permission Errors

If you get AWS permission errors:

1. Verify your AWS credentials are correct
2. Ensure your IAM user has S3 permissions (s3:CreateBucket, s3:PutObject, s3:GetObject)
3. Check the bucket policy if using an existing bucket

### Memory Issues

If you run into memory issues with large datasets:

1. Reduce `NUM_RECORDS` in your `.env` file
2. The application processes data in blocks, so it should handle large datasets efficiently

## Additional Resources

- **[QUICKSTART.md](QUICKSTART.md)** - Get started quickly with step-by-step instructions
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Deep dive into the architecture and design decisions
- **[WRITERS_COMPARISON.md](WRITERS_COMPARISON.md)** - Compare different Parquet writer implementations
- **[docs/parquet-writer-s3-output-stream.md](docs/parquet-writer-s3-output-stream.md)** - Technical explanation of how ParquetWriter works with S3
- **[src/parquet_s3_blocks_writer/generator/docs/crash-course.md](src/parquet_s3_blocks_writer/generator/docs/crash-course.md)** - Learn Python generators with practical examples

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
