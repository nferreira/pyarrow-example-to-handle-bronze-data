"""S3 Streaming Parquet writer using PyArrow's filesystem."""

import logging
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
from botocore.exceptions import ClientError

from .config import S3Config
from .parquet_writer_interface import ParquetBlockWriter

logger = logging.getLogger(__name__)


class S3StreamingParquetWriter(ParquetBlockWriter):
    """Write Parquet files directly to S3 using PyArrow's filesystem.

    This implementation uses PyArrow's ParquetWriter with S3FileSystem,
    which handles streaming writes and multipart uploads automatically.
    Each write_block() call writes a row group to the same Parquet file.
    """

    def __init__(self, s3_config: S3Config, block_size_mb: int = 1, part_size_mb: int = 5):
        """Initialize the S3 Streaming Parquet writer.

        Args:
            s3_config: S3 configuration with endpoint, credentials, etc.
            block_size_mb: Target block size in megabytes (for reference/logging)
            part_size_mb: Unused (kept for API compatibility)
        """
        self.s3_config = s3_config
        self.block_size_mb = block_size_mb
        self.block_size_bytes = block_size_mb * 1024 * 1024

        # Create PyArrow S3FileSystem
        if s3_config.endpoint_url:
            # For LocalStack or custom endpoints
            self.fs = pafs.S3FileSystem(
                access_key=s3_config.aws_access_key_id,
                secret_key=s3_config.aws_secret_access_key,
                endpoint_override=s3_config.endpoint_url,
                region=s3_config.region_name,
            )
            logger.info(f"Using custom S3 endpoint: {s3_config.endpoint_url}")
        else:
            # For AWS S3
            self.fs = pafs.S3FileSystem(
                access_key=s3_config.aws_access_key_id,
                secret_key=s3_config.aws_secret_access_key,
                region=s3_config.region_name,
            )

        self.bucket_name = s3_config.bucket_name

        # Writer state
        self._writer: Optional[pq.ParquetWriter] = None
        self._s3_path: Optional[str] = None
        self._compression: str = "snappy"
        self._num_blocks = 0
        self._total_rows = 0
        self._num_columns: Optional[int] = None
        self._schema: Optional[pa.Schema] = None

        logger.info(
            f"Initialized S3StreamingParquetWriter with {block_size_mb}MB block size"
        )
        logger.info(f"S3 Endpoint: {s3_config.endpoint_url or 'AWS'}")
        logger.info(f"S3 Bucket: {s3_config.bucket_name}")

    def start_writing(self, output_path: str, compression: str = "snappy") -> None:
        """Initialize the writer and prepare for block writing.

        Args:
            output_path: S3 key (filename within the bucket, or full s3:// path)
            compression: Compression codec to use (snappy, gzip, zstd, etc.)
        """
        if self._writer is not None:
            raise RuntimeError("Writer already started. Call finish_writing() first.")

        # Construct S3 path for PyArrow filesystem
        if output_path.startswith("s3://"):
            # Extract bucket and key from s3://bucket/key format
            path_parts = output_path[5:].split("/", 1)
            bucket = path_parts[0]
            key = path_parts[1] if len(path_parts) > 1 else ""
            self._s3_path = f"{bucket}/{key}"
        else:
            # Use configured bucket
            self._s3_path = f"{self.bucket_name}/{output_path}"

        self._compression = compression
        self._num_blocks = 0
        self._total_rows = 0
        self._num_columns = None
        self._schema = None

        # Ensure bucket exists
        self._ensure_bucket_exists()

        logger.info(f"Starting Parquet file writing to s3://{self._s3_path}")
        logger.info(f"Compression: {compression}")

    def write_block(self, df: pd.DataFrame) -> None:
        """Write a DataFrame block as a row group to S3.

        This method will be called multiple times with different DataFrames.
        Each DataFrame is written as a separate row group within the same
        Parquet file. The data is streamed directly to S3.

        Args:
            df: pandas DataFrame containing the data block to write

        Raises:
            RuntimeError: If start_writing() hasn't been called
            ValueError: If DataFrame schema doesn't match previous blocks
        """
        if self._s3_path is None:
            raise RuntimeError("Writer not started. Call start_writing() first.")

        if df.empty:
            logger.warning("Received empty DataFrame block, skipping...")
            return

        # Convert DataFrame to PyArrow Table
        table = pa.Table.from_pandas(df, preserve_index=False)

        # Initialize writer on first block
        if self._writer is None:
            self._schema = table.schema
            self._num_columns = table.num_columns

            # Create ParquetWriter with S3 filesystem
            self._writer = pq.ParquetWriter(
                self._s3_path,
                self._schema,
                filesystem=self.fs,
                compression=self._compression,
                use_dictionary=True,
                version="2.6",
            )
            logger.info(f"Initialized ParquetWriter writing to s3://{self._s3_path}")
            logger.info(f"Schema: {self._schema}")

        # Validate schema consistency
        if not table.schema.equals(self._schema):
            raise ValueError(
                f"DataFrame schema mismatch. Expected {self._schema}, got {table.schema}"
            )

        # Write the block as a row group
        # PyArrow handles the streaming and multipart upload automatically
        self._writer.write_table(table)
        self._num_blocks += 1
        self._total_rows += len(df)

        block_size_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        logger.info(
            f"Wrote block {self._num_blocks} to S3: {len(df):,} rows "
            f"(~{block_size_mb:.2f} MB in memory)"
        )
        logger.debug(f"Total rows written so far: {self._total_rows:,}")

    def finish_writing(self) -> dict:
        """Finalize the Parquet file on S3 and return statistics.

        This closes the ParquetWriter, which writes the final metadata/footer
        and completes any pending multipart uploads.

        Returns:
            Dictionary with write statistics

        Raises:
            RuntimeError: If no blocks were written
        """
        if self._writer is None:
            raise RuntimeError("No blocks written. Call write_block() at least once.")

        # Close the writer - this writes the Parquet footer and completes the file
        logger.info("Closing ParquetWriter and finalizing S3 upload...")
        self._writer.close()
        self._writer = None

        # Get file info from S3
        file_info = self.fs.get_file_info(self._s3_path)
        file_size = file_info.size
        file_size_mb = file_size / (1024 * 1024)

        # Calculate average row size
        avg_row_size = file_size / self._total_rows if self._total_rows > 0 else 0

        stats = {
            "file_path": f"s3://{self._s3_path}",
            "file_size_bytes": file_size,
            "file_size_mb": file_size_mb,
            "num_rows": self._total_rows,
            "num_columns": self._num_columns,
            "num_blocks": self._num_blocks,
            "compression": self._compression,
            "avg_row_size_bytes": avg_row_size,
        }

        logger.info(f"Successfully wrote Parquet file to S3: {file_size_mb:.2f} MB")
        logger.info(f"Total rows: {self._total_rows:,}")
        logger.info(f"Number of row groups (blocks): {self._num_blocks}")

        return stats

    def close(self) -> None:
        """Close the writer and release resources."""
        if self._writer is not None:
            logger.warning("Writer was not properly finished. Closing now...")
            self._writer.close()
            self._writer = None

        # Reset state
        self._s3_path = None
        self._compression = "snappy"
        self._num_blocks = 0
        self._total_rows = 0
        self._num_columns = None
        self._schema = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def _ensure_bucket_exists(self) -> None:
        """Ensure the S3 bucket exists, create if it doesn't."""
        try:
            # Check if bucket exists
            bucket_info = self.fs.get_file_info(self.bucket_name)
            if bucket_info.type == pafs.FileType.Directory:
                logger.debug(f"Bucket already exists: {self.bucket_name}")
            else:
                # Try to create bucket
                logger.info(f"Creating S3 bucket: {self.bucket_name}")
                self.fs.create_dir(self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
        except Exception as e:
            logger.warning(f"Could not verify/create bucket: {e}")
            # Continue anyway - might have permissions issues but can still write

    def read_metadata(self, parquet_path: str) -> dict:
        """Read metadata from a Parquet file on S3.

        Args:
            parquet_path: S3 path or key to the Parquet file

        Returns:
            Dictionary with metadata information
        """
        # Construct full path
        if parquet_path.startswith("s3://"):
            path_parts = parquet_path[5:].split("/", 1)
            bucket = path_parts[0]
            key = path_parts[1] if len(path_parts) > 1 else ""
            full_path = f"{bucket}/{key}"
        else:
            full_path = f"{self.bucket_name}/{parquet_path}"

        # Read metadata using filesystem
        parquet_file = pq.ParquetFile(full_path, filesystem=self.fs)
        metadata = parquet_file.metadata

        return {
            "num_rows": metadata.num_rows,
            "num_row_groups": metadata.num_row_groups,
            "num_columns": metadata.num_columns,
            "created_by": metadata.created_by,
            "format_version": metadata.format_version,
            "serialized_size": metadata.serialized_size,
        }
