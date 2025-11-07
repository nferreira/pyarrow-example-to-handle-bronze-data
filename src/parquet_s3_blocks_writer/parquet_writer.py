"""Concrete implementation of Parquet block writer using PyArrow."""

import logging
import os
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .parquet_writer_interface import ParquetBlockWriter as IParquetBlockWriter

logger = logging.getLogger(__name__)


class PyArrowParquetBlockWriter(IParquetBlockWriter):
    """Write Parquet files in blocks using PyArrow.

    This implementation accepts pandas DataFrames as blocks and writes them
    sequentially to a single Parquet file using PyArrow's ParquetWriter.
    """

    def __init__(self, block_size_mb: int = 1):
        """Initialize the PyArrow Parquet block writer.

        Args:
            block_size_mb: Target block size in megabytes (used for reference/logging)
        """
        self.block_size_mb = block_size_mb
        self.block_size_bytes = block_size_mb * 1024 * 1024
        self._writer: Optional[pq.ParquetWriter] = None
        self._output_path: Optional[str] = None
        self._compression: str = "snappy"
        self._num_blocks = 0
        self._total_rows = 0
        self._num_columns: Optional[int] = None
        self._schema: Optional[pa.Schema] = None

        logger.info(f"Initialized PyArrowParquetBlockWriter with {block_size_mb}MB block size")

    def start_writing(self, output_path: str, compression: str = "snappy") -> None:
        """Initialize the writer and prepare for block writing.

        Args:
            output_path: Path where the Parquet file will be written
            compression: Compression codec to use (snappy, gzip, zstd, etc.)
        """
        if self._writer is not None:
            raise RuntimeError("Writer already started. Call finish_writing() first.")

        self._output_path = output_path
        self._compression = compression
        self._num_blocks = 0
        self._total_rows = 0
        self._num_columns = None
        self._schema = None

        # Ensure output directory exists
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Starting Parquet file writing to {output_path}")
        logger.info(f"Compression: {compression}")

    def write_block(self, df: pd.DataFrame) -> None:
        """Write a DataFrame block to the Parquet file.

        This method will be called multiple times with different DataFrames,
        each representing a block of data to be written.

        Args:
            df: pandas DataFrame containing the data block to write

        Raises:
            RuntimeError: If start_writing() hasn't been called
            ValueError: If DataFrame schema doesn't match previous blocks
        """
        if self._output_path is None:
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
            self._writer = pq.ParquetWriter(
                self._output_path,
                self._schema,
                compression=self._compression,
                use_dictionary=True,
                version="2.6",
            )
            logger.info(f"Initialized ParquetWriter with schema: {self._schema}")

        # Validate schema consistency
        if not table.schema.equals(self._schema):
            raise ValueError(
                f"DataFrame schema mismatch. Expected {self._schema}, got {table.schema}"
            )

        # Write the block
        self._writer.write_table(table)
        self._num_blocks += 1
        self._total_rows += len(df)

        block_size_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        logger.info(
            f"Wrote block {self._num_blocks}: {len(df):,} rows "
            f"(~{block_size_mb:.2f} MB in memory)"
        )
        logger.debug(f"Total rows written so far: {self._total_rows:,}")

    def finish_writing(self) -> dict:
        """Finalize the Parquet file and return statistics.

        Returns:
            Dictionary with write statistics

        Raises:
            RuntimeError: If no blocks were written
        """
        if self._writer is None:
            raise RuntimeError("No blocks written. Call write_block() at least once.")

        # Close the writer
        self._writer.close()
        self._writer = None

        # Get file size
        file_size = os.path.getsize(self._output_path)
        file_size_mb = file_size / (1024 * 1024)

        # Calculate average row size
        avg_row_size = file_size / self._total_rows if self._total_rows > 0 else 0

        stats = {
            "file_path": self._output_path,
            "file_size_bytes": file_size,
            "file_size_mb": file_size_mb,
            "num_rows": self._total_rows,
            "num_columns": self._num_columns,
            "num_blocks": self._num_blocks,
            "compression": self._compression,
            "avg_row_size_bytes": avg_row_size,
        }

        logger.info(f"Finished writing Parquet file: {file_size_mb:.2f} MB")
        logger.info(f"Total rows: {self._total_rows:,}")
        logger.info(f"Number of blocks written: {self._num_blocks}")

        return stats

    def close(self) -> None:
        """Close the writer and release resources."""
        if self._writer is not None:
            logger.warning("Writer was not properly finished. Closing now...")
            self._writer.close()
            self._writer = None

        # Reset state
        self._output_path = None
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

    def read_metadata(self, parquet_path: str) -> dict:
        """Read metadata from a Parquet file.

        Args:
            parquet_path: Path to the Parquet file

        Returns:
            Dictionary with metadata information
        """
        parquet_file = pq.ParquetFile(parquet_path)
        metadata = parquet_file.metadata

        return {
            "num_rows": metadata.num_rows,
            "num_row_groups": metadata.num_row_groups,
            "num_columns": metadata.num_columns,
            "created_by": metadata.created_by,
            "format_version": metadata.format_version,
            "serialized_size": metadata.serialized_size,
        }
