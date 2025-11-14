"""Writer classes for Parquet and CSV formats."""

import itertools
import logging
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .models import WriteStatistics
from .protocols import LoggerProtocol


class ParquetWriter:
    """
    Writes DataFrames to Parquet format.

    Single Responsibility: Handle Parquet file writing operations.
    Uses context manager pattern for resource management.
    """

    def __init__(
        self,
        output_path: Path,
        compression: str = "snappy",
        logger: Optional[LoggerProtocol] = None,
    ):
        """
        Initialize Parquet writer.

        Args:
            output_path: Path to output Parquet file
            compression: Compression codec
            logger: Logger instance
        """
        self.output_path = Path(output_path)
        self.compression = compression
        self._logger = logger or logging.getLogger(__name__)
        self._writer: Optional[pq.ParquetWriter] = None
        self._total_rows = 0

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager and close writer."""
        self.close()

    def write(self, dataframes: Iterator[pd.DataFrame]) -> WriteStatistics:
        """
        Write dataframes to Parquet file.

        Args:
            dataframes: Iterator of DataFrames to write

        Returns:
            WriteStatistics with operation details
        """
        import time

        start_time = time.time()
        batch_count = 0

        for df in dataframes:
            table = pa.Table.from_pandas(df)

            # Initialize writer on first batch
            if self._writer is None:
                self._writer = pq.ParquetWriter(
                    str(self.output_path),
                    table.schema,
                    compression=self.compression,
                )

            # Write batch
            self._writer.write_table(table)
            self._total_rows += len(df)
            batch_count += 1

            if self._logger:
                self._logger.info(
                    f"Written {len(df)} rows (total: {self._total_rows})"
                )

        elapsed_time = time.time() - start_time
        file_size = self.output_path.stat().st_size if self.output_path.exists() else 0

        if self._logger:
            self._logger.info(
                f"Successfully wrote {self._total_rows} total rows to {self.output_path}"
            )

        return WriteStatistics(
            total_rows=self._total_rows,
            total_batches=batch_count,
            file_size_bytes=file_size,
            elapsed_time=elapsed_time,
        )

    def close(self):
        """Close the Parquet writer."""
        if self._writer:
            self._writer.close()
            self._writer = None


class CSVWriter:
    """
    Writes DataFrames to CSV format.

    Single Responsibility: Handle CSV file writing operations.
    Uses context manager pattern for resource management.
    """

    def __init__(
        self,
        output_path: Path,
        logger: Optional[LoggerProtocol] = None,
    ):
        """
        Initialize CSV writer.

        Args:
            output_path: Path to output CSV file
            logger: Logger instance
        """
        self.output_path = Path(output_path)
        self._logger = logger or logging.getLogger(__name__)
        self._file_handle: Optional[object] = None
        self._header_written = False
        self._total_rows = 0

    def __enter__(self):
        """Enter context manager."""
        self._file_handle = open(
            self.output_path, "w", newline="", encoding="utf-8"
        )
        if self._logger:
            self._logger.info(f"Writing transactions to CSV: {self.output_path}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager and close file."""
        self.close()

    def write(self, dataframes: Iterator[pd.DataFrame]) -> WriteStatistics:
        """
        Write dataframes to CSV file.

        Args:
            dataframes: Iterator of DataFrames to write

        Returns:
            WriteStatistics with operation details
        """
        import time

        start_time = time.time()
        batch_count = 0

        for df in dataframes:
            if not self._file_handle:
                raise RuntimeError("CSVWriter must be used as context manager")

            # Write header on first batch
            if not self._header_written:
                df.to_csv(self._file_handle, index=False, mode="w", header=True)
                self._header_written = True
            else:
                df.to_csv(self._file_handle, index=False, mode="a", header=False)

            self._total_rows += len(df)
            batch_count += 1

        elapsed_time = time.time() - start_time
        file_size = self.output_path.stat().st_size if self.output_path.exists() else 0

        if self._logger:
            self._logger.info(
                f"Successfully wrote {self._total_rows} total rows to CSV: {self.output_path}"
            )

        return WriteStatistics(
            total_rows=self._total_rows,
            total_batches=batch_count,
            file_size_bytes=file_size,
            elapsed_time=elapsed_time,
        )

    def close(self):
        """Close the CSV file."""
        if self._file_handle:
            self._file_handle.close()
            self._file_handle = None


class DualWriter:
    """
    Writes to both Parquet and CSV simultaneously.

    Single Responsibility: Coordinate writing to multiple formats.
    """

    def __init__(
        self,
        parquet_writer: ParquetWriter,
        csv_writer: Optional[CSVWriter] = None,
    ):
        """
        Initialize dual writer.

        Args:
            parquet_writer: Parquet writer instance
            csv_writer: Optional CSV writer instance
        """
        self.parquet_writer = parquet_writer
        self.csv_writer = csv_writer
        self._logger = logging.getLogger(__name__)

    def write(self, dataframes: Iterator[pd.DataFrame]) -> WriteStatistics:
        """
        Write to both formats using streaming approach.

        For CSV verification, we need to write the same data twice.
        Since batches are small and CSV verification is optional,
        we use itertools.tee to create two independent iterators.

        Args:
            dataframes: Iterator of DataFrames

        Returns:
            WriteStatistics from Parquet write
        """
        if self.csv_writer:
            # Use tee to create two independent iterators
            # This buffers only the difference between the two iterators
            parquet_iter, csv_iter = itertools.tee(dataframes, 2)

            # Write to Parquet first
            parquet_stats = self.parquet_writer.write(parquet_iter)

            # Then write to CSV
            csv_stats = self.csv_writer.write(csv_iter)

            if csv_stats and self._logger:
                self._logger.info(
                    f"CSV write: {csv_stats.total_rows} rows in {csv_stats.elapsed_time:.2f}s"
                )
        else:
            # Only write to Parquet
            parquet_stats = self.parquet_writer.write(dataframes)

        return parquet_stats

