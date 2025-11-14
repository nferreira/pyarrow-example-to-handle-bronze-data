"""Utility classes for file operations and profiling."""

import logging
import os
import time
import tracemalloc
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from .protocols import LoggerProtocol


class FileComparator:
    """
    Compares two CSV files for data consistency.

    Single Responsibility: Compare files and report differences.
    """

    def __init__(self, logger: Optional[LoggerProtocol] = None):
        """
        Initialize file comparator.

        Args:
            logger: Logger instance
        """
        self._logger = logger or logging.getLogger(__name__)

    def compare(self, file1_path: Path, file2_path: Path) -> bool:
        """
        Compare two CSV files.

        Args:
            file1_path: Path to first CSV file
            file2_path: Path to second CSV file

        Returns:
            True if files match, False otherwise
        """
        if self._logger:
            self._logger.info(f"Comparing CSV files: {file1_path} vs {file2_path}")

        try:
            df1 = pd.read_csv(file1_path)
            df2 = pd.read_csv(file2_path)

            # Compare shape
            if df1.shape != df2.shape:
                if self._logger:
                    self._logger.error(
                        f"Files have different shapes: {df1.shape} vs {df2.shape}"
                    )
                return False

            # Compare columns
            if list(df1.columns) != list(df2.columns):
                if self._logger:
                    self._logger.error(
                        f"Files have different columns: {list(df1.columns)} vs {list(df2.columns)}"
                    )
                return False

            # Sort for consistent comparison
            df1_sorted = df1.sort_values(by=list(df1.columns)).reset_index(drop=True)
            df2_sorted = df2.sort_values(by=list(df2.columns)).reset_index(drop=True)

            # Compare data
            if not df1_sorted.equals(df2_sorted):
                if self._logger:
                    diff_mask = ~(df1_sorted == df2_sorted)
                    diff_count = diff_mask.sum().sum()
                    self._logger.error(f"Files contain different data: {diff_count} differences")
                return False

            if self._logger:
                self._logger.info(
                    f"Files match perfectly! {len(df1):,} rows × {len(df1.columns)} columns"
                )
            return True

        except Exception as e:
            if self._logger:
                self._logger.error(f"Error comparing files: {e}", exc_info=True)
            return False


class ParquetReader:
    """
    Reads Parquet files and converts to CSV.

    Single Responsibility: Handle Parquet file reading operations.
    """

    def __init__(self, logger: Optional[LoggerProtocol] = None):
        """
        Initialize Parquet reader.

        Args:
            logger: Logger instance
        """
        self._logger = logger or logging.getLogger(__name__)

    def read_to_csv(
        self, parquet_path: Path, csv_output_path: Path
    ) -> pd.DataFrame:
        """
        Read Parquet file and write to CSV.

        Args:
            parquet_path: Path to Parquet file
            csv_output_path: Path for CSV output

        Returns:
            DataFrame containing the data
        """
        if self._logger:
            self._logger.info(
                f"Reading Parquet file: {parquet_path} -> CSV: {csv_output_path}"
            )

        df_result = pd.read_parquet(parquet_path)
        df_result.to_csv(csv_output_path, index=False, encoding="utf-8")

        if self._logger:
            self._logger.info(
                f"Successfully decoded {len(df_result):,} rows from Parquet to CSV"
            )

        return df_result


class MemoryProfiler:
    """
    Profiles memory usage for operations.

    Single Responsibility: Track and report memory statistics.
    """

    def __init__(self, logger: Optional[LoggerProtocol] = None):
        """
        Initialize memory profiler.

        Args:
            logger: Logger instance
        """
        self._logger = logger or logging.getLogger(__name__)
        self._psutil_available = self._check_psutil()

    def _check_psutil(self) -> bool:
        """Check if psutil is available."""
        try:
            import psutil

            return True
        except ImportError:
            if self._logger:
                self._logger.warning(
                    "psutil not available. Install with 'pip install psutil' for detailed memory stats."
                )
            return False

    def profile(self, operation_name: str, operation_func):
        """
        Profile memory usage of an operation.

        Args:
            operation_name: Name of the operation
            operation_func: Function to execute

        Returns:
            Dictionary with memory statistics
        """
        import gc

        gc.collect()

        tracemalloc.start()
        snapshot_start = tracemalloc.take_snapshot()

        start_time = time.time()
        operation_func()
        elapsed_time = time.time() - start_time

        snapshot_end = tracemalloc.take_snapshot()
        current_mem, peak_mem = tracemalloc.get_traced_memory()

        tracemalloc.stop()

        stats = {
            "operation": operation_name,
            "elapsed_time": elapsed_time,
            "current_memory": current_mem,
            "peak_memory": peak_mem,
        }

        if self._psutil_available:
            import psutil

            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            stats.update(
                {
                    "rss": mem_info.rss,
                    "vms": mem_info.vms,
                    "memory_percent": process.memory_percent(),
                }
            )

        return stats

