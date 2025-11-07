"""Abstract interface for Parquet block writers."""

from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd


class ParquetBlockWriter(ABC):
    """Abstract base class for writing Parquet files in blocks."""

    @abstractmethod
    def start_writing(self, output_path: str, compression: str = "snappy") -> None:
        """Initialize the writer and prepare for block writing.

        Args:
            output_path: Path where the Parquet file will be written
            compression: Compression codec to use (snappy, gzip, zstd, etc.)
        """
        pass

    @abstractmethod
    def write_block(self, df: pd.DataFrame) -> None:
        """Write a DataFrame block to the Parquet file.

        This method will be called multiple times with different DataFrames,
        each representing a block of data to be written.

        Args:
            df: pandas DataFrame containing the data block to write
        """
        pass

    @abstractmethod
    def finish_writing(self) -> dict:
        """Finalize the Parquet file and return statistics.

        Returns:
            Dictionary with write statistics (file_size, num_rows, etc.)
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the writer and release resources."""
        pass
