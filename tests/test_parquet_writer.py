"""Tests for parquet_writer module."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from parquet_s3_blocks_writer.parquet_writer import PyArrowParquetBlockWriter


def test_pyarrow_writer_initialization():
    """Test that PyArrowParquetBlockWriter can be initialized."""
    writer = PyArrowParquetBlockWriter(block_size_mb=1)
    assert writer is not None
    assert writer.block_size_mb == 1


def test_write_single_block():
    """Test writing a single DataFrame block."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test.parquet"

        # Create test data
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Write data
        writer = PyArrowParquetBlockWriter(block_size_mb=1)
        writer.start_writing(str(output_path))
        writer.write_block(df)
        stats = writer.finish_writing()
        writer.close()

        # Verify stats
        assert stats["num_rows"] == 3
        assert stats["num_columns"] == 3
        assert stats["num_blocks"] == 1
        assert stats["file_size_bytes"] > 0
        assert output_path.exists()


def test_write_multiple_blocks():
    """Test writing multiple DataFrame blocks."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test.parquet"

        # Create test data blocks
        df1 = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        df2 = pd.DataFrame({"id": [3, 4], "name": ["Charlie", "David"]})
        df3 = pd.DataFrame({"id": [5, 6], "name": ["Eve", "Frank"]})

        # Write data
        writer = PyArrowParquetBlockWriter(block_size_mb=1)
        writer.start_writing(str(output_path))
        writer.write_block(df1)
        writer.write_block(df2)
        writer.write_block(df3)
        stats = writer.finish_writing()
        writer.close()

        # Verify stats
        assert stats["num_rows"] == 6
        assert stats["num_columns"] == 2
        assert stats["num_blocks"] == 3
        assert output_path.exists()


def test_context_manager():
    """Test using writer as context manager."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test.parquet"

        df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})

        with PyArrowParquetBlockWriter(block_size_mb=1) as writer:
            writer.start_writing(str(output_path))
            writer.write_block(df)
            stats = writer.finish_writing()

        assert stats["num_rows"] == 3
        assert output_path.exists()


def test_schema_validation():
    """Test that schema mismatch raises error."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test.parquet"

        df1 = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        df2 = pd.DataFrame({"id": [3, 4], "age": [25, 30]})  # Different schema

        writer = PyArrowParquetBlockWriter(block_size_mb=1)
        writer.start_writing(str(output_path))
        writer.write_block(df1)

        # This should raise ValueError due to schema mismatch
        with pytest.raises(ValueError, match="schema mismatch"):
            writer.write_block(df2)

        writer.close()


def test_write_without_start():
    """Test that writing without start_writing raises error."""
    df = pd.DataFrame({"id": [1, 2, 3]})

    writer = PyArrowParquetBlockWriter(block_size_mb=1)

    with pytest.raises(RuntimeError, match="not started"):
        writer.write_block(df)


def test_finish_without_blocks():
    """Test that finishing without blocks raises error."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test.parquet"

        writer = PyArrowParquetBlockWriter(block_size_mb=1)
        writer.start_writing(str(output_path))

        with pytest.raises(RuntimeError, match="No blocks written"):
            writer.finish_writing()

        writer.close()


def test_empty_dataframe_block():
    """Test that empty DataFrame blocks are skipped."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test.parquet"

        df1 = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        df_empty = pd.DataFrame({"id": [], "name": []})
        df2 = pd.DataFrame({"id": [3, 4], "name": ["Charlie", "David"]})

        writer = PyArrowParquetBlockWriter(block_size_mb=1)
        writer.start_writing(str(output_path))
        writer.write_block(df1)
        writer.write_block(df_empty)  # Should be skipped
        writer.write_block(df2)
        stats = writer.finish_writing()
        writer.close()

        # Empty block should be skipped
        assert stats["num_rows"] == 4
        assert stats["num_blocks"] == 2


def test_read_metadata():
    """Test reading metadata from written file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test.parquet"

        df = pd.DataFrame({"id": range(100), "value": [f"value_{i}" for i in range(100)]})

        writer = PyArrowParquetBlockWriter(block_size_mb=1)
        writer.start_writing(str(output_path))
        writer.write_block(df)
        writer.finish_writing()

        # Read metadata
        metadata = writer.read_metadata(str(output_path))

        assert metadata["num_rows"] == 100
        assert metadata["num_columns"] == 2
        assert metadata["num_row_groups"] >= 1

        writer.close()
