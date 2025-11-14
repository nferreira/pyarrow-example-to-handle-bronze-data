"""Pipeline orchestrator classes."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterator, Optional

import pandas as pd

from .api_client import TransactionAPIClient
from .models import PipelineConfig, WriteStatistics
from .processors import DataFrameTransformer, TransactionBatcher, TransactionExtractor
from .protocols import LoggerProtocol
from .utils import FileComparator, MemoryProfiler, ParquetReader
from .writers import CSVWriter, DualWriter, ParquetWriter


class DataPipeline:
    """
    Orchestrates the complete data pipeline.

    Single Responsibility: Coordinate all pipeline components.
    Dependency Inversion: Depends on abstractions (APIClient, DataWriter protocols).
    """

    def __init__(
        self,
        api_client: TransactionAPIClient,
        config: PipelineConfig,
        logger: Optional[LoggerProtocol] = None,
    ):
        """
        Initialize pipeline.

        Args:
            api_client: API client instance
            config: Pipeline configuration
            logger: Logger instance
        """
        self.api_client = api_client
        self.config = config
        self._logger = logger or logging.getLogger(__name__)

        # Initialize components
        self.extractor = TransactionExtractor()
        self.batcher = TransactionBatcher(config.batch_size)
        self.transformer = DataFrameTransformer()

    def execute(self) -> WriteStatistics:
        """
        Execute the complete pipeline.

        Returns:
            WriteStatistics with operation results
        """
        import time

        if self._logger:
            self._logger.info("")
            self._logger.info("=" * 100)
            self._logger.info("=" * 100)
            self._logger.info("")
            self._logger.info("  " + "✅" * 20)
            self._logger.info("")
            self._logger.info("  " + "🚀" * 10 + "  GENERATOR-BASED PIPELINE - MEMORY EFFICIENT  " + "🚀" * 10)
            self._logger.info("")
            self._logger.info("  " + "✅" * 20)
            self._logger.info("")
            self._logger.info("=" * 100)
            self._logger.info("=" * 100)
            self._logger.info("")
            self._logger.info(
                "  ✅  RECOMMENDED APPROACH: This generator-based pipeline is memory efficient! ✅\n"
                "\n"
                "  💚 BENEFITS OF THIS APPROACH:\n"
                "    1. Processes data in small batches (one batch at a time)\n"
                "    2. Constant memory usage regardless of dataset size\n"
                "    3. Only one batch exists in memory at any given time\n"
                "    4. Memory is freed immediately after each batch is processed\n"
                "    5. Can handle datasets of ANY size without running out of memory\n"
                "\n"
                "  📊 MEMORY CONSUMPTION PATTERN:\n"
                "     Step 1: Fetch page → process immediately (memory: ~1 page)\n"
                "     Step 2: Extract transactions → stream to batcher (memory: ~1 batch)\n"
                "     Step 3: Create DataFrame → write to Parquet → free memory (memory: ~1 batch)\n"
                "     Peak memory: Constant (~batch_size records, typically < 10MB)\n"
                "\n"
                "  🔄 HOW IT WORKS:\n"
                "     - Generators create a 'lazy' pipeline that processes data on-demand\n"
                "     - Each generator yields data one item/batch at a time\n"
                "     - Data flows through the pipeline like a stream\n"
                "     - Memory usage stays constant regardless of total dataset size\n"
                "\n"
                "  📈 SCALABILITY:\n"
                "     - Can process millions of records with the same memory footprint\n"
                "     - Memory usage: O(batch_size) instead of O(total_records)\n"
                "     - Perfect for large datasets that don't fit in memory\n"
                "\n"
                "  💡 KEY ADVANTAGE: Memory usage is independent of dataset size!"
            )
            self._logger.info("")
            self._logger.info("=" * 100)
            self._logger.info("=" * 100)
            self._logger.info("")
            self._logger.info("Starting API to Parquet pipeline...")
            self._logger.info(
                f"Target: {self.config.total_pages} pages × {self.config.page_size} records"
            )
            self._logger.info(f"Batch size: {self.config.batch_size} records per batch")
            self._logger.info("")

        start_time = time.time()

        # Build generator pipeline
        page_responses = self.api_client.fetch_all_pages()
        transactions = self.extractor.extract(page_responses)
        batches = self.batcher.batch(transactions)
        dataframes = self.transformer.transform(batches)

        # Write to Parquet (and optionally CSV)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = (
            Path(f"to_s3_{timestamp}.csv")
            if self.config.enable_csv_verification
            else None
        )

        with ParquetWriter(
            self.config.output_file, self.config.compression, self._logger
        ) as parquet_writer:
            csv_writer = None
            if csv_path:
                csv_writer = CSVWriter(csv_path, self._logger)
                csv_writer.__enter__()

            try:
                writer = DualWriter(parquet_writer, csv_writer)
                stats = writer.write(dataframes)
            finally:
                if csv_writer:
                    csv_writer.__exit__(None, None, None)

        elapsed = time.time() - start_time
        if self._logger:
            self._logger.info(f"Pipeline completed in {elapsed:.2f} seconds")

        # CSV verification if enabled
        if self.config.enable_csv_verification and csv_path:
            self._verify_output(csv_path, timestamp)

        return stats

    def _verify_output(self, original_csv_path: Path, timestamp: str):
        """Verify output by comparing original and decoded CSV files."""
        if self._logger:
            self._logger.info("=" * 80)
            self._logger.info("CSV VERIFICATION: Comparing Original vs Decoded Data")
            self._logger.info("=" * 80)

        from_csv_path = Path(f"from_s3_{timestamp}.csv")

        # Read Parquet and write to CSV
        reader = ParquetReader(self._logger)
        reader.read_to_csv(self.config.output_file, from_csv_path)

        # Compare files
        comparator = FileComparator(self._logger)
        files_match = comparator.compare(original_csv_path, from_csv_path)

        if files_match:
            self._prompt_delete_files(original_csv_path, from_csv_path)
        else:
            if self._logger:
                self._logger.warning(
                    f"Files do not match. Keeping both CSV files for inspection"
                )

    def _prompt_delete_files(self, file1_path: Path, file2_path: Path):
        """Prompt user to delete matching CSV files."""
        if self._logger:
            self._logger.info(
                f"Both CSV files match. Would you like to delete them?\n"
                f"  - {file1_path}\n"
                f"  - {file2_path}"
            )

        try:
            response = input("\nDelete files? (yes/no): ").strip().lower()
            if response in ["yes", "y"]:
                if file1_path.exists():
                    file1_path.unlink()
                    if self._logger:
                        self._logger.info(f"Deleted: {file1_path}")
                if file2_path.exists():
                    file2_path.unlink()
                    if self._logger:
                        self._logger.info(f"Deleted: {file2_path}")
            else:
                if self._logger:
                    self._logger.info(f"Files kept: {file1_path}, {file2_path}")
        except (KeyboardInterrupt, Exception) as e:
            if self._logger:
                self._logger.warning(f"Cancelled or error: {e}. Files kept.")


class NonGeneratorPipeline:
    """
    Non-generator approach for comparison.

    This loads all data into memory - NOT recommended for large datasets.
    """

    def __init__(
        self,
        api_client: TransactionAPIClient,
        output_file: Path,
        logger: Optional[LoggerProtocol] = None,
    ):
        """
        Initialize non-generator pipeline.

        Args:
            api_client: API client instance
            output_file: Output file path
            logger: Logger instance
        """
        self.api_client = api_client
        self.output_file = Path(output_file)
        self._logger = logger or logging.getLogger(__name__)

    def execute(self) -> WriteStatistics:
        """
        Execute pipeline without generators.

        Returns:
            WriteStatistics with operation results
        """
        import time

        if self._logger:
            self._logger.warning("")
            self._logger.warning("=" * 100)
            self._logger.warning("=" * 100)
            self._logger.warning("")
            self._logger.warning("  " + "⚠️" * 20)
            self._logger.warning("")
            self._logger.warning("  " + "🚨" * 10 + "  NON-GENERATOR APPROACH - MEMORY INEFFICIENT  " + "🚨" * 10)
            self._logger.warning("")
            self._logger.warning("  " + "⚠️" * 20)
            self._logger.warning("")
            self._logger.warning("=" * 100)
            self._logger.warning("=" * 100)
            self._logger.warning("")
            self._logger.error(
                "  ⛔  WARNING: This approach is NOT recommended for large datasets! ⛔\n"
                "\n"
                "  ❌ PROBLEMS WITH THIS APPROACH:\n"
                "    1. It loads ALL data into memory first (all_transactions list)\n"
                "    2. Then creates a DataFrame from ALL data (duplicates memory usage)\n"
                "    3. Both the list AND DataFrame exist simultaneously in memory\n"
                "    4. This can cause memory spikes 2-3x the dataset size\n"
                "    5. May fail with OutOfMemory errors on large datasets\n"
                "\n"
                "  📊 MEMORY CONSUMPTION PATTERN:\n"
                "     Step 1: Fetch all pages → accumulate in list (memory: ~100% of data)\n"
                "     Step 2: Create DataFrame → duplicate data (memory: ~200% of data)\n"
                "     Step 3: Write to Parquet → temporary spike during conversion\n"
                "     Peak memory: Can reach 2-3x the actual dataset size!\n"
                "\n"
                "  💡 RECOMMENDATION: Use DataPipeline (generator-based) instead for memory efficiency."
            )
            self._logger.warning("")
            self._logger.warning("=" * 100)
            self._logger.warning("=" * 100)
            self._logger.warning("")

        start_time = time.time()

        # Fetch all data at once
        if self._logger:
            self._logger.warning(
                "\n📥 STEP 1: Fetching ALL pages and accumulating in memory..."
            )
            self._logger.warning(
                "   ⚠️  This will load ALL transaction data into a Python list."
            )
            self._logger.warning(
                "   ⚠️  Memory consumption: ~100% of dataset size (uncompressed)"
            )

        all_transactions = []
        page_count = 0
        for response in self.api_client.fetch_all_pages():
            page_count += 1
            if self._logger:
                self._logger.info(
                    f"   Fetching page {response.page} → Adding {len(response.data)} transactions to memory..."
                )
            all_transactions.extend([t.to_dict() for t in response.data])

        if self._logger:
            total_transactions = len(all_transactions)
            estimated_memory_mb = (
                total_transactions * 500 / 1024 / 1024
            )  # Rough estimate: ~500 bytes per transaction dict
            self._logger.warning(
                f"\n✅ Step 1 Complete: Loaded {total_transactions:,} transactions into memory"
            )
            self._logger.warning(
                f"   📊 Estimated memory usage: ~{estimated_memory_mb:.2f} MB (list of dicts)"
            )

        # Convert to single DataFrame
        if self._logger:
            self._logger.warning(
                "\n📊 STEP 2: Converting ALL data to pandas DataFrame..."
            )
            self._logger.warning(
                "   ⚠️  This creates a SECOND copy of ALL data in memory!"
            )
            self._logger.warning(
                "   ⚠️  Memory consumption: Now ~200% of dataset size (list + DataFrame)"
            )
            self._logger.warning(
                "   ⚠️  Both 'all_transactions' list AND DataFrame exist simultaneously!"
            )

        df = pd.DataFrame(all_transactions)
        df["amount"] = df["amount"].astype("float32")
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601")

        if self._logger:
            df_memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
            self._logger.warning(
                f"\n✅ Step 2 Complete: Created DataFrame with {len(df):,} rows"
            )
            self._logger.warning(
                f"   📊 DataFrame memory usage: ~{df_memory_mb:.2f} MB"
            )
            self._logger.warning(
                f"   📊 Total memory (list + DataFrame): ~{estimated_memory_mb + df_memory_mb:.2f} MB"
            )
            self._logger.warning(
                "   ⚠️  Peak memory consumption reached! Both structures in memory."
            )

        # Write to Parquet
        if self._logger:
            self._logger.warning(
                "\n💾 STEP 3: Writing DataFrame to Parquet file..."
            )
            self._logger.warning(
                "   ⚠️  During conversion, PyArrow may create temporary structures"
            )
            self._logger.warning(
                "   ⚠️  Memory spike possible during Parquet encoding"
            )

        df.to_parquet(self.output_file, compression="snappy", engine="pyarrow")

        if self._logger:
            self._logger.warning(
                "\n✅ Step 3 Complete: Data written to Parquet file"
            )
            self._logger.warning(
                "   💡 Memory will be freed after this step completes"
            )
            self._logger.warning(
                "   ⚠️  But peak memory usage was 2-3x the dataset size!"
            )

        elapsed = time.time() - start_time
        file_size = (
            self.output_file.stat().st_size if self.output_file.exists() else 0
        )

        if self._logger:
            self._logger.info(
                f"Wrote {len(df)} rows to {self.output_file} in {elapsed:.2f} seconds"
            )

        return WriteStatistics(
            total_rows=len(df),
            total_batches=1,
            file_size_bytes=file_size,
            elapsed_time=elapsed,
        )


class PipelineComparator:
    """
    Compares generator vs non-generator approaches.

    Single Responsibility: Compare pipeline performance and memory usage.
    """

    def __init__(self, logger: Optional[LoggerProtocol] = None):
        """
        Initialize comparator.

        Args:
            logger: Logger instance
        """
        self._logger = logger or logging.getLogger(__name__)
        self.profiler = MemoryProfiler(logger)

    def compare(
        self,
        generator_config: PipelineConfig,
        non_generator_output: Path,
    ):
        """
        Compare both approaches.

        Args:
            generator_config: Configuration for generator pipeline
            non_generator_output: Output path for non-generator approach
        """
        if self._logger:
            self._logger.info("=" * 80)
            self._logger.info("COMPREHENSIVE MEMORY COMPARISON")
            self._logger.info("=" * 80)

        # Generator approach
        gen_api_client = TransactionAPIClient(
            page_size=generator_config.page_size,
            total_pages=generator_config.total_pages,
        )
        gen_pipeline = DataPipeline(gen_api_client, generator_config, self._logger)

        gen_stats = self.profiler.profile("Generator Approach", gen_pipeline.execute)

        # Non-generator approach
        non_gen_api_client = TransactionAPIClient(
            page_size=generator_config.page_size,
            total_pages=generator_config.total_pages,
        )
        non_gen_pipeline = NonGeneratorPipeline(
            non_gen_api_client, non_generator_output, self._logger
        )

        non_gen_stats = self.profiler.profile(
            "Non-Generator Approach", non_gen_pipeline.execute
        )

        # Print comparison
        self._print_comparison(gen_stats, non_gen_stats)

    def _print_comparison(self, gen_stats: Dict, non_gen_stats: Dict):
        """Print comparison results."""
        if self._logger:
            self._logger.info("=" * 80)
            self._logger.info("COMPARISON SUMMARY")
            self._logger.info("=" * 80)

            peak_diff = non_gen_stats["peak_memory"] - gen_stats["peak_memory"]
            peak_diff_percent = (
                (peak_diff / gen_stats["peak_memory"] * 100)
                if gen_stats["peak_memory"] > 0
                else 0
            )

            self._logger.info(
                f"Peak Memory Usage:\n"
                f"  Generator:     {self._format_bytes(gen_stats['peak_memory'])}\n"
                f"  Non-Generator: {self._format_bytes(non_gen_stats['peak_memory'])}\n"
                f"  Difference:    {self._format_bytes(peak_diff)} ({peak_diff_percent:+.1f}%)"
            )

            self._logger.info(
                f"Execution Time:\n"
                f"  Generator:     {gen_stats['elapsed_time']:.2f} seconds\n"
                f"  Non-Generator: {non_gen_stats['elapsed_time']:.2f} seconds"
            )

    @staticmethod
    def _format_bytes(bytes_value: int) -> str:
        """Format bytes to human-readable format."""
        for unit in ["B", "KB", "MB", "GB"]:
            if bytes_value < 1024.0:
                return f"{bytes_value:.2f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.2f} TB"

