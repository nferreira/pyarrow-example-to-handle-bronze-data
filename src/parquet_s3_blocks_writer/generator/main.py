"""Main entry point for the data pipeline."""

import argparse
import logging
from pathlib import Path

import pandas as pd

from .api_client import TransactionAPIClient
from .models import PipelineConfig
from .pipeline import DataPipeline, NonGeneratorPipeline, PipelineComparator

# Configure logging
logger = logging.getLogger(__name__)


def main(use_generator: bool = True):
    """
    Main entry point for the pipeline.

    Args:
        use_generator: If True, use generator-based pipeline (memory efficient).
                      If False, use non-generator pipeline (loads all data into memory).
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    config = PipelineConfig(
        output_file=Path("transactions.parquet"),
        page_size=100,
        batch_size=1000,
    )

    api_client = TransactionAPIClient(
        page_size=config.page_size, total_pages=config.total_pages
    )

    if use_generator:
        # Generator-based pipeline (memory efficient)
        logger.info("=" * 80)
        logger.info("🚀 GENERATOR-BASED PIPELINE (Memory Efficient)")
        logger.info("=" * 80)
        logger.info("Using streaming generators to process data in batches")
        logger.info("Memory consumption: Constant (processes one batch at a time)")
        logger.info("=" * 80)

        pipeline = DataPipeline(api_client, config)
        stats = pipeline.execute()

        logger.info(f"\n✅ Pipeline completed: {stats.total_rows} rows written")
        logger.info(f"   File size: {stats.file_size_bytes / 1024 / 1024:.2f} MB")
        logger.info(f"   Execution time: {stats.elapsed_time:.2f} seconds")

        # Read back the Parquet file
        logger.info("\n" + "=" * 80)
        logger.info("Reading Back the Parquet File")
        logger.info("=" * 80)

        df_result = pd.read_parquet(config.output_file)
        logger.info(f"Total transactions loaded: {len(df_result)}")
        logger.info(
            f"Memory usage: {df_result.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
        )
        logger.info(f"\nSample data:\n{df_result.head()}")
        logger.info(f"\nTransactions by status:\n{df_result['status'].value_counts()}")
        logger.info(
            f"\nTransactions by category:\n{df_result['category'].value_counts()}"
        )
    else:
        # Non-generator pipeline (loads all data into memory)
        logger.info("=" * 80)
        logger.info("⚠️  NON-GENERATOR PIPELINE (Memory Inefficient)")
        logger.info("=" * 80)
        logger.info("This approach loads ALL data into memory - NOT recommended for large datasets!")
        logger.info("=" * 80)

        config.enable_csv_verification = False
        non_gen_pipeline = NonGeneratorPipeline(api_client, config.output_file, logger=logger)
        stats = non_gen_pipeline.execute()

        logger.info(f"\n✅ Non-generator pipeline completed: {stats.total_rows} rows written")
        logger.info(f"   File size: {stats.file_size_bytes / 1024 / 1024:.2f} MB")
        logger.info(f"   Execution time: {stats.elapsed_time:.2f} seconds")
        logger.info(
            f"\n⚠️  Note: This approach loaded ALL data into memory first!"
        )


def compare_approaches():
    """Compare generator vs non-generator approaches."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    config = PipelineConfig(
        output_file=Path("transactions_gen.parquet"),
        page_size=50,
        batch_size=200,
        enable_csv_verification=False,
    )

    comparator = PipelineComparator()
    comparator.compare(config, Path("transactions_no_gen.parquet"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run data pipeline to convert API data to Parquet format"
    )
    parser.add_argument(
        "--non-generator",
        action="store_true",
        help="Use non-generator pipeline (loads all data into memory). Default: use generator-based pipeline.",
    )
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Compare generator vs non-generator approaches",
    )

    args = parser.parse_args()

    if args.compare:
        compare_approaches()
    else:
        # Default to generator-based pipeline unless --non-generator flag is set
        use_generator = not args.non_generator
        main(use_generator=use_generator)
