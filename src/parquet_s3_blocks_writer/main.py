"""Main entry point for the Parquet S3 Blocks Writer application."""

import logging
import sys
import time
from pathlib import Path

from .config import get_app_config, get_s3_config
from .data_generator import DataGenerator
from .parquet_writer import PyArrowParquetBlockWriter
from .parquet_writer_interface import ParquetBlockWriter
from .s3_streaming_parquet_writer import S3StreamingParquetWriter
from .s3_uploader import S3Uploader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False):
    """Configure logging level.

    Args:
        verbose: Enable verbose logging
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)


def print_summary(
    generation_time: float,
    write_stats: dict,
    upload_stats: dict,
    write_time: float,
    upload_time: float,
):
    """Print summary statistics.

    Args:
        generation_time: Time taken to generate data
        write_stats: Statistics from Parquet writing
        upload_stats: Statistics from S3 upload
        write_time: Time taken to write Parquet file
        upload_time: Time taken to upload to S3
    """
    print("\n" + "=" * 80)
    print("EXECUTION SUMMARY")
    print("=" * 80)

    print("\nData Generation:")
    print(f"  Time taken: {generation_time:.2f} seconds")
    print(f"  Number of rows: {write_stats['num_rows']:,}")
    print(f"  Number of columns: {write_stats['num_columns']}")

    print("\nParquet File Writing:")
    print(f"  Time taken: {write_time:.2f} seconds")
    print(f"  File size: {write_stats['file_size_mb']:.2f} MB")
    print(f"  Number of blocks (row groups): {write_stats['num_blocks']}")
    print(f"  Compression: {write_stats['compression']}")
    print(f"  Average row size: {write_stats['avg_row_size_bytes']:.2f} bytes")
    print(f"  File path: {write_stats['file_path']}")

    print("\nS3 Upload:")
    print(f"  Time taken: {upload_time:.2f} seconds")
    print(f"  Upload method: {upload_stats['method']}")
    print(f"  Number of parts: {upload_stats['num_parts']}")
    print(f"  Bucket: {upload_stats['bucket']}")
    print(f"  S3 Key: {upload_stats['s3_key']}")
    print(
        f"  S3 URI: s3://{upload_stats['bucket']}/{upload_stats['s3_key']}"
    )

    print("\nTotal Execution:")
    total_time = generation_time + write_time + upload_time
    print(f"  Total time: {total_time:.2f} seconds")
    print(f"  Average throughput: {write_stats['file_size_mb'] / total_time:.2f} MB/s")

    print("\n" + "=" * 80)


def main():
    """Main execution function."""
    logger.info("Starting Parquet S3 Blocks Writer")
    logger.info("=" * 80)

    try:
        # Load configuration
        app_config = get_app_config()
        s3_config = get_s3_config()

        logger.info(f"Target data size: {app_config.target_data_size_mb} MB")
        logger.info(f"Block size: {app_config.block_size_mb} MB")
        logger.info(f"Number of records: {app_config.num_records:,}")
        logger.info(f"Writer type: {app_config.writer_type}")
        logger.info(f"S3 Bucket: {s3_config.bucket_name}")
        if s3_config.endpoint_url:
            logger.info(f"S3 Endpoint: {s3_config.endpoint_url}")

        # Step 1: Generate and write fake data in blocks
        logger.info("\n" + "-" * 80)
        logger.info("STEP 1 & 2: Generating and writing data in blocks")
        logger.info("-" * 80)

        # Determine output path based on writer type
        if app_config.writer_type == "local":
            output_dir = Path("./output")
            output_dir.mkdir(exist_ok=True)
            parquet_file = str(output_dir / "data.parquet")
        else:
            # For S3 writers, use S3 path directly
            parquet_file = "data.parquet"

        # Calculate records per block (approximate 1MB per block in memory)
        # Assuming ~1KB per record on average, adjust based on your data
        estimated_bytes_per_record = 1000
        records_per_block = (app_config.block_size_mb * 1024 * 1024) // estimated_bytes_per_record
        records_per_block = max(100, records_per_block)  # At least 100 records per block

        logger.info(f"Records per block: {records_per_block:,}")

        start_time_total = time.time()
        generation_time_total = 0
        write_time_total = 0

        # Initialize generator and writer based on type
        generator = DataGenerator()
        writer: ParquetBlockWriter

        if app_config.writer_type == "local":
            logger.info("Using local filesystem writer (PyArrowParquetBlockWriter)")
            writer = PyArrowParquetBlockWriter(block_size_mb=app_config.block_size_mb)
        elif app_config.writer_type == "s3_streaming":
            logger.info("Using S3 streaming writer (S3StreamingParquetWriter)")
            writer = S3StreamingParquetWriter(
                s3_config=s3_config, block_size_mb=app_config.block_size_mb, part_size_mb=5
            )
        else:
            raise ValueError(
                f"Unknown writer type: {app_config.writer_type}. "
                f"Valid options: local, s3_streaming"
            )

        # Start writing
        writer.start_writing(str(parquet_file), compression="snappy")

        try:
            # Generate and write data in blocks
            for df_block in generator.generate_dataframe_blocks(
                app_config.num_records, records_per_block
            ):
                gen_end = time.time()
                generation_time_total = gen_end - start_time_total

                # Write the block
                write_start = time.time()
                writer.write_block(df_block)
                write_end = time.time()
                write_time_total += write_end - write_start

            # Finish writing and get stats
            write_stats = writer.finish_writing()
        finally:
            writer.close()

        total_time = time.time() - start_time_total
        generation_time = generation_time_total
        write_time = write_time_total

        logger.info(f"Data generation and writing completed in {total_time:.2f} seconds")
        logger.info(f"  Generation time: {generation_time:.2f} seconds")
        logger.info(f"  Writing time: {write_time:.2f} seconds")

        # Step 3: Upload to S3 (only for local writer)
        upload_time = 0.0
        upload_stats = None

        if app_config.writer_type == "local":
            logger.info("\n" + "-" * 80)
            logger.info("STEP 3: Uploading to S3")
            logger.info("-" * 80)

            start_time = time.time()
            uploader = S3Uploader(s3_config)
            upload_stats = uploader.upload_file(
                parquet_file, s3_key="data.parquet", use_multipart=True
            )
            upload_time = time.time() - start_time

            logger.info(f"S3 upload completed in {upload_time:.2f} seconds")
        else:
            logger.info("\n" + "-" * 80)
            logger.info("STEP 3: Upload to S3 (skipped - data already on S3)")
            logger.info("-" * 80)
            # Create mock upload stats for summary
            upload_stats = {
                "method": "direct_s3_write",
                "num_parts": write_stats.get("num_parts", write_stats.get("num_blocks", 0)),
                "bucket": s3_config.bucket_name,
                "s3_key": parquet_file,
            }

        # Print summary
        print_summary(generation_time, write_stats, upload_stats, write_time, upload_time)

        logger.info("\nExecution completed successfully!")
        return 0

    except KeyboardInterrupt:
        logger.warning("\nExecution interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"\nError during execution: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
