import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Iterator, Dict, List
from datetime import datetime, timedelta
import random
import time

# ============================================================================
# Configuration Constants
# ============================================================================

# Default number of pages to generate ~10MB of data
# Assuming ~1KB per record uncompressed, ~300-400 bytes compressed with Snappy
# For 10MB compressed: ~30,000 records needed = 300 pages (with page_size=100)
DEFAULT_TOTAL_PAGES = 300

# ============================================================================
# Step 1: Simulate the Paginated API
# ============================================================================

def get_transactions_api(page: int, page_size: int = 100, total_pages: int = None) -> Dict:
    """
    Simulates a paginated API call that returns transaction data.
    In reality, this would be requests.get(f'{BASE_URL}/transactions?page={page}')
    
    Args:
        page: Current page number
        page_size: Number of records per page
        total_pages: Total number of pages to generate (if None, uses default to ensure ~10MB)
    """
    # Simulate API latency
    time.sleep(0.1)
    
    # Use default total_pages if not specified
    if total_pages is None:
        total_pages = DEFAULT_TOTAL_PAGES
    
    # Generate random transaction data for this page
    transactions = []
    start_id = (page - 1) * page_size
    
    for i in range(page_size):
        transaction = {
            'transaction_id': f'TXN{start_id + i:08d}',
            'user_id': f'USER{random.randint(1000, 9999)}',
            'amount': round(random.uniform(10.0, 5000.0), 2),
            'currency': random.choice(['USD', 'EUR', 'GBP', 'JPY']),
            'status': random.choice(['completed', 'pending', 'failed']),
            'category': random.choice(['food', 'transport', 'entertainment', 'shopping', 'bills']),
            'timestamp': (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat(),
            'merchant': random.choice(['Amazon', 'Uber', 'Netflix', 'Starbucks', 'Apple']),
            'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'crypto'])
        }
        transactions.append(transaction)
    
    return {
        'data': transactions,
        'page': page,
        'page_size': page_size,
        'has_more': page < total_pages
    }


# ============================================================================
# Step 2: Generator to Fetch All Pages
# ============================================================================

def fetch_all_pages(page_size: int = 100, total_pages: int = None) -> Iterator[Dict]:
    """
    Generator that yields API responses page by page.
    Automatically handles pagination until no more data.
    
    Args:
        page_size: Number of records per page
        total_pages: Total number of pages to fetch (defaults to ~300 for ~10MB)
    """
    # Use default total_pages if not specified
    if total_pages is None:
        total_pages = DEFAULT_TOTAL_PAGES
    
    page = 1
    while True:
        print(f"Fetching page {page}...")
        response = get_transactions_api(page, page_size, total_pages)
        yield response
        
        if not response['has_more']:
            print(f"Reached last page: {page}")
            break
        
        page += 1


# ============================================================================
# Step 3: Generator to Extract Transaction Records
# ============================================================================

def extract_transactions(api_responses: Iterator[Dict]) -> Iterator[Dict]:
    """
    Generator that extracts individual transaction records from API responses.
    Flattens the paginated structure into a stream of transactions.
    """
    for response in api_responses:
        for transaction in response['data']:
            yield transaction


# ============================================================================
# Step 4: Generator to Batch Transactions
# ============================================================================

def batch_transactions(transactions: Iterator[Dict], batch_size: int = 1000) -> Iterator[List[Dict]]:
    """
    Generator that batches transactions into groups for efficient DataFrame creation.
    This is crucial for memory efficiency - we don't want to accumulate all records.
    """
    batch = []
    for transaction in transactions:
        batch.append(transaction)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    
    # Don't forget the last partial batch
    if batch:
        yield batch


# ============================================================================
# Step 5: Generator to Create DataFrames
# ============================================================================

def create_dataframes(batches: Iterator[List[Dict]]) -> Iterator[pd.DataFrame]:
    """
    Generator that converts batches of transactions into pandas DataFrames.
    Each DataFrame represents a batch that will be written to Parquet.
    """
    for batch_num, batch in enumerate(batches, 1):
        df = pd.DataFrame(batch)
        
        # Data type optimization
        df['amount'] = df['amount'].astype('float32')
        # Use format='ISO8601' to handle timestamps with or without microseconds
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')
        
        # Add metadata
        df['batch_number'] = batch_num
        df['processed_at'] = datetime.now()
        
        print(f"Created DataFrame batch {batch_num} with {len(df)} records")
        yield df


# ============================================================================
# Step 6: Write to Parquet in Bulk Using PyArrow
# ============================================================================

def write_to_parquet_streaming(
    dataframes: Iterator[pd.DataFrame],
    output_file: str,
    compression: str = 'snappy',
    csv_output_path: str = None
):
    """
    Writes DataFrames to a single Parquet file using PyArrow.
    Uses streaming approach - writes batches incrementally.
    Also writes all data to CSV file if csv_output_path is provided.
    
    Args:
        dataframes: Iterator of DataFrames to write
        output_file: Output Parquet file path
        compression: Compression codec for Parquet
        csv_output_path: Optional path to CSV file to write all transactions
    """
    writer = None
    total_rows = 0
    csv_file = None
    header_written = False
    
    try:
        # Open CSV file for writing if path provided
        if csv_output_path:
            csv_file = open(csv_output_path, 'w', newline='', encoding='utf-8')
            print(f"📝 Writing transactions to CSV: {csv_output_path}")
        
        for df in dataframes:
            # Convert pandas DataFrame to PyArrow Table
            table = pa.Table.from_pandas(df)
            
            # Initialize writer on first batch (infers schema from first table)
            if writer is None:
                writer = pq.ParquetWriter(
                    output_file,
                    table.schema,
                    compression=compression
                )
            
            # Write this batch to the Parquet file
            writer.write_table(table)
            total_rows += len(df)
            print(f"Written {len(df)} rows (total: {total_rows})")
            
            # Write to CSV file if provided
            if csv_file is not None:
                # Write header on first batch
                if not header_written:
                    df.to_csv(csv_file, index=False, mode='w', header=True)
                    header_written = True
                else:
                    # Append data without header
                    df.to_csv(csv_file, index=False, mode='a', header=False)
        
        print(f"\n✅ Successfully wrote {total_rows} total rows to {output_file}")
        if csv_file:
            print(f"✅ Successfully wrote {total_rows} total rows to CSV: {csv_output_path}")
    
    finally:
        if writer:
            writer.close()
        if csv_file:
            csv_file.close()


# ============================================================================
# Step 7: Putting It All Together - The Complete Pipeline
# ============================================================================

def read_parquet_to_csv(parquet_file: str, csv_output_path: str) -> pd.DataFrame:
    """
    Read Parquet file and write it to CSV file.
    
    Args:
        parquet_file: Path to the Parquet file
        csv_output_path: Path where CSV file should be written
        
    Returns:
        DataFrame containing the data
    """
    print(f"\n📥 Reading Parquet file: {parquet_file}")
    print(f"📝 Writing decoded data to CSV: {csv_output_path}")
    
    # Read Parquet file
    df_result = pd.read_parquet(parquet_file)
    
    # Write to CSV
    df_result.to_csv(csv_output_path, index=False, encoding='utf-8')
    
    print(f"✅ Successfully decoded {len(df_result):,} rows from Parquet to CSV")
    
    return df_result


def compare_csv_files(file1_path: str, file2_path: str) -> bool:
    """
    Compare two CSV files to check if they contain the same data.
    
    Args:
        file1_path: Path to first CSV file
        file2_path: Path to second CSV file
        
    Returns:
        True if files match, False otherwise
    """
    print(f"\n🔍 Comparing CSV files:")
    print(f"   File 1: {file1_path}")
    print(f"   File 2: {file2_path}")
    
    try:
        # Read both CSV files
        df1 = pd.read_csv(file1_path)
        df2 = pd.read_csv(file2_path)
        
        # Compare shape
        if df1.shape != df2.shape:
            print(f"\n❌ Files have different shapes:")
            print(f"   File 1: {df1.shape[0]} rows × {df1.shape[1]} columns")
            print(f"   File 2: {df2.shape[0]} rows × {df2.shape[1]} columns")
            return False
        
        # Compare columns
        if list(df1.columns) != list(df2.columns):
            print(f"\n❌ Files have different columns:")
            print(f"   File 1 columns: {list(df1.columns)}")
            print(f"   File 2 columns: {list(df2.columns)}")
            return False
        
        # Sort both dataframes by all columns to ensure consistent comparison
        # (since Parquet might have different row order)
        df1_sorted = df1.sort_values(by=list(df1.columns)).reset_index(drop=True)
        df2_sorted = df2.sort_values(by=list(df2.columns)).reset_index(drop=True)
        
        # Compare data
        if not df1_sorted.equals(df2_sorted):
            print(f"\n❌ Files contain different data")
            
            # Show differences
            diff_mask = ~(df1_sorted == df2_sorted)
            if diff_mask.any().any():
                print(f"\n   Differences found in {diff_mask.sum().sum()} cells")
                # Show first few differences
                diff_rows = diff_mask.any(axis=1)
                if diff_rows.any():
                    print(f"\n   First few rows with differences:")
                    print(df1_sorted[diff_rows].head())
                    print("\n   vs")
                    print(df2_sorted[diff_rows].head())
            return False
        
        print(f"\n✅ Files match perfectly!")
        print(f"   Both files contain {len(df1):,} rows and {len(df1.columns)} columns")
        return True
        
    except Exception as e:
        print(f"\n❌ Error comparing files: {e}")
        import traceback
        traceback.print_exc()
        return False


def prompt_delete_files(file1_path: str, file2_path: str) -> None:
    """
    Prompt user to delete files if they want to.
    
    Args:
        file1_path: Path to first file
        file2_path: Path to second file
    """
    print(f"\n🗑️  Both CSV files match. Would you like to delete them?")
    print(f"   Files to delete:")
    print(f"   - {file1_path}")
    print(f"   - {file2_path}")
    
    try:
        response = input("\n   Delete files? (yes/no): ").strip().lower()
        if response in ['yes', 'y']:
            import os
            if os.path.exists(file1_path):
                os.remove(file1_path)
                print(f"   ✅ Deleted: {file1_path}")
            if os.path.exists(file2_path):
                os.remove(file2_path)
                print(f"   ✅ Deleted: {file2_path}")
        else:
            print(f"   Files kept: {file1_path}, {file2_path}")
    except KeyboardInterrupt:
        print(f"\n   Cancelled. Files kept.")
    except Exception as e:
        print(f"\n   Error deleting files: {e}")
        print(f"   Files kept: {file1_path}, {file2_path}")


def pipeline_api_to_parquet(
    output_file: str = 'transactions.parquet',
    page_size: int = 100,
    batch_size: int = 1000,
    total_pages: int = None,
    enable_csv_verification: bool = True
):
    """
    Complete pipeline: API -> Generator Chain -> Parquet
    
    Pipeline stages:
    1. fetch_all_pages: Yields API responses page by page
    2. extract_transactions: Flattens to individual transactions
    3. batch_transactions: Groups transactions into batches
    4. create_dataframes: Converts batches to DataFrames
    5. write_to_parquet_streaming: Writes to Parquet incrementally
    6. CSV verification: Compare original data with decoded Parquet data
    
    Args:
        output_file: Output Parquet file path
        page_size: Number of records per API page
        batch_size: Number of records per DataFrame batch
        total_pages: Total number of pages to fetch (defaults to ~300 for ~10MB)
        enable_csv_verification: If True, write CSV files and compare them
    """
    # Use default total_pages if not specified
    if total_pages is None:
        total_pages = DEFAULT_TOTAL_PAGES
    
    print("🚀 Starting API to Parquet pipeline...")
    print(f"   Target: {total_pages} pages × {page_size} records = ~{total_pages * page_size:,} records (~10MB)")
    print()
    start_time = time.time()
    
    # Generate timestamp for CSV files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    to_csv_path = f"to_s3_{timestamp}.csv"
    from_csv_path = f"from_s3_{timestamp}.csv"
    
    # Build the generator pipeline
    api_responses = fetch_all_pages(page_size, total_pages)
    transactions = extract_transactions(api_responses)
    batches = batch_transactions(transactions, batch_size)
    dataframes = create_dataframes(batches)
    
    # Execute the pipeline by consuming the final generator
    # Write to Parquet and optionally to CSV
    csv_path = to_csv_path if enable_csv_verification else None
    write_to_parquet_streaming(dataframes, output_file, csv_output_path=csv_path)
    
    elapsed = time.time() - start_time
    print(f"\n⏱️  Pipeline completed in {elapsed:.2f} seconds")
    
    # If CSV verification is enabled, read from Parquet and compare
    if enable_csv_verification:
        print("\n" + "=" * 80)
        print("CSV VERIFICATION: Comparing Original vs Decoded Data")
        print("=" * 80)
        
        # Read Parquet and write to CSV
        read_parquet_to_csv(output_file, from_csv_path)
        
        # Compare the two CSV files
        files_match = compare_csv_files(to_csv_path, from_csv_path)
        
        # If files match, prompt user to delete
        if files_match:
            prompt_delete_files(to_csv_path, from_csv_path)
        else:
            print(f"\n⚠️  Files do not match. Keeping both CSV files for inspection:")
            print(f"   - {to_csv_path}")
            print(f"   - {from_csv_path}")


# ============================================================================
# Step 8: Advanced - Parallel Processing with Generators
# ============================================================================

from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import threading

def fetch_pages_parallel(max_workers: int = 5) -> Iterator[Dict]:
    """
    Fetches multiple pages in parallel using ThreadPoolExecutor.
    Still yields results in order using a queue.
    """
    # First, determine total pages
    first_response = get_transactions_api(1)
    yield first_response
    
    if not first_response['has_more']:
        return
    
    # Estimate total pages (in real scenario, API would tell us)
    total_pages = random.randint(5, 20)
    
    def fetch_page(page_num):
        return get_transactions_api(page_num)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all page requests
        futures = {
            executor.submit(fetch_page, page): page 
            for page in range(2, total_pages + 1)
        }
        
        # Yield results as they complete
        for future in futures:
            try:
                response = future.result()
                yield response
            except Exception as e:
                print(f"Error fetching page: {e}")


# ============================================================================
# Step 9: Usage Examples
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("Example 1: Basic Pipeline - API to Single Parquet File")
    print("=" * 80)
    pipeline_api_to_parquet(
        output_file='transactions.parquet',
        page_size=100,
        batch_size=1000
    )
    
    print("\n" + "=" * 80)
    print("Example 2: Reading Back the Parquet File")
    print("=" * 80)
    df_result = pd.read_parquet('transactions.parquet')
    print(f"\nTotal transactions loaded: {len(df_result)}")
    print(f"Memory usage: {df_result.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    print(f"\nSample data:")
    print(df_result.head())
    print(f"\nTransactions by status:")
    print(df_result['status'].value_counts())
    print(f"\nTransactions by category:")
    print(df_result['category'].value_counts())


# ============================================================================
# Step 10: Performance Comparison - With vs Without Generators
# ============================================================================

def pipeline_without_generators(output_file: str = 'transactions_no_gen.parquet', total_pages: int = None):
    """
    NON-GENERATOR approach for comparison.
    Loads everything into memory first - BAD for large datasets!
    
    Args:
        output_file: Output Parquet file path
        total_pages: Total number of pages to fetch (defaults to ~300 for ~10MB)
    """
    print("⚠️  Non-generator approach (loads all data into memory)...\n")
    start_time = time.time()
    
    # Use default total_pages if not specified
    if total_pages is None:
        total_pages = DEFAULT_TOTAL_PAGES
    
    # Fetch all data at once
    all_transactions = []
    page = 1
    page_size = 100
    while True:
        response = get_transactions_api(page, page_size, total_pages)
        all_transactions.extend(response['data'])
        if not response['has_more']:
            break
        page += 1
    
    # Convert to single DataFrame
    df = pd.DataFrame(all_transactions)
    df['amount'] = df['amount'].astype('float32')
    # Use format='ISO8601' to handle timestamps with or without microseconds
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')
    
    # Write to Parquet
    df.to_parquet(output_file, compression='snappy', engine='pyarrow')
    
    elapsed = time.time() - start_time
    print(f"✅ Wrote {len(df)} rows to {output_file}")
    print(f"⏱️  Completed in {elapsed:.2f} seconds")
    print(f"💾 Peak memory usage: ~{df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")


# ============================================================================
# Memory Efficiency Demonstration
# ============================================================================

def compare_approaches():
    """
    Compares memory usage between generator and non-generator approaches.
    Provides comprehensive memory allocation and usage statistics.
    """
    import tracemalloc
    import gc
    import os
    
    # Try to import psutil for advanced memory stats, fallback if not available
    try:
        import psutil
        PSUTIL_AVAILABLE = True
    except ImportError:
        PSUTIL_AVAILABLE = False
        print("⚠️  psutil not available. Install with 'pip install psutil' for detailed memory stats.")
    
    def format_bytes(bytes_value):
        """Format bytes to human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.2f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.2f} TB"
    
    def get_memory_stats():
        """Get current memory statistics."""
        if PSUTIL_AVAILABLE:
            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            return {
                'rss': mem_info.rss,  # Resident Set Size
                'vms': mem_info.vms,  # Virtual Memory Size
                'percent': process.memory_percent(),
            }
        else:
            # Fallback: return zeros if psutil not available
            return {
                'rss': 0,
                'vms': 0,
                'percent': 0.0,
            }
    
    def print_memory_snapshot(snapshot, label, top_n=10):
        """Print top memory allocations from a snapshot."""
        print(f"\n   📊 Top {top_n} Memory Allocations:")
        top_stats = snapshot.statistics('lineno')
        
        for index, stat in enumerate(top_stats[:top_n], 1):
            print(f"   {index:2d}. {stat.traceback.format()[-1].strip()}")
            print(f"       {format_bytes(stat.size)} ({stat.count} allocations)")
    
    print("\n" + "=" * 80)
    print("COMPREHENSIVE MEMORY COMPARISON")
    print("=" * 80)
    
    # Get initial system memory
    initial_mem = get_memory_stats()
    if PSUTIL_AVAILABLE:
        print(f"\n🖥️  Initial System Memory:")
        print(f"   RSS (Resident Set Size): {format_bytes(initial_mem['rss'])}")
        print(f"   VMS (Virtual Memory Size): {format_bytes(initial_mem['vms'])}")
        print(f"   Memory Percent: {initial_mem['percent']:.2f}%")
    
    # Generator approach
    print("\n" + "=" * 80)
    print("1️⃣  GENERATOR APPROACH (Memory Efficient)")
    print("=" * 80)
    
    # Force garbage collection before starting
    gc.collect()
    mem_before_gen = get_memory_stats()
    
    tracemalloc.start()
    snapshot_start = tracemalloc.take_snapshot()
    
    gen_start_time = time.time()
    pipeline_api_to_parquet('transactions_gen.parquet', page_size=50, batch_size=200)
    gen_elapsed = time.time() - gen_start_time
    
    snapshot_end = tracemalloc.take_snapshot()
    current_gen, peak_gen = tracemalloc.get_traced_memory()
    
    # Get memory after generation
    mem_after_gen = get_memory_stats()
    
    # Calculate differences
    mem_diff_gen = {
        'rss': mem_after_gen['rss'] - mem_before_gen['rss'],
        'vms': mem_after_gen['vms'] - mem_before_gen['vms'],
        'percent': mem_after_gen['percent'] - mem_before_gen['percent'],
    }
    
    print(f"\n⏱️  Execution Time: {gen_elapsed:.2f} seconds")
    print(f"\n💾 Memory Statistics:")
    print(f"   Current Memory (traced): {format_bytes(current_gen)}")
    print(f"   Peak Memory (traced): {format_bytes(peak_gen)}")
    if PSUTIL_AVAILABLE:
        print(f"   RSS Increase: {format_bytes(mem_diff_gen['rss'])} ({mem_diff_gen['rss'] / 1024**2:.2f} MB)")
        print(f"   VMS Increase: {format_bytes(mem_diff_gen['vms'])} ({mem_diff_gen['vms'] / 1024**2:.2f} MB)")
        print(f"   Memory Percent Increase: {mem_diff_gen['percent']:.2f}%")
        print(f"   Final RSS: {format_bytes(mem_after_gen['rss'])} ({mem_after_gen['rss'] / 1024**2:.2f} MB)")
        print(f"   Final Memory Percent: {mem_after_gen['percent']:.2f}%")
    
    # Show top allocations
    top_stats_gen = snapshot_end.compare_to(snapshot_start, 'lineno')
    print(f"\n   📊 Top 10 Memory Allocations:")
    for index, stat in enumerate(top_stats_gen[:10], 1):
        size_mb = stat.size_diff / 1024**2
        print(f"   {index:2d}. {format_bytes(stat.size_diff)} ({stat.count_diff} allocations)")
        print(f"       {stat.traceback.format()[-1].strip()}")
    
    tracemalloc.stop()
    
    # Force garbage collection between tests
    gc.collect()
    time.sleep(1)  # Give system time to release memory
    
    # Non-generator approach
    print("\n" + "=" * 80)
    print("2️⃣  NON-GENERATOR APPROACH (Loads All Data)")
    print("=" * 80)
    
    mem_before_non_gen = get_memory_stats()
    
    tracemalloc.start()
    snapshot_start = tracemalloc.take_snapshot()
    
    non_gen_start_time = time.time()
    pipeline_without_generators('transactions_no_gen.parquet')
    non_gen_elapsed = time.time() - non_gen_start_time
    
    snapshot_end = tracemalloc.take_snapshot()
    current_non_gen, peak_non_gen = tracemalloc.get_traced_memory()
    
    mem_after_non_gen = get_memory_stats()
    
    # Calculate differences
    mem_diff_non_gen = {
        'rss': mem_after_non_gen['rss'] - mem_before_non_gen['rss'],
        'vms': mem_after_non_gen['vms'] - mem_before_non_gen['vms'],
        'percent': mem_after_non_gen['percent'] - mem_before_non_gen['percent'],
    }
    
    print(f"\n⏱️  Execution Time: {non_gen_elapsed:.2f} seconds")
    print(f"\n💾 Memory Statistics:")
    print(f"   Current Memory (traced): {format_bytes(current_non_gen)}")
    print(f"   Peak Memory (traced): {format_bytes(peak_non_gen)}")
    if PSUTIL_AVAILABLE:
        print(f"   RSS Increase: {format_bytes(mem_diff_non_gen['rss'])} ({mem_diff_non_gen['rss'] / 1024**2:.2f} MB)")
        print(f"   VMS Increase: {format_bytes(mem_diff_non_gen['vms'])} ({mem_diff_non_gen['vms'] / 1024**2:.2f} MB)")
        print(f"   Memory Percent Increase: {mem_diff_non_gen['percent']:.2f}%")
        print(f"   Final RSS: {format_bytes(mem_after_non_gen['rss'])} ({mem_after_non_gen['rss'] / 1024**2:.2f} MB)")
        print(f"   Final Memory Percent: {mem_after_non_gen['percent']:.2f}%")
    
    # Show top allocations
    top_stats_non_gen = snapshot_end.compare_to(snapshot_start, 'lineno')
    print(f"\n   📊 Top 10 Memory Allocations:")
    for index, stat in enumerate(top_stats_non_gen[:10], 1):
        size_mb = stat.size_diff / 1024**2
        print(f"   {index:2d}. {format_bytes(stat.size_diff)} ({stat.count_diff} allocations)")
        print(f"       {stat.traceback.format()[-1].strip()}")
    
    tracemalloc.stop()
    
    # Comparison Summary
    print("\n" + "=" * 80)
    print("📈 COMPARISON SUMMARY")
    print("=" * 80)
    
    peak_diff = peak_non_gen - peak_gen
    peak_diff_percent = ((peak_non_gen - peak_gen) / peak_gen * 100) if peak_gen > 0 else 0
    
    rss_diff = mem_diff_non_gen['rss'] - mem_diff_gen['rss']
    rss_diff_percent = ((mem_diff_non_gen['rss'] - mem_diff_gen['rss']) / mem_diff_gen['rss'] * 100) if mem_diff_gen['rss'] > 0 else 0
    
    time_diff = non_gen_elapsed - gen_elapsed
    time_diff_percent = ((non_gen_elapsed - gen_elapsed) / gen_elapsed * 100) if gen_elapsed > 0 else 0
    
    print(f"\n💾 Peak Memory Usage:")
    print(f"   Generator:     {format_bytes(peak_gen)} ({peak_gen / 1024**2:.2f} MB)")
    print(f"   Non-Generator: {format_bytes(peak_non_gen)} ({peak_non_gen / 1024**2:.2f} MB)")
    print(f"   Difference:    {format_bytes(peak_diff)} ({peak_diff / 1024**2:.2f} MB, {peak_diff_percent:+.1f}%)")
    
    if PSUTIL_AVAILABLE:
        print(f"\n📊 RSS Memory Increase:")
        print(f"   Generator:     {format_bytes(mem_diff_gen['rss'])} ({mem_diff_gen['rss'] / 1024**2:.2f} MB)")
        print(f"   Non-Generator: {format_bytes(mem_diff_non_gen['rss'])} ({mem_diff_non_gen['rss'] / 1024**2:.2f} MB)")
        print(f"   Difference:    {format_bytes(rss_diff)} ({rss_diff / 1024**2:.2f} MB, {rss_diff_percent:+.1f}%)")
    
    print(f"\n⏱️  Execution Time:")
    print(f"   Generator:     {gen_elapsed:.2f} seconds")
    print(f"   Non-Generator: {non_gen_elapsed:.2f} seconds")
    print(f"   Difference:    {time_diff:+.2f} seconds ({time_diff_percent:+.1f}%)")
    
    efficiency_ratio = peak_gen / peak_non_gen if peak_non_gen > 0 else 0
    print(f"\n🎯 Memory Efficiency:")
    print(f"   Generator uses {efficiency_ratio:.1%} of non-generator peak memory")
    print(f"   Memory saved: {format_bytes(peak_diff)} ({peak_diff_percent:.1f}% reduction)")
    
    print(f"\n💡 Key Insights:")
    if peak_gen < peak_non_gen:
        print(f"   ✅ Generator approach uses {peak_diff / 1024**2:.2f} MB LESS peak memory")
        print(f"   ✅ This represents a {peak_diff_percent:.1f}% reduction in memory usage")
    else:
        print(f"   ⚠️  Generator approach uses {peak_diff / 1024**2:.2f} MB MORE peak memory")
    
    if PSUTIL_AVAILABLE and rss_diff > 0:
        print(f"   ✅ Generator approach increases RSS by {rss_diff / 1024**2:.2f} MB LESS")
    
    print(f"\n   📌 With millions of records, this difference becomes HUGE!")
    print(f"   📌 Generator approach scales better for large datasets")
    print(f"   📌 Non-generator approach may fail with insufficient memory")


# Run the comparison
if __name__ == "__main__":
    compare_approaches()