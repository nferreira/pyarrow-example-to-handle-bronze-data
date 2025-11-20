"""
Example 21: Real-World Case Study - Paginated API to Parquet Pipeline

A complete implementation demonstrating:
- Fetching transaction data from a paginated API
- Processing it efficiently using generators
- Writing to Parquet format without loading everything into memory

This example shows the power of generator chains for processing large datasets.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Iterator, Dict, List
from datetime import datetime, timedelta
import random
import time
import os


# ============================================================================
# Step 1: Simulate the Paginated API
# ============================================================================

def get_transactions_api(page: int, page_size: int = 100) -> Dict:
    """
    Simulates a paginated API call that returns transaction data.
    In reality, this would be requests.get(f'{BASE_URL}/transactions?page={page}')
    """
    # Simulate API latency
    time.sleep(0.01)  # Reduced for faster demo
    
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
    
    # Fixed number of pages for demo (instead of random)
    total_pages = 5
    return {
        'data': transactions,
        'page': page,
        'page_size': page_size,
        'has_more': page < total_pages
    }


# ============================================================================
# Step 2: Generator to Fetch All Pages
# ============================================================================

def fetch_all_pages(page_size: int = 100) -> Iterator[Dict]:
    """
    Generator that yields API responses page by page.
    Automatically handles pagination until no more data.
    """
    page = 1
    while True:
        print(f"Fetching page {page}...")
        response = get_transactions_api(page, page_size)
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
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
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
    compression: str = 'snappy'
):
    """
    Writes DataFrames to a single Parquet file using PyArrow.
    Uses streaming approach - writes batches incrementally.
    """
    writer = None
    total_rows = 0
    
    try:
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
        
        print(f"\n✅ Successfully wrote {total_rows} total rows to {output_file}")
    
    finally:
        if writer:
            writer.close()


# ============================================================================
# Alternative: Write Multiple Parquet Files (Partitioned)
# ============================================================================

def write_to_parquet_partitioned(
    dataframes: Iterator[pd.DataFrame],
    output_dir: str,
    partition_by: str = 'category',
    compression: str = 'snappy'
):
    """
    Writes DataFrames to multiple Parquet files partitioned by a column.
    Useful for large datasets that need to be queried by specific fields.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    writers = {}  # Cache writers for each partition
    total_rows = 0
    
    try:
        for df in dataframes:
            # Group by partition column
            for partition_value, group_df in df.groupby(partition_by):
                partition_path = os.path.join(
                    output_dir,
                    f"{partition_by}={partition_value}",
                    "data.parquet"
                )
                
                # Create directory for partition
                os.makedirs(os.path.dirname(partition_path), exist_ok=True)
                
                # Convert to PyArrow Table
                table = pa.Table.from_pandas(group_df)
                
                # Get or create writer for this partition
                if partition_value not in writers:
                    writers[partition_value] = pq.ParquetWriter(
                        partition_path,
                        table.schema,
                        compression=compression
                    )
                
                writers[partition_value].write_table(table)
                total_rows += len(group_df)
                print(f"Written {len(group_df)} rows to partition {partition_by}={partition_value}")
        
        print(f"\n✅ Successfully wrote {total_rows} total rows across {len(writers)} partitions")
    
    finally:
        for writer in writers.values():
            writer.close()


# ============================================================================
# Step 7: Putting It All Together - The Complete Pipeline
# ============================================================================

def pipeline_api_to_parquet(
    output_file: str = 'transactions.parquet',
    page_size: int = 100,
    batch_size: int = 1000
):
    """
    Complete pipeline: API -> Generator Chain -> Parquet
    
    Pipeline stages:
    1. fetch_all_pages: Yields API responses page by page
    2. extract_transactions: Flattens to individual transactions
    3. batch_transactions: Groups transactions into batches
    4. create_dataframes: Converts batches to DataFrames
    5. write_to_parquet_streaming: Writes to Parquet incrementally
    """
    print("🚀 Starting API to Parquet pipeline...\n")
    start_time = time.time()
    
    # Build the generator pipeline
    api_responses = fetch_all_pages(page_size)
    transactions = extract_transactions(api_responses)
    batches = batch_transactions(transactions, batch_size)
    dataframes = create_dataframes(batches)
    
    # Execute the pipeline by consuming the final generator
    write_to_parquet_streaming(dataframes, output_file)
    
    elapsed = time.time() - start_time
    print(f"\n⏱️  Pipeline completed in {elapsed:.2f} seconds")


# ============================================================================
# Step 8: Performance Comparison - With vs Without Generators
# ============================================================================

def pipeline_without_generators(output_file: str = 'transactions_no_gen.parquet'):
    """
    NON-GENERATOR approach for comparison.
    Loads everything into memory first - BAD for large datasets!
    """
    print("⚠️  Non-generator approach (loads all data into memory)...\n")
    start_time = time.time()
    
    # Fetch all data at once
    all_transactions = []
    page = 1
    while True:
        response = get_transactions_api(page)
        all_transactions.extend(response['data'])
        if not response['has_more']:
            break
        page += 1
    
    # Convert to single DataFrame
    df = pd.DataFrame(all_transactions)
    df['amount'] = df['amount'].astype('float32')
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Write to Parquet
    df.to_parquet(output_file, compression='snappy', engine='pyarrow')
    
    elapsed = time.time() - start_time
    print(f"✅ Wrote {len(df)} rows to {output_file}")
    print(f"⏱️  Completed in {elapsed:.2f} seconds")
    print(f"💾 Peak memory usage: ~{df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")


# ============================================================================
# Main Execution
# ============================================================================

if __name__ == "__main__":
    import tempfile
    
    # Use temporary directory for output files
    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = os.path.join(tmpdir, 'transactions.parquet')
        output_dir = os.path.join(tmpdir, 'transactions_partitioned')
        
        print("=" * 80)
        print("Example 1: Basic Pipeline - API to Single Parquet File")
        print("=" * 80)
        pipeline_api_to_parquet(
            output_file=output_file,
            page_size=100,
            batch_size=200  # Smaller batch for demo
        )
        
        print("\n" + "=" * 80)
        print("Example 2: Partitioned Parquet Files by Category")
        print("=" * 80)
        api_responses = fetch_all_pages(page_size=100)
        transactions = extract_transactions(api_responses)
        batches = batch_transactions(transactions, batch_size=200)
        dataframes = create_dataframes(batches)
        write_to_parquet_partitioned(dataframes, output_dir, partition_by='category')
        
        print("\n" + "=" * 80)
        print("Example 3: Reading Back the Parquet File")
        print("=" * 80)
        df_result = pd.read_parquet(output_file)
        print(f"\nTotal transactions loaded: {len(df_result)}")
        print(f"Memory usage: {df_result.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        print(f"\nSample data:")
        print(df_result.head())
        print(f"\nTransactions by status:")
        print(df_result['status'].value_counts())
        print(f"\nTransactions by category:")
        print(df_result['category'].value_counts())
        
        print("\n" + "=" * 80)
        print("Example 4: Performance Comparison")
        print("=" * 80)
        output_file_no_gen = os.path.join(tmpdir, 'transactions_no_gen.parquet')
        pipeline_without_generators(output_file_no_gen)
        
        print("\n✅ All examples completed!")
        print("💡 Notice how generators enable memory-efficient processing!")

