"""Data processing classes for transforming transactions."""

import logging
from datetime import datetime
from typing import Iterator, List

import pandas as pd

from .models import PageResponse, Transaction


class TransactionExtractor:
    """
    Extracts individual transactions from page responses.

    Single Responsibility: Flatten paginated structure into transaction stream.
    """

    def extract(self, page_responses: Iterator[PageResponse]) -> Iterator[Transaction]:
        """
        Extract transactions from page responses.

        Args:
            page_responses: Iterator of page responses

        Yields:
            Individual Transaction objects
        """
        for response in page_responses:
            for transaction in response.data:
                yield transaction


class TransactionBatcher:
    """
    Batches transactions into groups for efficient processing.

    Single Responsibility: Group transactions into batches.
    """

    def __init__(self, batch_size: int = 1000):
        """
        Initialize batcher.

        Args:
            batch_size: Number of transactions per batch
        """
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        self.batch_size = batch_size

    def batch(self, transactions: Iterator[Transaction]) -> Iterator[List[Transaction]]:
        """
        Batch transactions into groups.

        Args:
            transactions: Iterator of transactions

        Yields:
            Lists of transactions (batches)
        """
        batch: List[Transaction] = []
        for transaction in transactions:
            batch.append(transaction)
            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        # Yield remaining transactions
        if batch:
            yield batch


class DataFrameTransformer:
    """
    Transforms transaction batches into pandas DataFrames.

    Single Responsibility: Convert transaction data to DataFrames with proper types.
    """

    def transform(self, batches: Iterator[List[Transaction]]) -> Iterator[pd.DataFrame]:
        """
        Transform batches to DataFrames.

        Args:
            batches: Iterator of transaction batches

        Yields:
            DataFrames with optimized data types
        """
        logger = logging.getLogger(__name__)
        for batch_num, batch in enumerate(batches, 1):
            # Convert to dictionaries
            transaction_dicts = [t.to_dict() for t in batch]

            # Create DataFrame
            df = pd.DataFrame(transaction_dicts)

            # Optimize data types
            df["amount"] = df["amount"].astype("float32")
            df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601")

            # Add metadata
            df["batch_number"] = batch_num
            df["processed_at"] = datetime.now()

            logger.info(f"Created DataFrame batch {batch_num} with {len(df)} records")
            yield df

