"""API client for fetching transaction data."""

import logging
import random
import time
from datetime import datetime, timedelta
from typing import Iterator, List, Optional

from .models import Category, Currency, PageResponse, PaymentMethod, Transaction, TransactionStatus
from .protocols import LoggerProtocol


class TransactionAPIClient:
    """
    API client for fetching transaction data.

    Single Responsibility: Handles all API communication logic.
    """

    def __init__(
        self,
        page_size: int = 100,
        total_pages: int = 300,
        latency_seconds: float = 0.1,
        logger: Optional[LoggerProtocol] = None,
    ):
        """
        Initialize API client.

        Args:
            page_size: Number of records per page
            total_pages: Total number of pages available
            latency_seconds: Simulated API latency
            logger: Logger instance (defaults to module logger)
        """
        self.page_size = page_size
        self.total_pages = total_pages
        self.latency_seconds = latency_seconds
        self._logger = logger or logging.getLogger(__name__)

    def fetch_page(self, page: int) -> PageResponse:
        """
        Fetch a single page of transactions.

        Args:
            page: Page number to fetch

        Returns:
            PageResponse containing transactions and metadata
        """
        if self._logger:
            self._logger.debug(f"Fetching page {page}...")

        # Simulate API latency
        time.sleep(self.latency_seconds)

        transactions = self._generate_transactions_for_page(page)
        has_more = page < self.total_pages

        return PageResponse(
            data=transactions,
            page=page,
            page_size=self.page_size,
            has_more=has_more,
        )

    def fetch_all_pages(self) -> Iterator[PageResponse]:
        """
        Generator that yields all pages sequentially.

        Yields:
            PageResponse for each page
        """
        page = 1
        while True:
            response = self.fetch_page(page)
            yield response

            if not response.has_more:
                if self._logger:
                    self._logger.info(f"Reached last page: {page}")
                break

            page += 1

    def _generate_transactions_for_page(self, page: int) -> List[Transaction]:
        """Generate transaction data for a specific page."""
        transactions = []
        start_id = (page - 1) * self.page_size

        for i in range(self.page_size):
            transaction = Transaction(
                transaction_id=f"TXN{start_id + i:08d}",
                user_id=f"USER{random.randint(1000, 9999)}",
                amount=round(random.uniform(10.0, 5000.0), 2),
                currency=random.choice(list(Currency)).value,
                status=random.choice(list(TransactionStatus)).value,
                category=random.choice(list(Category)).value,
                timestamp=(
                    datetime.now() - timedelta(days=random.randint(0, 365))
                ).isoformat(),
                merchant=random.choice(
                    ["Amazon", "Uber", "Netflix", "Starbucks", "Apple"]
                ),
                payment_method=random.choice(list(PaymentMethod)).value,
            )
            transactions.append(transaction)

        return transactions

