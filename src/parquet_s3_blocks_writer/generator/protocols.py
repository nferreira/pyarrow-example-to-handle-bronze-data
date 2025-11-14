"""Protocol definitions for dependency inversion."""

from typing import Iterator, Protocol

import pandas as pd

from .models import PageResponse, WriteStatistics


class APIClient(Protocol):
    """Protocol for API clients."""

    def fetch_page(self, page: int) -> PageResponse:
        """Fetch a single page of data."""
        ...


class DataWriter(Protocol):
    """Protocol for data writers."""

    def write(self, dataframes: Iterator[pd.DataFrame]) -> WriteStatistics:
        """Write dataframes to output."""
        ...


class LoggerProtocol(Protocol):
    """Protocol for logging."""

    def info(self, message: str) -> None:
        """Log info message."""
        ...

    def debug(self, message: str) -> None:
        """Log debug message."""
        ...

    def warning(self, message: str) -> None:
        """Log warning message."""
        ...

    def error(self, message: str) -> None:
        """Log error message."""
        ...

