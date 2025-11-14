"""Generator package for converting API data to Parquet format."""

from .api_client import TransactionAPIClient
from .models import (
    Category,
    Currency,
    PageResponse,
    PaymentMethod,
    PipelineConfig,
    Transaction,
    TransactionStatus,
    WriteStatistics,
)
from .pipeline import DataPipeline, NonGeneratorPipeline, PipelineComparator
from .processors import DataFrameTransformer, TransactionBatcher, TransactionExtractor
from .protocols import APIClient, DataWriter, LoggerProtocol
from .utils import FileComparator, MemoryProfiler, ParquetReader
from .writers import CSVWriter, DualWriter, ParquetWriter

__all__ = [
    # Models
    "Transaction",
    "PageResponse",
    "PipelineConfig",
    "WriteStatistics",
    "TransactionStatus",
    "Currency",
    "Category",
    "PaymentMethod",
    # Protocols
    "APIClient",
    "DataWriter",
    "LoggerProtocol",
    # API Client
    "TransactionAPIClient",
    # Processors
    "TransactionExtractor",
    "TransactionBatcher",
    "DataFrameTransformer",
    # Writers
    "ParquetWriter",
    "CSVWriter",
    "DualWriter",
    # Utils
    "FileComparator",
    "ParquetReader",
    "MemoryProfiler",
    # Pipeline
    "DataPipeline",
    "NonGeneratorPipeline",
    "PipelineComparator",
]

