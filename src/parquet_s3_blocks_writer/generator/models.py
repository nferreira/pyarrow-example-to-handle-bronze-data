"""Data models and configuration classes."""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List


class TransactionStatus(str, Enum):
    """Transaction status enumeration."""

    COMPLETED = "completed"
    PENDING = "pending"
    FAILED = "failed"


class Currency(str, Enum):
    """Currency enumeration."""

    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    JPY = "JPY"


class Category(str, Enum):
    """Transaction category enumeration."""

    FOOD = "food"
    TRANSPORT = "transport"
    ENTERTAINMENT = "entertainment"
    SHOPPING = "shopping"
    BILLS = "bills"


class PaymentMethod(str, Enum):
    """Payment method enumeration."""

    CREDIT_CARD = "credit_card"
    DEBIT_CARD = "debit_card"
    PAYPAL = "paypal"
    CRYPTO = "crypto"


@dataclass
class Transaction:
    """Transaction data model."""

    transaction_id: str
    user_id: str
    amount: float
    currency: str
    status: str
    category: str
    timestamp: str
    merchant: str
    payment_method: str

    def to_dict(self) -> Dict:
        """Convert transaction to dictionary."""
        return {
            "transaction_id": self.transaction_id,
            "user_id": self.user_id,
            "amount": self.amount,
            "currency": self.currency,
            "status": self.status,
            "category": self.category,
            "timestamp": self.timestamp,
            "merchant": self.merchant,
            "payment_method": self.payment_method,
        }


@dataclass
class PageResponse:
    """API page response data model."""

    data: List[Transaction]
    page: int
    page_size: int
    has_more: bool


@dataclass
class PipelineConfig:
    """Pipeline configuration."""

    page_size: int = 100
    batch_size: int = 1000
    total_pages: int = 300
    compression: str = "snappy"
    output_file: Path = field(default_factory=lambda: Path("transactions.parquet"))
    enable_csv_verification: bool = True

    @classmethod
    def default(cls) -> "PipelineConfig":
        """Create default configuration."""
        return cls()

    def __post_init__(self):
        """Validate configuration after initialization."""
        if self.page_size <= 0:
            raise ValueError("page_size must be positive")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.total_pages <= 0:
            raise ValueError("total_pages must be positive")


@dataclass
class WriteStatistics:
    """Statistics for write operations."""

    total_rows: int = 0
    total_batches: int = 0
    file_size_bytes: int = 0
    elapsed_time: float = 0.0

