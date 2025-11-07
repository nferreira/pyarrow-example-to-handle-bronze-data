"""Configuration management for the application."""

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class S3Config:
    """S3 configuration parameters."""

    endpoint_url: Optional[str]
    region_name: str
    aws_access_key_id: str
    aws_secret_access_key: str
    bucket_name: str

    @classmethod
    def from_env(cls) -> "S3Config":
        """Load S3 configuration from environment variables."""
        return cls(
            endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:4566"),
            region_name=os.getenv("AWS_REGION", "us-east-1"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            bucket_name=os.getenv("S3_BUCKET_NAME", "parquet-data-bucket"),
        )


@dataclass
class AppConfig:
    """Application configuration parameters."""

    target_data_size_mb: int
    block_size_mb: int
    num_records: int
    writer_type: str

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Load application configuration from environment variables."""
        return cls(
            target_data_size_mb=int(os.getenv("TARGET_DATA_SIZE_MB", "10")),
            block_size_mb=int(os.getenv("BLOCK_SIZE_MB", "1")),
            num_records=int(os.getenv("NUM_RECORDS", "100000")),
            writer_type=os.getenv("WRITER_TYPE", "s3_streaming"),  # local, s3_streaming
        )


def get_s3_config() -> S3Config:
    """Get S3 configuration."""
    return S3Config.from_env()


def get_app_config() -> AppConfig:
    """Get application configuration."""
    return AppConfig.from_env()
