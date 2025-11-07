"""Upload files to S3 in chunks."""

import logging
import os
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from .config import S3Config

logger = logging.getLogger(__name__)


class S3Uploader:
    """Upload files to S3 with chunk support."""

    def __init__(self, config: S3Config, chunk_size_mb: int = 5):
        """Initialize S3 uploader.

        Args:
            config: S3 configuration
            chunk_size_mb: Size of each chunk for multipart upload (minimum 5MB for AWS)
        """
        self.config = config
        self.chunk_size_bytes = chunk_size_mb * 1024 * 1024

        # Create S3 client
        client_kwargs = {
            "region_name": config.region_name,
            "aws_access_key_id": config.aws_access_key_id,
            "aws_secret_access_key": config.aws_secret_access_key,
        }

        if config.endpoint_url:
            client_kwargs["endpoint_url"] = config.endpoint_url
            logger.info(f"Using custom S3 endpoint: {config.endpoint_url}")

        self.s3_client = boto3.client("s3", **client_kwargs)
        self.bucket_name = config.bucket_name

        logger.info(f"Initialized S3Uploader for bucket: {self.bucket_name}")
        logger.info(f"Chunk size: {chunk_size_mb}MB")

    def create_bucket_if_not_exists(self) -> bool:
        """Create S3 bucket if it doesn't exist.

        Returns:
            True if bucket was created, False if it already existed
        """
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket '{self.bucket_name}' already exists")
            return False
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                try:
                    # For LocalStack and us-east-1, don't specify LocationConstraint
                    if self.config.region_name == "us-east-1":
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={
                                "LocationConstraint": self.config.region_name
                            },
                        )
                    logger.info(f"Created bucket '{self.bucket_name}'")
                    return True
                except ClientError as create_error:
                    logger.error(f"Error creating bucket: {create_error}")
                    raise
            else:
                logger.error(f"Error checking bucket: {e}")
                raise

    def upload_file(
        self, file_path: str, s3_key: Optional[str] = None, use_multipart: bool = True
    ) -> dict:
        """Upload a file to S3.

        Args:
            file_path: Path to the file to upload
            s3_key: S3 object key (defaults to filename)
            use_multipart: Whether to use multipart upload for large files

        Returns:
            Dictionary with upload statistics
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        if s3_key is None:
            s3_key = Path(file_path).name

        file_size = os.path.getsize(file_path)
        file_size_mb = file_size / (1024 * 1024)

        logger.info(f"Uploading {file_path} to s3://{self.bucket_name}/{s3_key}")
        logger.info(f"File size: {file_size_mb:.2f} MB")

        # Ensure bucket exists
        self.create_bucket_if_not_exists()

        # Use multipart upload for files larger than chunk size
        if use_multipart and file_size > self.chunk_size_bytes:
            return self._multipart_upload(file_path, s3_key, file_size)
        else:
            return self._simple_upload(file_path, s3_key, file_size)

    def _simple_upload(self, file_path: str, s3_key: str, file_size: int) -> dict:
        """Perform a simple upload for small files.

        Args:
            file_path: Path to the file to upload
            s3_key: S3 object key
            file_size: Size of the file in bytes

        Returns:
            Dictionary with upload statistics
        """
        try:
            self.s3_client.upload_file(file_path, self.bucket_name, s3_key)
            logger.info(f"Successfully uploaded file using simple upload")

            return {
                "method": "simple",
                "file_path": file_path,
                "s3_key": s3_key,
                "bucket": self.bucket_name,
                "file_size_bytes": file_size,
                "file_size_mb": file_size / (1024 * 1024),
                "num_parts": 1,
            }
        except ClientError as e:
            logger.error(f"Error uploading file: {e}")
            raise

    def _multipart_upload(self, file_path: str, s3_key: str, file_size: int) -> dict:
        """Perform a multipart upload for large files.

        Args:
            file_path: Path to the file to upload
            s3_key: S3 object key
            file_size: Size of the file in bytes

        Returns:
            Dictionary with upload statistics
        """
        try:
            # Initialize multipart upload
            mpu = self.s3_client.create_multipart_upload(Bucket=self.bucket_name, Key=s3_key)
            upload_id = mpu["UploadId"]

            logger.info(f"Started multipart upload with ID: {upload_id}")

            parts = []
            part_number = 1

            with open(file_path, "rb") as f:
                while True:
                    data = f.read(self.chunk_size_bytes)
                    if not data:
                        break

                    logger.debug(
                        f"Uploading part {part_number} "
                        f"({len(data) / (1024 * 1024):.2f} MB)..."
                    )

                    part = self.s3_client.upload_part(
                        Bucket=self.bucket_name,
                        Key=s3_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=data,
                    )

                    parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                    logger.info(f"Uploaded part {part_number}/{(file_size + self.chunk_size_bytes - 1) // self.chunk_size_bytes}")

                    part_number += 1

            # Complete multipart upload
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

            logger.info(f"Successfully completed multipart upload with {len(parts)} parts")

            return {
                "method": "multipart",
                "file_path": file_path,
                "s3_key": s3_key,
                "bucket": self.bucket_name,
                "file_size_bytes": file_size,
                "file_size_mb": file_size / (1024 * 1024),
                "num_parts": len(parts),
                "upload_id": upload_id,
            }

        except ClientError as e:
            logger.error(f"Error in multipart upload: {e}")
            # Abort the multipart upload on error
            try:
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket_name, Key=s3_key, UploadId=upload_id
                )
                logger.info("Aborted multipart upload")
            except Exception as abort_error:
                logger.error(f"Error aborting multipart upload: {abort_error}")
            raise

    def list_objects(self, prefix: str = "") -> list:
        """List objects in the S3 bucket.

        Args:
            prefix: Prefix to filter objects

        Returns:
            List of object keys
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            if "Contents" in response:
                return [obj["Key"] for obj in response["Contents"]]
            return []
        except ClientError as e:
            logger.error(f"Error listing objects: {e}")
            raise
