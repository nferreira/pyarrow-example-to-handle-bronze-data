"""Generate fake data using Faker library."""

import logging
from typing import Dict, Generator, List

import pandas as pd
import pyarrow as pa
from faker import Faker

logger = logging.getLogger(__name__)


class DataGenerator:
    """Generate fake data for testing."""

    def __init__(self, seed: int = 42):
        """Initialize the data generator.

        Args:
            seed: Random seed for reproducibility
        """
        self.faker = Faker()
        Faker.seed(seed)

    def generate_records(self, num_records: int) -> Dict[str, List]:
        """Generate fake records with multiple columns.

        Args:
            num_records: Number of records to generate

        Returns:
            Dictionary with column names as keys and lists of values
        """
        logger.info(f"Generating {num_records:,} fake records...")

        data = {
            "id": [],
            "name": [],
            "email": [],
            "address": [],
            "city": [],
            "state": [],
            "zip_code": [],
            "country": [],
            "phone_number": [],
            "date_of_birth": [],
            "job": [],
            "company": [],
            "ssn": [],
            "credit_card_number": [],
            "iban": [],
            "ipv4": [],
            "user_agent": [],
            "text": [],
        }

        for i in range(num_records):
            data["id"].append(i)
            data["name"].append(self.faker.name())
            data["email"].append(self.faker.email())
            data["address"].append(self.faker.street_address())
            data["city"].append(self.faker.city())
            data["state"].append(self.faker.state())
            data["zip_code"].append(self.faker.zipcode())
            data["country"].append(self.faker.country())
            data["phone_number"].append(self.faker.phone_number())
            data["date_of_birth"].append(self.faker.date_of_birth())
            data["job"].append(self.faker.job())
            data["company"].append(self.faker.company())
            data["ssn"].append(self.faker.ssn())
            data["credit_card_number"].append(self.faker.credit_card_number())
            data["iban"].append(self.faker.iban())
            data["ipv4"].append(self.faker.ipv4())
            data["user_agent"].append(self.faker.user_agent())
            data["text"].append(self.faker.text(max_nb_chars=200))

            if (i + 1) % 10000 == 0:
                logger.debug(f"Generated {i + 1:,} records...")

        logger.info(f"Successfully generated {num_records:,} records")
        return data

    def create_arrow_table(self, data: Dict[str, List]) -> pa.Table:
        """Convert generated data to PyArrow table.

        Args:
            data: Dictionary with column names and values

        Returns:
            PyArrow Table
        """
        logger.debug("Converting data to PyArrow table...")
        table = pa.table(data)
        logger.info(
            f"Created PyArrow table with {table.num_rows:,} rows and {table.num_columns} columns"
        )
        return table

    def generate_table(self, num_records: int) -> pa.Table:
        """Generate fake data and return as PyArrow table.

        Args:
            num_records: Number of records to generate

        Returns:
            PyArrow Table with generated data
        """
        data = self.generate_records(num_records)
        return self.create_arrow_table(data)

    def create_dataframe(self, data: Dict[str, List]) -> pd.DataFrame:
        """Convert generated data to pandas DataFrame.

        Args:
            data: Dictionary with column names and values

        Returns:
            pandas DataFrame
        """
        logger.debug("Converting data to pandas DataFrame...")
        df = pd.DataFrame(data)
        logger.info(
            f"Created DataFrame with {len(df):,} rows and {len(df.columns)} columns"
        )
        return df

    def generate_dataframe(self, num_records: int) -> pd.DataFrame:
        """Generate fake data and return as pandas DataFrame.

        Args:
            num_records: Number of records to generate

        Returns:
            pandas DataFrame with generated data
        """
        data = self.generate_records(num_records)
        return self.create_dataframe(data)

    def generate_dataframe_blocks(
        self, total_records: int, block_size: int
    ) -> Generator[pd.DataFrame, None, None]:
        """Generate fake data as a generator yielding DataFrame blocks.

        This is useful for generating large datasets that don't fit in memory,
        yielding one block at a time.

        Args:
            total_records: Total number of records to generate
            block_size: Number of records per block

        Yields:
            pandas DataFrame blocks
        """
        logger.info(
            f"Generating {total_records:,} records in blocks of {block_size:,}"
        )

        num_blocks = (total_records + block_size - 1) // block_size
        current_id = 0

        for block_num in range(num_blocks):
            records_in_block = min(block_size, total_records - current_id)
            logger.info(
                f"Generating block {block_num + 1}/{num_blocks} "
                f"({records_in_block:,} records)"
            )

            data = {
                "id": [],
                "name": [],
                "email": [],
                "address": [],
                "city": [],
                "state": [],
                "zip_code": [],
                "country": [],
                "phone_number": [],
                "date_of_birth": [],
                "job": [],
                "company": [],
                "ssn": [],
                "credit_card_number": [],
                "iban": [],
                "ipv4": [],
                "user_agent": [],
                "text": [],
            }

            for i in range(records_in_block):
                data["id"].append(current_id + i)
                data["name"].append(self.faker.name())
                data["email"].append(self.faker.email())
                data["address"].append(self.faker.street_address())
                data["city"].append(self.faker.city())
                data["state"].append(self.faker.state())
                data["zip_code"].append(self.faker.zipcode())
                data["country"].append(self.faker.country())
                data["phone_number"].append(self.faker.phone_number())
                data["date_of_birth"].append(self.faker.date_of_birth())
                data["job"].append(self.faker.job())
                data["company"].append(self.faker.company())
                data["ssn"].append(self.faker.ssn())
                data["credit_card_number"].append(self.faker.credit_card_number())
                data["iban"].append(self.faker.iban())
                data["ipv4"].append(self.faker.ipv4())
                data["user_agent"].append(self.faker.user_agent())
                data["text"].append(self.faker.text(max_nb_chars=200))

            current_id += records_in_block
            yield self.create_dataframe(data)
