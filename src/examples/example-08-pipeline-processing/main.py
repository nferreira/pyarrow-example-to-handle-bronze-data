"""
Example 08: Pipeline Processing

Chain generators together to create powerful data processing pipelines.
Each stage processes and yields data without accumulating it.
"""

import os
import tempfile
import csv


def read_csv(filename):
    """Generator that reads CSV file line by line."""
    with open(filename) as f:
        for line in f:
            yield line.strip().split(',')


def filter_by_status(rows, status):
    """Generator that filters rows by status."""
    for row in rows:
        if row[2] == status:
            yield row


def extract_names(rows):
    """Generator that extracts names from rows."""
    for row in rows:
        yield row[0]


if __name__ == "__main__":
    # Create a temporary CSV file with sample data
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
        test_file = f.name
        writer = csv.writer(f)
        writer.writerow(['name', 'email', 'status'])
        writer.writerow(['Alice', 'alice@example.com', 'active'])
        writer.writerow(['Bob', 'bob@example.com', 'inactive'])
        writer.writerow(['Charlie', 'charlie@example.com', 'active'])
        writer.writerow(['Diana', 'diana@example.com', 'active'])
        writer.writerow(['Eve', 'eve@example.com', 'inactive'])
    
    try:
        print("Chaining generators together:")
        print("read_csv -> filter_by_status -> extract_names\n")
        
        # Chain generators together
        pipeline = extract_names(
            filter_by_status(
                read_csv(test_file),
                'active'
            )
        )
        
        print("Active user names:")
        for name in pipeline:
            print(f"  {name}")
        
        print("\n✅ Each stage processes data without accumulating it!")
    finally:
        # Clean up
        os.unlink(test_file)

