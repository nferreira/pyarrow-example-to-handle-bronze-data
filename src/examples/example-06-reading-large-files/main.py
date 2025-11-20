"""
Example 06: Reading Large Files

Generators are perfect for processing large files line by line
without loading everything into memory.
"""

import os
import tempfile


def read_large_file(file_path):
    """Generator that reads a file line by line."""
    with open(file_path, 'r') as file:
        for line in file:
            yield line.strip()


if __name__ == "__main__":
    # Create a temporary file with sample data
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        test_file = f.name
        # Write some sample log lines
        f.write("INFO: Application started\n")
        f.write("ERROR: Connection failed\n")
        f.write("INFO: User logged in\n")
        f.write("ERROR: Invalid credentials\n")
        f.write("INFO: Data processed successfully\n")
    
    try:
        print("Processing file line by line (without loading all into memory):")
        print("\nLines containing 'ERROR':")
        for line in read_large_file(test_file):
            if 'ERROR' in line:
                print(f"  {line}")
        
        print("\n✅ Processed millions of lines without loading all into memory!")
    finally:
        # Clean up
        os.unlink(test_file)

