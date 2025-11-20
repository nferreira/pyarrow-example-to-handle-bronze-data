"""
Example 19: itertools.islice - Slice a Generator

The islice() function allows you to slice generators without converting to a list.
This is memory-efficient for large sequences.
"""

from itertools import islice


def infinite_counter():
    """Generator that counts infinitely."""
    n = 0
    while True:
        yield n
        n += 1


if __name__ == "__main__":
    print("Using islice() to slice a generator:")
    
    counter = infinite_counter()
    
    # Get items 10-15 (without creating a list of all items)
    chunk = list(islice(counter, 10, 15))
    print(f"Items 10-15: {chunk}")
    
    # Get first 5 items
    counter2 = infinite_counter()
    first_5 = list(islice(counter2, 5))
    print(f"First 5 items: {first_5}")
    
    # Get items 0-10 with step 2
    counter3 = infinite_counter()
    every_other = list(islice(counter3, 0, 10, 2))
    print(f"Items 0-10 step 2: {every_other}")
    
    print("\n✅ islice() allows slicing without converting to list!")
    print("   Perfect for infinite generators or large sequences.")

