"""
Example 01: Memory-Intensive Approach (The Problem)

This example demonstrates the problem with traditional approaches:
loading everything into memory at once.

Before generators, if you wanted to process a large sequence,
you'd create the entire list in memory.

The Problem: This loads everything into memory at once.
For massive datasets, this is wasteful or impossible.
"""


def get_numbers(n):
    """Memory-intensive approach - creates entire list in memory."""
    result = []
    for i in range(n):
        result.append(i * 2)
    return result

def get_numbers_generator(n):
    """Memory-intensive approach - creates entire list in memory."""
    for i in range(n):
        yield i * 2


if __name__ == "__main__":
    import sys
    
    # Creates 1 million items in memory!
    numbers = get_numbers(1_000_000)
    
    print("Non-generator approach stats:")
    print(f"Memory usage: {sys.getsizeof(numbers) / 1024 / 1024:.2f} MB")
    print(f"Total count: {len(numbers)}")
    print(f"First 10 numbers: {numbers[:10]}")
    print("\n⚠️  Problem: All data is loaded into memory at once!")
    
    # Create generator for comparison
    numbers_generator = get_numbers_generator(1_000_000)
    
    print("\nGenerator approach stats:")
    print(f"Memory usage: {sys.getsizeof(numbers_generator)} bytes (much smaller!)")
    print("Note: Generators don't have len() - they produce values on-demand")
    
    # Get first 10 numbers from generator
    first_10_gen = [next(numbers_generator) for _ in range(10)]
    print(f"First 10 numbers: {first_10_gen}")
    
    print("\n✅ Generators produce values on-demand, saving memory!")
    print("   But you can't get the length without consuming the generator.")

    
    

