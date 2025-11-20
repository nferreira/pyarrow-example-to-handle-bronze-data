"""
Example 05: Generator Expressions

Just like list comprehensions, but lazy.
Generator expressions create values on demand, saving memory.
"""

import sys


if __name__ == "__main__":
    print("Creating list comprehension (entire list in memory):")
    squares_list = [x**2 for x in range(1000000)]
    
    print("Creating generator expression (values created on demand):")
    squares_gen = (x**2 for x in range(1000000))
    
    # Memory comparison
    print("\nMemory usage comparison:")
    print(f"List comprehension: {sys.getsizeof(squares_list) / 1024 / 1024:.2f} MB")
    print(f"Generator expression: {sys.getsizeof(squares_gen)} bytes")
    
    print("\nBoth produce the same values:")
    print(f"First 5 from list: {squares_list[:5]}")
    print(f"First 5 from generator: {[next(squares_gen) for _ in range(5)]}")
    
    print("\n✅ Generator expressions use way less memory!")

