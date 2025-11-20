"""
Example 15: Performance and Memory Efficiency

Comparing memory usage between list comprehensions and generators.
Both return the same result, but generators use way less memory.
"""

import sys


def sum_list(n):
    """Sum using list comprehension - creates entire list in memory."""
    return sum([x for x in range(n)])


def sum_gen(n):
    """Sum using generator expression - creates values on demand."""
    return sum(x for x in range(n))


if __name__ == "__main__":
    n = 1000000
    
    print("Comparing memory usage:")
    print(f"n = {n:,}")
    print()
    
    # List comprehension
    print("1. List comprehension approach:")
    list_result = sum_list(n)
    print(f"   Result: {list_result:,}")
    print(f"   Memory: Creates entire list in memory")
    
    # Generator expression
    print("\n2. Generator expression approach:")
    gen_result = sum_gen(n)
    print(f"   Result: {gen_result:,}")
    print(f"   Memory: Creates values on demand")
    
    print(f"\n✅ Both return same result: {list_result == gen_result}")
    print("   But generator uses way less memory!")

