"""
Example 20: itertools.chain - Combine Generators

The chain() function combines multiple generators into a single iterator.
This is useful for processing data from multiple sources.
"""

from itertools import chain


if __name__ == "__main__":
    print("Using chain() to combine generators:")
    
    gen1 = (x for x in range(3))
    gen2 = (x for x in range(3, 6))
    gen3 = (x for x in range(6, 9))
    
    # Combine generators
    combined = chain(gen1, gen2, gen3)
    
    print(f"Combined result: {list(combined)}")
    
    # Can also combine lists and generators
    print("\nCombining lists and generators:")
    list1 = [1, 2, 3]
    gen4 = (x * 2 for x in range(3))
    combined2 = chain(list1, gen4)
    print(f"Combined result: {list(combined2)}")
    
    print("\n✅ chain() efficiently combines multiple iterables!")
    print("   Useful for processing data from multiple sources.")

