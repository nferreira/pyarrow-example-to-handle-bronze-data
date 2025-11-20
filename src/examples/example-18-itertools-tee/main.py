"""
Example 18: itertools.tee - Clone a Generator

The tee() function creates multiple independent iterators from a single generator.
This allows you to iterate over the same data multiple times.
"""

from itertools import tee


if __name__ == "__main__":
    print("Using tee() to clone a generator:")
    
    gen = (x**2 for x in range(5))
    
    # Create 2 independent iterators
    gen1, gen2 = tee(gen, 2)
    
    print(f"\nFirst iterator: {list(gen1)}")
    print(f"Second iterator: {list(gen2)}")
    
    print("\n✅ tee() creates independent iterators from the same generator!")
    print("   Note: The original generator is consumed, but tee() caches values.")

