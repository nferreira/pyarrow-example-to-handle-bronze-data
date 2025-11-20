"""
Example 03: How Generators Actually Work

When you call a generator function, it returns a generator object
(an iterator) but doesn't execute the function body yet.

Execution only happens when you call next() or iterate over it.
"""


def simple_gen():
    """Simple generator demonstrating lazy evaluation."""
    print("Starting")
    yield 1
    print("Between yields")
    yield 2
    print("Ending")
    yield 3


if __name__ == "__main__":
    print("Creating generator (nothing printed yet!):")
    gen = simple_gen()  # Nothing printed yet!
    
    print("\nCalling next() - execution starts:")
    print(f"next(gen) = {next(gen)}")    # Prints: Starting, then returns 1
    
    print("\nCalling next() again:")
    print(f"next(gen) = {next(gen)}")    # Prints: Between yields, then returns 2
    
    print("\nCalling next() again:")
    print(f"next(gen) = {next(gen)}")    # Prints: Ending, then returns 3
    
    print("\nCalling next() after exhausted:")
    try:
        print(f"next(gen) = {next(gen)}")    # Raises StopIteration
    except StopIteration:
        print("  StopIteration raised - generator is exhausted!")
    
    print("\n✅ Generators are lazy - they only execute when consumed!")

