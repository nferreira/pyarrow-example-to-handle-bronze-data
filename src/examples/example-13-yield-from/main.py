"""
Example 13: yield from - Delegating to Sub-generators

The yield from statement allows a generator to delegate to another generator.
It's a cleaner way to compose generators.
"""


def inner_gen():
    """Inner generator that yields values."""
    yield 1
    yield 2
    return "inner done"


def outer_gen():
    """Outer generator that delegates to inner generator."""
    result = yield from inner_gen()  # Delegates completely
    print(f"Inner returned: {result}")
    yield 3


if __name__ == "__main__":
    print("Using yield from to delegate to sub-generators:")
    print()
    
    for value in outer_gen():
        print(f"  {value}")
    
    print("\n✅ yield from provides clean generator composition!")
    print("   Output: 1, 2, Inner returned: inner done, 3")

