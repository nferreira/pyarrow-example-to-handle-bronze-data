"""
Example 04: The Iterator Protocol

Generators implement the iterator protocol automatically.
This is why you can use them in for loops.
"""


def count_up_to(n):
    """Generator that counts from 1 to n."""
    count = 1
    while count <= n:
        yield count
        count += 1


if __name__ == "__main__":
    gen = count_up_to(3)
    
    print("Checking if generator is its own iterator:")
    print(f"iter(gen) is gen = {iter(gen) is gen}")  # True - it's its own iterator
    
    print("\nUsing generator in a for loop:")
    print("(for loops automatically call next() until StopIteration)")
    for num in count_up_to(5):
        print(f"  {num}")
    
    print("\n✅ Generators implement __iter__() and __next__() automatically!")

