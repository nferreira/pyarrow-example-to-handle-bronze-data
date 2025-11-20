"""
Example 02: Your First Generator

Generators produce values on-demand, one at a time.
They remember their state between calls.

Key concept: yield pauses the function and returns a value.
Next time you call it, execution resumes right after the yield.
"""


def count_up_to(n):
    """Generator that counts from 1 to n."""
    count = 1
    while count <= n:
        yield count  # The magic keyword!
        count += 1


if __name__ == "__main__":
    # Create a generator object
    counter = count_up_to(5)
    
    print("Using next() to get values one at a time:")
    print(f"next(counter) = {next(counter)}")  # 1
    print(f"next(counter) = {next(counter)}")  # 2
    print(f"next(counter) = {next(counter)}")  # 3
    
    print("\nUsing in a for loop:")
    for num in count_up_to(5):
        print(f"  {num}")
    
    print("\n✅ Generators produce values on-demand, saving memory!")

