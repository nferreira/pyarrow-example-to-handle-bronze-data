"""
Example 07: Infinite Sequences

Generators can produce infinite sequences because they generate
values on-demand, not all at once.
"""


def fibonacci():
    """Generator that produces Fibonacci numbers infinitely."""
    a, b = 0, 1
    while True:
        yield a
        a, b = b, a + b


if __name__ == "__main__":
    print("Getting first 10 Fibonacci numbers:")
    fib = fibonacci()
    first_10 = [next(fib) for _ in range(10)]
    print(f"  {first_10}")
    
    print("\nGetting next 5 Fibonacci numbers:")
    next_5 = [next(fib) for _ in range(5)]
    print(f"  {next_5}")
    
    print("\n✅ Infinite sequences are possible with generators!")
    print("   (Try not to convert to list - it will run forever!)")

