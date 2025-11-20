"""
Example 09: Generator send() Method

The send() method allows you to send values INTO the generator.
This enables two-way communication with generators.

Note: You must prime the generator with next() before using send().
"""


def echo_generator():
    """Generator that echoes back values sent to it."""
    value = None
    while True:
        value = yield value
        if value is not None:
            value = f"Echo: {value}"


if __name__ == "__main__":
    print("Using send() to send values into the generator:")
    
    gen = echo_generator()
    
    # Prime the generator (required before using send())
    print("Priming generator with next()...")
    next(gen)
    
    print(f"\nsend('Hello') = {gen.send('Hello')}")  # Echo: Hello
    print(f"send('World') = {gen.send('World')}")  # Echo: World
    print(f"send('Python') = {gen.send('Python')}")  # Echo: Python
    
    print("\n✅ send() enables two-way communication with generators!")

