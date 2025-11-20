"""
Example 11: Generator close() Method

The close() method stops the generator and triggers the finally block.
This is useful for resource cleanup.
"""


def resource_handler():
    """Generator that demonstrates resource cleanup."""
    print("Acquiring resource")
    try:
        while True:
            yield "data"
    finally:
        print("Releasing resource")


if __name__ == "__main__":
    print("Using close() to stop the generator and trigger cleanup:")
    
    gen = resource_handler()
    
    print("\nGetting some data:")
    print(f"  next(gen) = {next(gen)}")
    print(f"  next(gen) = {next(gen)}")
    
    print("\nClosing generator (triggers finally block):")
    gen.close()  # Triggers finally block
    
    print("\nTrying to use closed generator:")
    try:
        next(gen)
    except StopIteration:
        print("  StopIteration raised - generator is closed!")
    
    print("\n✅ close() ensures proper resource cleanup!")

