"""
Example 10: Generator throw() Method

The throw() method allows you to inject exceptions into the generator.
This is useful for error handling and control flow.
"""


def resilient_processor():
    """Generator that can handle exceptions thrown into it."""
    while True:
        try:
            data = yield
            print(f"Processing: {data}")
        except ValueError:
            print("Caught ValueError, continuing...")
        except KeyError:
            print("Caught KeyError, continuing...")


if __name__ == "__main__":
    print("Using throw() to inject exceptions into the generator:")
    
    gen = resilient_processor()
    
    # Prime the generator
    next(gen)
    
    print("\nSending valid data:")
    gen.send("valid data")
    
    print("\nThrowing ValueError:")
    gen.throw(ValueError)  # Generator catches it
    
    print("\nSending more data:")
    gen.send("more data")
    
    print("\nThrowing KeyError:")
    gen.throw(KeyError)  # Generator catches it
    
    print("\nSending final data:")
    gen.send("final data")
    
    print("\n✅ throw() enables exception handling in generators!")

