"""
Example 16: When NOT to Use Generators

Generators have limitations:
1. Sequential access only (no random access)
2. No length (len() doesn't work)
3. One-time use (exhausted after one pass)
"""


if __name__ == "__main__":
    print("Demonstrating generator limitations:\n")
    
    gen = (x for x in range(5))
    
    print("1. No random access:")
    print("   gen[0] would raise TypeError")
    print("   Generators are sequential only")
    
    print("\n2. No length:")
    try:
        length = len(gen)
    except TypeError as e:
        print(f"   len(gen) raises: {type(e).__name__}: {e}")
    
    print("\n3. One-time use (exhausted after one pass):")
    gen1 = (x for x in range(5))
    list1 = list(gen1)  # [0, 1, 2, 3, 4]
    list2 = list(gen1)  # [] - exhausted!
    
    print(f"   First iteration: {list1}")
    print(f"   Second iteration: {list2}")
    print("   Generator is exhausted!")
    
    print("\n✅ Solution: Use a list if you need multiple iterations,")
    print("   or recreate the generator each time.")

