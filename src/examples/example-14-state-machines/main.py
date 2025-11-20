"""
Example 14: Generator-based State Machines

Generators can be used to implement state machines elegantly.
Each yield represents a state transition.
"""


def traffic_light():
    """Generator that cycles through traffic light states."""
    while True:
        yield "Green"
        yield "Yellow"
        yield "Red"


if __name__ == "__main__":
    print("Traffic light state machine:")
    
    light = traffic_light()
    
    print("\nFirst 7 states:")
    for i in range(7):
        state = next(light)
        print(f"  State {i+1}: {state}")
    
    print("\n✅ Generators make state machines simple and elegant!")
    print("   Output: Green, Yellow, Red, Green, Yellow, Red, Green")

