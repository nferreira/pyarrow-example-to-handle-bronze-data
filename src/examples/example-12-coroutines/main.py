"""
Example 12: Coroutines (Pre-async/await)

Generators can be used as coroutines to maintain state between calls.
This pattern was used before async/await was introduced in Python 3.5.
"""


def averager():
    """Coroutine that maintains running average."""
    total = 0
    count = 0
    average = None
    while True:
        value = yield average
        total += value
        count += 1
        average = total / count


if __name__ == "__main__":
    print("Using generator as coroutine to maintain state:")
    
    avg = averager()
    
    # Prime it
    next(avg)
    
    print("\nSending values and getting running average:")
    print(f"  send(10) = {avg.send(10)}")  # 10.0
    print(f"  send(20) = {avg.send(20)}")  # 15.0
    print(f"  send(30) = {avg.send(30)}")  # 20.0
    print(f"  send(40) = {avg.send(40)}")  # 25.0
    print(f"  send(50) = {avg.send(50)}")  # 30.0
    
    print("\n✅ Generators can maintain state between calls (coroutines)!")

