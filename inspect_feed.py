from dhanhq import DhanFeed
import inspect

print("DhanFeed Constructor:")
try:
    print(inspect.signature(DhanFeed.__init__))
except Exception as e:
    print(e)

print("\nDhanFeed Dict:")
print(DhanFeed.__dict__.keys())
