from dhanhq import DhanFeed
import inspect

try:
    print(inspect.getsource(DhanFeed.connect))
except Exception as e:
    print(e)
