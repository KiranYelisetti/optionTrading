from dhanhq import DhanFeed
import inspect

try:
    print(inspect.getsource(DhanFeed.process_data))
except Exception as e:
    print(e)
