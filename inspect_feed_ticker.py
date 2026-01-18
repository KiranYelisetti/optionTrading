from dhanhq import DhanFeed
import inspect

try:
    print(inspect.getsource(DhanFeed.process_ticker))
except Exception as e:
    print(e)
