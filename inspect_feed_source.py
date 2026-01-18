from dhanhq import DhanFeed
import inspect

try:
    print(inspect.getsource(DhanFeed.run_forever))
except Exception as e:
    print(e)
