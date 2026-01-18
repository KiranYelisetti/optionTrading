from dhanhq import dhanhq
import inspect

print("Methods in dhanhq:")
methods = [m for m in dir(dhanhq) if not m.startswith('__')]
for m in methods:
    print(m)
