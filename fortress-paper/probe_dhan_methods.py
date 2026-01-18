from dhanhq import dhanhq
from config import CLIENT_ID, ACCESS_TOKEN
import inspect

try:
    dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
    print("Dhan Object Created.")
    
    methods = [m for m in dir(dhan) if not m.startswith('__')]
    print("Available Methods in 'dhan':")
    for m in methods:
        print(f" - {m}")
        
    # Check if there is anything like 'get_instruments' or 'search'
    
except Exception as e:
    print(f"Error: {e}")
