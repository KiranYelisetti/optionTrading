import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'fortress-paper'))
from config import CLIENT_ID, ACCESS_TOKEN
from dhanhq import DhanFeed

# Hardcoded valid instrument for testing (e.g. NIFTY 50 or similar)
# 49229 is NIFTY Jan Future as per logs. assuming it's valid.
instruments = [(2, "49229")] 

print(f"Testing Feed with Client ID: {CLIENT_ID}")
print(f"Token Length: {len(ACCESS_TOKEN)}")

def on_connect():
    print("✅ Connected to Feed!")

def on_message(data):
    print(f"📩 Data Received: {data}")

try:
    feed = DhanFeed(CLIENT_ID, ACCESS_TOKEN, instruments=instruments, version='v2') 
    # Try v2? Inspection showed signature accepts version.
    
    # We can't pass callbacks in init if the library doesn't support it (as per inspection).
    # So we subclass just like main.py
    
    class TestFeed(DhanFeed):
        def process_ticker(self, data):
            print("Ticker Data Received")
            return super().process_ticker(data)

    print("Initializing TestFeed...")
    feed = TestFeed(CLIENT_ID, ACCESS_TOKEN, instruments=instruments)
    feed.run_forever()
    
except Exception as e:
    print(f"❌ Feed Error: {e}")
