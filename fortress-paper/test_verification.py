import sys
import os

# Add local directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

try:
    print("Testing Imports...")
    # Import directly from the added path
    from config import CAPITAL, ZONES_FILE, DB_PATH, TRADE_LOG_FILE
    from core.virtual_broker import VirtualBroker
    from core.strategy import FortressStrategy
    from core.data_recorder import DataRecorder
    print("✅ Imports Successful.")

    print("Testing Initialization...")
    broker = VirtualBroker(log_file=TRADE_LOG_FILE)
    print("✅ VirtualBroker Initialized.")
    
    strategy = FortressStrategy(zones_file=ZONES_FILE)
    print("✅ FortressStrategy Initialized.")
    
    recorder = DataRecorder(db_path=DB_PATH)
    print("✅ DataRecorder Initialized.")
    
    print("🎉 All Systems Go!")

except Exception as e:
    print(f"❌ Test Failed: {e}")
    import traceback
    traceback.print_exc()

