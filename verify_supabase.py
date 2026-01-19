import os
import datetime
from supabase import create_client

# Credentials provided by user
URL = "https://zxrgjceyrygmyeorpqne.supabase.co"
KEY = "sb_publishable_bfukv8_i2H7jLy8-1A72Pg_WmHJ7xLz" 

def verify():
    print("🚀 Connecting to Supabase...")
    try:
        supabase = create_client(URL, KEY)
        print("✅ Client Created.")
        
        # 1. Test Insert to 'trading_zones'
        print("👉 Testing 'trading_zones' insert...")
        zone_data = {
            "id": "TEST_ZONE_VERIFY",
            "symbol": "TEST",
            "type": "SUPPLY",
            "timeframe": "1D",
            "range_high": 100.0,
            "range_low": 90.0,
            "status": "TEST",
            "note": "Verification Run",
            "updated_at": datetime.datetime.now().isoformat()
        }
        res = supabase.table('trading_zones').upsert(zone_data).execute()
        print(f"✅ Zones Saved: {res.data}")
        
        # 2. Test Insert to 'market_candles'
        print("👉 Testing 'market_candles' insert...")
        candle_data = {
            "symbol": "TEST",
            "timestamp": datetime.datetime.now().isoformat(),
            "timeframe": "1m",
            "open": 100.0,
            "high": 105.0,
            "low": 95.0,
            "close": 102.0,
            "volume": 1000.0
        }
        res = supabase.table('market_candles').upsert(candle_data).execute()
        print(f"✅ Candle Saved: {res.data}")
        
        # 3. Test Insert to 'trade_logs'
        print("👉 Testing 'trade_logs' insert...")
        log_data = {
            "symbol": "TEST",
            "action": "TEST_BUY",
            "price": 100.0,
            "timestamp": datetime.datetime.now().isoformat(),
            "details": "Verification Run"
        }
        res = supabase.table('trade_logs').insert(log_data).execute()
        print(f"✅ Log Saved: {res.data}")
        
        print("\n🎉 Supabase Verification SUCCESSFUL! cleanup starting...")
        
        # Cleanup
        supabase.table('trading_zones').delete().eq('id', 'TEST_ZONE_VERIFY').execute()
        supabase.table('market_candles').delete().eq('symbol', 'TEST').execute()
        # logs usually usually can keep or cleanup
        
        print("✅ Cleanup Done.")

    except Exception as e:
        print(f"❌ Verification Failed: {e}")

if __name__ == "__main__":
    verify()
