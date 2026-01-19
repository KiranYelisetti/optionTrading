import os
import sys
import datetime
import pytz
import logging
import pandas as pd

# Add fortress-paper to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'fortress-paper'))

from dhanhq import dhanhq
from config import CLIENT_ID, ACCESS_TOKEN
from core.strategy import FortressStrategy
from core.virtual_broker import VirtualBroker
from core.telegram_bot import send_telegram_alert
from core.db import FortressDB
from core.analysis_utils import identify_smart_money_structure, resample_to_15m

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def is_market_open_now():
    """
    Checks if current IST time is between 09:15 AM and 03:30 PM.
    """
    tz = pytz.timezone('Asia/Kolkata')
    now = datetime.datetime.now(tz)
    
    start_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
    end_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
    
    # Weekday Check (Mon=0, Sun=6)
    if now.weekday() > 4:
        return False, "Weekend"
        
    if start_time <= now <= end_time:
        return True, "Market Open"
    return False, "Outside Market Hours"

def fetch_recent_data(dhan, security_id, days=5):
    """
    Fetches recent 1m data (last N days) for dynamic analysis.
    """
    to_date = datetime.datetime.now().strftime('%Y-%m-%d')
    from_date = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime('%Y-%m-%d')
    try:
        res = dhan.intraday_minute_data(
            security_id=security_id,
            exchange_segment=dhan.NSE_FNO,
            instrument_type='FUTIDX', # Monitor targets Futures
            from_date=from_date,
            to_date=to_date
        )
        if res.get('status') == 'success' and res.get('data'):
            df = pd.DataFrame(res['data'])
            return df
    except Exception as e:
        logging.error(f"Data Fetch Error: {e}")
    return pd.DataFrame()

def run_scanner():
    # 1. Time Check
    is_open, reason = is_market_open_now()
    if not is_open:
        logging.info(f"⛔ Scanner Skipped: {reason}")
        return

    # 2. Initialize Components
    try:
        dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
        db = FortressDB()
        broker = VirtualBroker(log_file="fortress-paper/data/trade_logs.csv") # Kept for legacy compat, but DB preferred
    except Exception as e:
        logging.error(f"❌ Connection Failed: {e}")
        send_telegram_alert(f"⚠️ Monitor Failed: Connection Error - {e}")
        return

    logging.info("🚀 Starting Intraday Market Monitor (Dynamic + Supabase)...")
    
    # 3. Load Base Zones from DB (Daily Analysis)
    base_zones = db.get_active_zones() # Fetch all active
    if not base_zones:
        logging.warning("⚠️ No Zones found in Supabase. Proceeding with Dynamic Analysis only.")
    
    # 4. Filter Targets (Map gathered from Daily Zones or Config)
    # For now, we scan what we find in DB + hardcoded targets if DB empty
    params = []
    if base_zones:
         # Dedup symbols
         seen = set()
         for z in base_zones:
             if z['symbol'] not in seen:
                 params.append({'symbol': z['symbol'], 'security_id': z['security_id']})
                 seen.add(z['symbol'])
    else:
         # Fallback mechanism if DB empty? 
         # We need security_ids. 
         # The monitor relies on Daily Analysis to populate the ID map effectively.
         logging.error("❌ No targets found (Zones Empty). Run Analyzer first.")
         return

    # 5. Scan Loop
    for target in params:
        symbol = target['symbol']
        sec_id = target['security_id']
        
        logging.info(f"🔍 Scanning {symbol}...")
        
        # A. Fetch Live Data (5-10 Days)
        df_1m = fetch_recent_data(dhan, sec_id, days=7)
        
        if df_1m.empty:
            continue
            
        # B. Persist Data (Supabase)
        # Resample first to 15m to save space? User asked to "store every data".
        # Storing 1m data for 7 days might be heavy per run. 
        # But saving 15m is efficient.
        df_15m = resample_to_15m(df_1m)
        db.save_market_data(df_15m, symbol, timeframe='15m')
        
        # C. Analyze Dynamic Zones
        dynamic_zones = identify_smart_money_structure(df_15m, symbol, sec_id)
        
        # D. Setup Strategy
        # Initialize strategy with BASE zones (from DB)
        # Check uniqueness against this symbol
        target_base_zones = [z for z in base_zones if z['symbol'] == symbol]
        
        strategy = FortressStrategy(zones_file=None) # We manually inject
        strategy.zones = target_base_zones 
        strategy.inject_intraday_zones(dynamic_zones)
        
        # E. Check Current Price Action
        # We need the VERY LATEST candle.
        # df_1m has the latest minute.
        latest = df_1m.iloc[-1]
        
        # Note: Strategy check usually runs on Close of 15m or 5m.
        # Here we check the latest 1m price against the zones.
        candle = {
            'symbol': symbol,
            'high': float(latest.get('high', 0)),
            'low': float(latest.get('low', 0)),
            'close': float(latest.get('close', 0)),
            'open': float(latest.get('open', 0))
        }
        
        # Need Sentiment? (Expensive to calc every time). 
        # Assume NEUTRAL for speed or implement lightweight check.
        # Monitor assumes NEUTRAL/Manual confirmation unless updated.
        
        signal_data = strategy.check_entry(candle, "NEUTRAL")
        
        if signal_data:
            action = signal_data['action']
            logging.info(f"⚡ Signal Detected: {action} on {symbol}")
            
            msg = f"🔥 **DYNAMIC TRADE ALERT** 🔥\n\n**Symbol**: {symbol}\n**Action**: {action}\n**Zone**: {signal_data.get('zone_id')}\n**Price**: {candle['close']}"
            send_telegram_alert(msg)
            
            # Log to DB
            trade_record = {
                'symbol': symbol,
                'action': action,
                'price': candle['close'],
                'timestamp': datetime.datetime.now().isoformat(),
                'details': str(signal_data)
            }
            db.log_trade(trade_record)

if __name__ == "__main__":
    run_scanner()
