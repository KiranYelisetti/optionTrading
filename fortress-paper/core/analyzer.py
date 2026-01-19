import datetime
import json
import os
import sys
import pandas as pd
from dhanhq import dhanhq

# Add parent directory to path to allow importing config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import CLIENT_ID, ACCESS_TOKEN
from core.db import FortressDB
from core.analysis_utils import identify_smart_money_structure, resample_to_15m

class MarketAnalyzer:
    def __init__(self):
        self.dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
        self.db = FortressDB()
        
    def get_current_futures_symbol(self, base="NIFTY"):
        """
        Dynamically finds the current month's expiry symbol.
        """
        today = datetime.date.today()
        # Find last day of current month
        next_month = today.replace(day=28) + datetime.timedelta(days=4)
        last_day_month = next_month - datetime.timedelta(days=next_month.day)
        
        # Backtrack to Thursday
        offset = (last_day_month.weekday() - 3) % 7
        last_thursday = last_day_month - datetime.timedelta(days=offset)
        
        # Check if today is PAST the last Thursday of this month
        if today > last_thursday.date():
             # Move to next month
             next_month_date = today.replace(day=28) + datetime.timedelta(days=32) # Jump to next month
             last_day_next_month = next_month_date - datetime.timedelta(days=next_month_date.day)
             offset_next = (last_day_next_month.weekday() - 3) % 7
             last_thursday = last_day_next_month - datetime.timedelta(days=offset_next)
        
        fmt_date = last_thursday.strftime("%d %b %y").upper()
        return f"{base} {fmt_date} FUT"

    def _get_security_id(self, target_root):
        """
        Fetches Scrip Master, filters for Target Futures.
        """
        print(f"📥 Fetching Scrip Master for {target_root}...")
        try:
            res = self.dhan.fetch_security_list() 
            if isinstance(res, pd.DataFrame):
                df = res
            elif isinstance(res, dict) and 'data' in res: 
                 df = pd.DataFrame(res['data'])
            else:
                 df = res
            
            if not isinstance(df, pd.DataFrame) or df.empty:
                raise ValueError("Descrip Master Invalid")

            # Normalize Columns
            for col in df.columns:
                if df[col].dtype == object:
                    df[col] = df[col].str.upper().str.strip()

            symbol_col = next((c for c in df.columns if 'SYMBOL' in c), None)
            id_col = next((c for c in df.columns if 'SECURITY_ID' in c), None)
            inst_col = next((c for c in df.columns if 'INSTRUMENT_NAME' in c), None)
            
            if symbol_col and id_col:
                futures_mask = df[symbol_col].str.startswith(target_root)
                
                if inst_col:
                    futures_mask = futures_mask & (df[inst_col] == 'FUTIDX')
                else:
                    futures_mask = futures_mask & df[symbol_col].str.contains('FUT')
                
                futures = df[futures_mask].copy()
                
                expiry_col = next((c for c in df.columns if 'EXPIRY_DATE' in c), None)
                if not futures.empty and expiry_col:
                     futures[expiry_col] = pd.to_datetime(futures[expiry_col])
                     today = pd.Timestamp.now().normalize()
                     futures = futures[futures[expiry_col] >= today]
                     futures = futures.sort_values(by=expiry_col)
                
                if not futures.empty:
                     row = futures.iloc[0] # Near Month
                     sec_id = str(row[id_col])
                     sym = row[symbol_col]
                     print(f"✅ Found Future: {sym} (ID: {sec_id})")
                     return sec_id, sym

                print(f"⚠️ {target_root} Futures not found.")
                return None, None
            
        except Exception as e:
            print(f"⚠️ Error in fetch_security_list: {e}")
        return None, None

    def fetch_deep_history(self, security_id, symbol_name):
        """
        Fetches 60 days of 1m data using pagination/batches.
        """
        print(f"🔄 Fetching Deep History (60 Days) for {symbol_name}...")
        
        # Dhan API limit is usually per call. We'll loop 5 days at a time.
        # 60 days / 5 = 12 calls.
        
        all_dfs = []
        end_date = datetime.datetime.now()
        
        # 12 weeks back (approx 3 months cover)
        # Or just loop 60 days
        days_per_batch = 5
        total_days = 60
        
        current_end = end_date
        
        for i in range(0, total_days, days_per_batch):
            current_start = current_end - datetime.timedelta(days=days_per_batch)
            
            to_str = current_end.strftime('%Y-%m-%d')
            from_str = current_start.strftime('%Y-%m-%d')
            
            # Skip weekends logic not strictly needed if API returns empty for those days
            
            print(f"   Fetching batch: {from_str} to {to_str}")
            try:
                res = self.dhan.intraday_minute_data(
                    security_id=security_id,
                    exchange_segment=self.dhan.NSE_FNO,
                    instrument_type='FUTIDX', 
                    from_date=from_str,
                    to_date=to_str
                )
                
                if res.get('status') == 'success':
                    data = res.get('data')
                    if data:
                        df = pd.DataFrame(data)
                        all_dfs.append(df)
            except Exception as e:
                 print(f"   ⚠️ Batch failed: {e}")
            
            current_end = current_start
            
        if not all_dfs:
            return None
            
        full_df = pd.concat(all_dfs, ignore_index=True)
        # Deduplicate based on timestamp if overlaps
        if not full_df.empty:
             col = 'start_time' if 'start_time' in full_df.columns else 'timestamp'
             full_df = full_df.drop_duplicates(subset=[col])
             full_df = full_df.sort_values(by=col)
             print(f"✅ Total fetched: {len(full_df)} candles.")
             return full_df
             
        return None

    def run_analysis(self):
        print("🚀 Starting Fortress Sweep Analysis (Daily - 60D)...")
        targets = ["NIFTY", "BANKNIFTY"]
        all_zones = []
        
        for target in targets:
            sec_id, sym = self._get_security_id(target)
            if sec_id:
                # 1. Fetch Deep Data
                df_1m = self.fetch_deep_history(sec_id, sym)
                
                if df_1m is not None:
                    # 2. Resample
                    df_15m = resample_to_15m(df_1m)
                    print(f"📊 {sym}: Using {len(df_15m)} 15m candles.")
                    
                    # 3. Save Market Data (Persistence)
                    self.db.save_market_data(df_15m, sym, timeframe='15m')
                    
                    # 4. Analyze
                    zones = identify_smart_money_structure(df_15m, sym, sec_id)
                    all_zones.extend(zones)
                else:
                    print(f"❌ No data for {sym}")
            else:
                print(f"❌ Security ID not found for {target}")
                
        if all_zones:
             # 5. Save Zones to DB
             self.db.save_zones(all_zones)
        else:
             print("⚠️ No zones identified.")

if __name__ == "__main__":
    analyzer = MarketAnalyzer()
    analyzer.run_analysis()
