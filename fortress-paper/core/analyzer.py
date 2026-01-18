import datetime
import json
import os
import sys
import pandas as pd
from dhanhq import dhanhq

# Add parent directory to path to allow importing config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import CLIENT_ID, ACCESS_TOKEN, ZONES_FILE

class MarketAnalyzer:
    def __init__(self):
        self.dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
        
    def get_current_futures_symbol(self, base="NIFTY"):
        """
        Dynamically finds the current month's expiry symbol.
        Logic: Last Thursday of the current month.
        Format: NIFTY 30 JAN 25 FUT (Example)
        """
        today = datetime.date.today()
        # Find last day of current month
        next_month = today.replace(day=28) + datetime.timedelta(days=4)
        last_day_month = next_month - datetime.timedelta(days=next_month.day)
        
        # Backtrack to Thursday
        offset = (last_day_month.weekday() - 3) % 7
        last_thursday = last_day_month - datetime.timedelta(days=offset)
        
        # Format: DD MMM YY (e.g., 30 JAN 26)
        # Note: Dhan format might be 'NIFTY-JAN2026-FUT' in Scrip Master, 
        # but user requested specific format 'NIFTY 30 JAN 25 FUT'.
        # We will use this to PRINT/LOG, but we will use Scrip Master to find the ACTUAL TRADING SYMBOL for API.
        
        fmt_date = last_thursday.strftime("%d %b %y").upper()
        return f"{base} {fmt_date} FUT"

    def _get_security_id(self, target_root):
        """
        Fetches Scrip Master, filters for Target Futures (NIFTY/BANKNIFTY) matching Current Month.
        Returns: Security ID, Trading Symbol (API compliant), Formatting Symbol
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

            symbol_col = next((c for c in df.columns if 'SYMBOL' in c), None) # SEM_TRADING_SYMBOL
            id_col = next((c for c in df.columns if 'SECURITY_ID' in c), None)
            inst_col = next((c for c in df.columns if 'INSTRUMENT_NAME' in c), None)
            
            if symbol_col and id_col:
                # Filter pattern
                futures_mask = df[symbol_col].str.startswith(target_root)
                
                if inst_col:
                    futures_mask = futures_mask & (df[inst_col] == 'FUTIDX')
                else:
                    futures_mask = futures_mask & df[symbol_col].str.contains('FUT')
                
                futures = df[futures_mask]
                
                # Sort by Expiry
                expiry_col = next((c for c in df.columns if 'EXPIRY_DATE' in c), None)
                if not futures.empty and expiry_col:
                     # Convert to datetime for comparison
                     futures[expiry_col] = pd.to_datetime(futures[expiry_col])
                     today = pd.Timestamp.now().normalize()
                     
                     # Filter only future expiries
                     futures = futures[futures[expiry_col] >= today]
                     futures = futures.sort_values(by=expiry_col)
                
                if not futures.empty:
                     row = futures.iloc[0] # Near Month (valid)
                     sec_id = str(row[id_col])
                     sym = row[symbol_col]
                     print(f"✅ Found Future: {sym} (ID: {sec_id})")
                     return sec_id, sym

                print(f"⚠️ {target_root} Futures not found.")
                return None, None
            
        except Exception as e:
            print(f"⚠️ Error in fetch_security_list: {e}")
        return None, None

    def fetch_15m_data(self, security_id, symbol_name):
        """
        Fetches 1m candles and resamples to 15m.
        """
        to_date = datetime.datetime.now().strftime('%Y-%m-%d')
        from_date = (datetime.datetime.now() - datetime.timedelta(days=5)).strftime('%Y-%m-%d') 
        
        print(f"🔄 Fetching 1m Data for {symbol_name} ({from_date} to {to_date})...")
        
        try:
            # intraday_minute_data(security_id, exchange_segment, instrument_type, from_date, to_date)
            res = self.dhan.intraday_minute_data(
                security_id=security_id,
                exchange_segment=self.dhan.NSE_FNO,
                instrument_type='FUTIDX', 
                from_date=from_date,
                to_date=to_date
            )
            
            if res.get('status') == 'success':
                 data = res.get('data')
                 df = pd.DataFrame(data)
                 # Normalize columns immediately
                 df.columns = [c.lower() for c in df.columns]
                 
                 # Resample to 15m
                 # Check for start_time OR timestamp
                 date_col = 'start_time' if 'start_time' in df.columns else 'timestamp' if 'timestamp' in df.columns else None
                 
                 if not df.empty and date_col:
                     # Calculate OHLC
                     if date_col == 'timestamp':
                         # Convert epoch to datetime if needed, or if string
                         # Dhan timestamp might be int?
                         # Let's inspect first row content previously: 1768... looks like epoch.
                         df['start_time'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce')
                         # Fallback if unit is ms? 176... is seconds for 2026.
                     else:
                         df['start_time'] = pd.to_datetime(df['start_time'])
                         
                     df = df.set_index('start_time')
                     df = df.sort_index()
                     
                     df['open'] = df['open'].astype(float)
                     df['high'] = df['high'].astype(float)
                     df['low'] = df['low'].astype(float)
                     df['close'] = df['close'].astype(float)
                     
                     ohlc_dict = {
                         'open': 'first',
                         'high': 'max',
                         'low': 'min',
                         'close': 'last'
                     }
                     if 'volume' in df.columns:
                         df['volume'] = df['volume'].astype(float)
                         ohlc_dict['volume'] = 'sum'

                     df_15m = df.resample('15min').agg(ohlc_dict).dropna()
                     df_15m = df_15m.reset_index() # Assign back!
                     print(f"📉 Resampled {len(df)} 1m candles to {len(df_15m)} 15m candles.")
                     return df_15m
                 else:
                     return df
            else:
                 print(f"❌ API Error: {res}")
                 return None

        except Exception as e:
            print(f"❌ Fetch Error: {e}")
            return None

    def analyze_structure(self, df, symbol_name, security_id):
        """
        Identifies PDH, PDL, and Order Blocks (SMC).
        """
        if df is None or df.empty:
            print("❌ DF is empty/None in analyze_structure")
            return []
            
        zones = []
        try:
            # Normalize
            df.columns = [c.lower() for c in df.columns]
            print(f"🔍 Columns: {df.columns.tolist()}")
            print(f"🔍 First Row: {df.iloc[0].to_dict()}")
            
            # 1. Previous Day High/Low (PDH/PDL)
            # Need to group by Date.
            # Assuming 'start_time' is available.
            if 'start_time' not in df.columns:
                 print("❌ 'start_time' missing in columns")
                 return []
                 
            df['date'] = pd.to_datetime(df['start_time']).dt.date
            
            # Get Unique Dates
            dates = df['date'].unique()
            if len(dates) < 2:
                print("⚠️ Not enough data for PDH/PDL (Need > 1 day)")
                # Just use recent high/low
                pdh = df['high'].max()
                pdl = df['low'].min()
            else:
                prev_date = dates[-2] # Second to last (Last is Current/Incomplete?)
                prev_df = df[df['date'] == prev_date]
                pdh = prev_df['high'].max()
                pdl = prev_df['low'].min()
                
            print(f"📍 {symbol_name} PDH: {pdh}, PDL: {pdl}")
            
            # Add Zones for PDH/PDL (Liquidity Levels)
            zones.append({
                "id": f"{symbol_name}_PDH",
                "symbol": symbol_name,
                "security_id": security_id,
                "type": "SUPPLY", # PDH acts as Supply/Liquidity
                "timeframe": "1D",
                "range_high": pdh + 10, # Slight buffer
                "range_low": pdh - 10,
                "status": "ACTIVE",
                "note": "Previous Day High - Wait for Sweep"
            })
            
            zones.append({
                "id": f"{symbol_name}_PDL",
                "symbol": symbol_name,
                "security_id": security_id,
                "type": "DEMAND", # PDL acts as Demand/Liquidity
                "timeframe": "1D",
                "range_high": pdl + 10,
                "range_low": pdl - 10,
                "status": "ACTIVE",
                "note": "Previous Day Low - Wait for Sweep"
            })

            # 2. Order Blocks (15m)
            # Simple SMC Logic:
            # Bullish OB: Last Red Candle before a BOS (Break of Structure) or Imbalance.
            # Simplified: Strong Move (> 1.5x Avg Body).
            
            df['open'] = df['open'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            df['close'] = df['close'].astype(float)
            df['body'] = abs(df['close'] - df['open'])
            avg_body = df['body'].rolling(20).mean()
            
            for i in range(20, len(df)-2):
                curr = df.iloc[i]
                prev = df.iloc[i-1]
                
                # Check for Imbalance (Strong Move)
                if curr['body'] > (avg_body.iloc[i] * 1.5):
                    
                    # Bullish Move (Green) -> Prev Candle was Red?
                    if curr['close'] > curr['open']:
                        if prev['close'] < prev['open']: # Red Candle
                            # Valid Bullish OB
                            zones.append({
                                "id": f"OB_DEMAND_{len(zones)}",
                                "symbol": symbol_name,
                                "security_id": security_id,
                                "type": "DEMAND",
                                "timeframe": "15m",
                                "range_high": prev['high'],
                                "range_low": prev['low'],
                                "note": "15m Bullish Order Block"
                            })
                            
                    # Bearish Move (Red) -> Prev Candle was Green?
                    elif curr['close'] < curr['open']:
                        if prev['close'] > prev['open']: # Green Candle
                            # Valid Bearish OB
                            zones.append({
                                "id": f"OB_SUPPLY_{len(zones)}",
                                "symbol": symbol_name,
                                "security_id": security_id,
                                "type": "SUPPLY",
                                "timeframe": "15m",
                                "range_high": prev['high'],
                                "range_low": prev['low'],
                                "note": "15m Bearish Order Block"
                            })

        except Exception as e:
            print(f"❌ Analysis Error: {e}")
            
        # Return PDH/PDL (First 2) + Latest 4 OBs
        # zones[0] and zones[1] are PDH/PDL usually.
        # But if no OBs found, slicing might be weird.
        if len(zones) <= 6:
            return zones
        
        # Keep first 2, and last 4
        final_zones = zones[:2] + zones[-4:]
        return final_zones

    def run_analysis(self):
        print("🚀 Starting Fortress Sweep Analysis (15m)...")
        targets = ["NIFTY", "BANKNIFTY"]
        all_zones = []
        
        for target in targets:
            sec_id, sym = self._get_security_id(target)
            if sec_id:
                df = self.fetch_15m_data(sec_id, sym)
                if df is not None:
                    print(f"📊 {sym}: {len(df)} candles fetched.")
                    zones = self.analyze_structure(df, sym, sec_id)
                    all_zones.extend(zones)
                else:
                    print(f"❌ No data for {sym}")
            else:
                print(f"❌ Security ID not found for {target}")
                
        if all_zones:
             with open(ZONES_FILE, 'w') as f:
                 json.dump(all_zones, f, indent=2)
             print(f"✅ Updated {ZONES_FILE} with {len(all_zones)} zones.")
        else:
             print("⚠️ No zones identified.")

if __name__ == "__main__":
    analyzer = MarketAnalyzer()
    analyzer.run_analysis()
