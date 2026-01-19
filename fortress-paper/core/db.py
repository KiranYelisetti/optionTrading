import os
import datetime
from supabase import create_client, Client
import pandas as pd
import json

class FortressDB:
    def __init__(self):
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        
        if not url or not key:
            print("❌ Supabase URL/KEY not found in env.")
            self.supabase = None
        else:
            try:
                self.supabase: Client = create_client(url, key)
                print("✅ Supabase Connected.")
            except Exception as e:
                print(f"❌ Supabase Connection Failed: {e}")
                self.supabase = None

    def save_zones(self, zones_list):
        """
        Upserts zones to 'trading_zones' table.
        """
        if not self.supabase or not zones_list:
            return
            
        print(f"💾 Saving {len(zones_list)} zones to Supabase...")
        try:
            # Upsert requires a primary key or unique constraint.
            # Assuming 'id' is unique in the list or suitable for upsert.
            # Our IDs are like "NIFTY_PDH", "OB_DEMAND_5" (which might conflict if re-generated).
            # ideally we should clear old zones for the symbol or generate unique IDs based on timestamp.
            # For this iteration, we will just insert/upsert. 
            # If ID collision happens, we update.
            
            # Prune data to match schema if needed
            data = []
            now_iso = datetime.datetime.now().isoformat()
            
            for z in zones_list:
                z['updated_at'] = now_iso
                # Ensure all fields are present
                data.append(z)
                
            self.supabase.table('trading_zones').upsert(data).execute()
            print("✅ Zones Saved.")
        except Exception as e:
            print(f"❌ Error saving zones: {e}")

    def get_active_zones(self, symbol=None):
        """
        Fetches active zones.
        """
        if not self.supabase:
            return []
            
        try:
            query = self.supabase.table('trading_zones').select("*").eq('status', 'ACTIVE')
            if symbol:
                query = query.eq('symbol', symbol)
                
            response = query.execute()
            return response.data
        except Exception as e:
            print(f"❌ Error fetching zones: {e}")
            return []

    def log_trade(self, trade_data):
        """
        Logs trade execution.
        """
        if not self.supabase:
            return
            
        try:
            self.supabase.table('trade_logs').insert(trade_data).execute()
        except Exception as e:
            print(f"❌ Error logging trade: {e}")

    def save_market_data(self, df, symbol, timeframe='15m'):
        """
        Saves OHLCV data to 'market_candles'.
        """
        if not self.supabase or df is None or df.empty:
            return
            
        print(f"💾 Saving {len(df)} candles for {symbol} to Supabase...")
        try:
            # Convert DF to list of dicts
            # Ensure columns match Supabase Schema: symbol, timestamp, open, high, low, close, volume, timeframe
            records = []
            
            # Reset index if date is index
            df_copy = df.copy()
            if 'start_time' not in df_copy.columns and isinstance(df_copy.index, pd.DatetimeIndex):
                df_copy = df_copy.reset_index()
                df_copy.rename(columns={'index': 'start_time'}, inplace=True)
            elif 'start_time' not in df_copy.columns:
                 # Try finding date column
                 pass
            
            for _, row in df_copy.iterrows():
                ts = row['start_time'].isoformat() if hasattr(row['start_time'], 'isoformat') else str(row['start_time'])
                
                record = {
                    'symbol': symbol,
                    'timestamp': ts,
                    'timeframe': timeframe,
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': float(row.get('volume', 0))
                }
                records.append(record)
                
            # Batch upsert might be better, but standard insert works.
            # Upsert based on (symbol, timestamp, timeframe) unique constraint? Assumed set in DB.
            self.supabase.table('market_candles').upsert(records, on_conflict='symbol, timeframe, timestamp').execute()
            print("✅ Market Data Saved.")
            
        except Exception as e:
            print(f"❌ Error saving market data: {e}")
