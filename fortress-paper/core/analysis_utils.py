import pandas as pd
import datetime

def resample_to_15m(df):
    """
    Resamples 1m data to 15m candles.
    """
    if df is None or df.empty:
        return None

    # Normalize columns
    df.columns = [c.lower() for c in df.columns]

    # Check for start_time OR timestamp
    date_col = 'start_time' if 'start_time' in df.columns else 'timestamp' if 'timestamp' in df.columns else None

    if not date_col:
        return df

    # Calculate OHLC
    # Create a copy to avoid SettingWithCopy warnings if slice
    df = df.copy()

    if date_col == 'timestamp':
        # Convert epoch to datetime if needed. 
        # Assuming Dhan returns seconds or close to it. 
        # Inspection showed 1768... which is seconds for 2026.
        df['start_time'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce')
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

    # Resample
    df_15m = df.resample('15min').agg(ohlc_dict).dropna()
    df_15m = df_15m.reset_index() 
    return df_15m

def identify_smart_money_structure(df, symbol_name, security_id):
    """
    Identifies PDH, PDL, and Order Blocks (SMC).
    """
    if df is None or df.empty:
        print("❌ DF is empty/None in identify_smart_money_structure")
        return []
        
    zones = []
    try:
        # Normalize
        df.columns = [c.lower() for c in df.columns]
        
        # 1. Previous Day High/Low (PDH/PDL)
        # Need to group by Date.
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
            # Avoid division by zero if avg_body is NaN or 0 (though unlikely with price)
            avg = avg_body.iloc[i]
            if pd.isna(avg) or avg == 0:
                continue

            if curr['body'] > (avg * 1.5):
                
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
        import traceback
        traceback.print_exc()
        
    # Return PDH/PDL (First 2) + Latest 4 OBs
    # zones[0] and zones[1] are PDH/PDL usually.
    # But if no OBs found, slicing might be weird.
    if len(zones) <= 6:
        return zones
    
    # Keep first 2, and last 4
    final_zones = zones[:2] + zones[-4:]
    return final_zones
