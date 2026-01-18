from dhanhq import dhanhq
from config import CLIENT_ID, ACCESS_TOKEN
import pandas as pd
import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check():
    try:
        logging.info("🚀 Starting Symbol Format Verification...")
        dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
        
        logging.info("📥 Fetching Scrip Master from Dhan...")
        res = dhan.fetch_security_list()
        
        if isinstance(res, dict) and 'data' in res:
             df = pd.DataFrame(res['data'])
        elif isinstance(res, pd.DataFrame):
             df = res
        else:
             logging.error("❌ Invalid Scrip Master Format")
             return

        # Normalize columns
        df.columns = [x.strip().upper() for x in df.columns]
        
        # Helper Logic (Same as main.py)
        today = datetime.date.today()
        days_ahead = 3 - today.weekday()
        if days_ahead < 0: days_ahead += 7
        next_thursday = today + datetime.timedelta(days=days_ahead)
        expiry_str = next_thursday.strftime("%d %b").upper() # e.g., "23 JAN"
        
        logging.info(f"📅 Calculated Expiry String: '{expiry_str}'")
        
        # Test Nifty Strike (Round to nearest 50)
        # Just pick a realistic strike e.g. 25000 or current market + 500
        test_strike = 25000 
        test_symbol = f"NIFTY {expiry_str} {test_strike} PE"
        
        logging.info(f"🔎 Searching for: '{test_symbol}'")
        
        # Search
        symbol_col = next((c for c in df.columns if 'TRADING_SYMBOL' in c), 'SEM_TRADING_SYMBOL')
        row = df[df[symbol_col] == test_symbol]
        
        if not row.empty:
            logging.info("✅ SUCCESS! Symbol found in Scrip Master.")
            logging.info(f"📄 Record: {row.iloc[0].to_dict()}")
        else:
            logging.error("❌ FAILED! Symbol not found.")
            logging.info("💡 Checking similar symbols to debug format:")
            # Filter for NIFTY and PE to show examples
            mask = df[symbol_col].str.contains("NIFTY") & df[symbol_col].str.contains("PE") & df[symbol_col].str.contains(str(test_strike))
            similar = df[mask].head(5)
            logging.info(f"Found {len(similar)} similar records:")
            print(similar[[symbol_col]].to_string(index=False))

    except Exception as e:
        logging.error(f"❌ Error: {e}")

if __name__ == "__main__":
    check()
