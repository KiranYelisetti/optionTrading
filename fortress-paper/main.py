import logging
import threading
import time
import datetime
import json
from dhanhq import dhanhq, DhanFeed
import pandas as pd
from config import CLIENT_ID, ACCESS_TOKEN, ZONES_FILE, DB_PATH, LOG_FILE_PATH, TRADE_LOG_FILE
from core.virtual_broker import VirtualBroker
from core.strategy import FortressStrategy
from core.data_recorder import DataRecorder

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global State
dhan = None
feed = None 
running = True

# Initialize Modules
broker = VirtualBroker(log_file=TRADE_LOG_FILE)
strategy = FortressStrategy(zones_file=ZONES_FILE)
recorder = DataRecorder(db_path=DB_PATH)

def slow_loop():
    """
    Background Task: Fetches Option Chain every 3 minutes.
    Updates Strategy Sentiment.
    """
    logging.info("🐢 Slow Loop Started (Thread)")
    while running:
        try:
            # Placeholder for Option Chain Logic
            pass 
        except Exception as e:
            logging.error(f"Slow Loop Error: {e}")
        time.sleep(180)

def check_candle_loop():
    """
    Background Task: Fetches 5-min Candles every minute.
    Checks for Sweep Entries.
    """
    logging.info("🕯️ Candle Check Loop Started (1m Interval)")
    
    # Symbols to watch
    try:
        with open(ZONES_FILE, 'r') as f:
            zones = json.load(f)
            watch_list = {} # {security_id: symbol}
            for z in zones:
                if 'security_id' in z:
                    watch_list[z['security_id']] = z['symbol']
    except:
        watch_list = {}
    
    processed_candles = set()
    
    while running:
        try:
            # We iterate over unique instruments to watch
            for sec_id, symbol in watch_list.items():
                to_date = datetime.datetime.now().strftime('%Y-%m-%d')
                
                try:
                    res = dhan.intraday_minute_data(
                        security_id=sec_id,
                        exchange_segment=dhan.NSE_FNO,
                        instrument_type='FUTIDX',
                        from_date=to_date,
                        to_date=to_date,
                        interval='5' # <--- 5-Minute Data as per Strategy Refinement
                    )
                    
                    if res.get('status') == 'success' and res.get('data'):
                         data = res.get('data')
                         if not data:
                             continue
                             
                         latest = data[-1]
                         # Check for duplicates using start_time (assuming API returns it)
                         # Dhan API data list usually has 'start_time' or similar. 
                         # Let's check keys from previous inspection or assumption. 
                         # Standard key 'start_time' or 'date'? 
                         # Based on analyzer.py usage, it might be an ordered list or dict.
                         # Actually `intraday_minute_data` returns a dict with 'data' as list of things.
                         # Each item has 'start_time' usually.
                         
                         c_time = latest.get('start_time') or latest.get('time')
                         if not c_time:
                             # Fallback if key missing (unlikely if success)
                             continue
                             
                         # Unique Key for this candle
                         c_key = f"{symbol}_{c_time}"
                         
                         if c_key in processed_candles:
                             continue
                             
                         processed_candles.add(c_key)
                         
                         candle = {
                             'symbol': symbol,
                             'high': float(latest.get('high', 0)),
                             'low': float(latest.get('low', 0)),
                             'close': float(latest.get('close', 0)),
                             'open': float(latest.get('open', 0))
                         }
                         
                         sentiment = strategy.market_sentiment_flag
                         signal_data = strategy.check_entry(candle, sentiment)
                         
                         if signal_data:
                             signal = signal_data['action']
                             atm_strike = signal_data['atm_strike']
                             underlying = signal_data['underlying']
                             reason = signal_data['reason']
                             
                             logging.info(f"⚡ Signal {signal} on {symbol} (Reason: {reason})")
                             
                             width = 500 if "BANKNIFTY" in underlying else 200
                             
                             if signal == "BUY_PUT_SPREAD":
                                 # Bull Put Spread (Bullish Strategy: Sell PE, Buy Lower PE)
                                 # Wait, standard Bull Put Spread is SELL High Strike PE, BUY Low Strike PE.
                                 # This is a CREDIT Strategy.
                                 sell_strike = atm_strike 
                                 buy_strike = atm_strike - width
                                 
                                 # Leg 1: BUY Hedge (Long OTM)
                                 leg1 = {'symbol': f"{underlying} {buy_strike} PE", 'qty': 50, 'price': 0, 'side': 'BUY'} 
                                 # Leg 2: SELL Premium (Short ATM)
                                 leg2 = {'symbol': f"{underlying} {sell_strike} PE", 'qty': 50, 'price': 0, 'side': 'SELL'}
                                 
                                 broker.execute_spread(leg1, leg2)
                                 subscribe_to_legs(leg1['symbol'], leg2['symbol'])
                                 
                             elif signal == "SELL_CALL_SPREAD":
                                 # Bear Call Spread (Bearish Strategy: Sell CE, Buy Higher CE)
                                 # Credit Strategy.
                                 sell_strike = atm_strike
                                 buy_strike = atm_strike + width
                                 
                                 leg1 = {'symbol': f"{underlying} {buy_strike} CE", 'qty': 50, 'price': 0, 'side': 'BUY'}
                                 leg2 = {'symbol': f"{underlying} {sell_strike} CE", 'qty': 50, 'price': 0, 'side': 'SELL'}
                                 
                                 broker.execute_spread(leg1, leg2)
                                 subscribe_to_legs(leg1['symbol'], leg2['symbol'])
                                 
                except Exception as e_inner:
                    logging.error(f"Error checking candle for {symbol}: {e_inner}")
                    
        except Exception as e:
            logging.error(f"Candle Loop Error: {e}")
            
            
        # Align to next minute start for precision
        sleep_time = 60 - (time.time() % 60)
        time.sleep(sleep_time) 

# ... existing code ...

SCRIP_MASTER_DF = None

def load_scrip_master():
    global SCRIP_MASTER_DF
    logging.info("📥 Loading Scrip Master (this may take a moment)...")
    try:
        res = dhan.fetch_security_list()
        if isinstance(res, pd.DataFrame):
            SCRIP_MASTER_DF = res
        elif isinstance(res, dict) and 'data' in res:
            SCRIP_MASTER_DF = pd.DataFrame(res['data'])
        else:
            logging.warning(f"⚠️ Unexpected Scrip Master format: {type(res)}")
            return

        # Normalize columns for consistency
        SCRIP_MASTER_DF.columns = [x.strip().upper() for x in SCRIP_MASTER_DF.columns]
        logging.info(f"✅ Scrip Master Loaded: {len(SCRIP_MASTER_DF)} records")
    except Exception as e:
        logging.error(f"❌ Failed to load Scrip Master: {e}")

def subscribe_to_legs(leg1_symbol, leg2_symbol):
    """
    Dynamically subscribes to new Option Strikes.
    """
    
    if not feed:
        logging.error("❌ Feed not ready for subscription.")
        return
        
    if SCRIP_MASTER_DF is None:
        logging.error("❌ Scrip Master not loaded. Cannot look up IDs.")
        return

    tokens_to_sub = []
    
    for sym in [leg1_symbol, leg2_symbol]:
        # Filter Master to find ID
        # Strategy format: "NIFTY 30 JAN 25500 CE"
        # We need to match this against 'SEM_TRADING_SYMBOL' or construct it.
        # Assuming Strategy generates symbols matching Dhan's 'SEM_TRADING_SYMBOL' or 'SEM_CUSTOM_SYMBOL'.
        # Let's try exact match on SEM_TRADING_SYMBOL first.
        
        # Note: SCRIP_MASTER_DF columns normalized to UPPER.
        # Likely 'SEM_TRADING_SYMBOL'
        
        row = SCRIP_MASTER_DF[SCRIP_MASTER_DF['SEM_TRADING_SYMBOL'] == sym]
        if not row.empty:
            sec_id = str(row.iloc[0]['SEM_SECURITY_ID'])
            # Exchange Segment: NSE_FNO is usually 2.
            # We can verify from master 'SEM_EXM_EXCH_ID' (NSE) and 'SEM_SEGMENT' (D)?
            # Converting to Dhan convention: (ExchangeSegment, SecurityId)
            # NSE_FNO = 2
            tokens_to_sub.append((dhan.NSE_FNO, sec_id))
            logging.info(f"➕ Dynamically Subscribing: {sym} ({sec_id})")
        else:
            logging.warning(f"❌ ID Lookup Failed for {sym}")

    if tokens_to_sub:
        feed.subscribe_symbols(tokens_to_sub) # Check method name: subscribe or subscribe_symbols?
        # Inspection of `test_feed.py` output showed `subscribe_instruments` in `__dict__` and `subscribe_symbols`
        # Let's use `subscribe_symbols` if that's what the inspection showed or `subscribe_instruments`.
        # Inspection output step 543 showed: 'subscribe_instruments', 'subscribe_symbols', 'unsubscribe_symbols'
        # feed.subscribe_symbols takes (exchange_segment, security_id) tuples usually? 
        # Actually in inspection `subscribe_symbols(self, instruments)`
        # Let's use `subscribe_symbols`.

def check_candle_loop():
    # ... (rest of logic same) ...
    # At end of loop:
        # Align to next minute start
        sleep_time = 60 - (time.time() % 60)
        time.sleep(sleep_time)

# ... main function updates ...
def main():
    # ... logs ...
    load_scrip_master() 
    # ... rest ...

class LiveFeed(DhanFeed):
    """
    Custom Feed Handler to intercept messages.
    """
    def process_ticker(self, data):
        # Decode using Parent Logic
        res = super().process_ticker(data)
        # Map Keys for on_market_update
        # Inspection showed keys: "LTP", "security_id", etc.
        # We need to ensure consistency.
        
        # Add Symbol if possible? 
        # Ticker data only has Security ID.
        # We need a Map!
        # logic: local lookup map.
        
        # Pass to main handler
        on_market_update(res)
        return res
        
    def process_quote(self, data):
        res = super().process_quote(data)
        on_market_update(res)
        return res
        
    def process_oi(self, data):
        res = super().process_oi(data)
        on_market_update(res)
        return res
        
    async def _read_loop(self):
        try:
            await self.connect()
            logging.info("✅ Live Feed Connected via v2!")
            async for message in self.ws:
                self.process_data(message)
        except Exception as e:
            logging.error(f"Feed Loop Error: {e}")
            
    def run_forever(self):
        try:
            # Get existing loop or create new
            if self.loop.is_closed():
                import asyncio
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self._read_loop())
        except KeyboardInterrupt:
            pass
        except Exception as e:
             logging.error(f"Run Error: {e}")


def on_market_update(tick_data):
    """
    Fast Loop: Log Data & Check Stops.
    """
    try:
        recorder.log_tick(tick_data)
        
        if 'ltp' in tick_data and 'symbol' in tick_data:
            broker.update_ltp(tick_data['symbol'], float(tick_data['ltp']))
        
        risk_status = broker.check_risk()
        if risk_status:
            logging.warning(f"⚠️ RISK TRIGGER: {risk_status}. Closing All.")
            broker.close_all_positions(reason=risk_status)
            
    except Exception as e:
        logging.error(f"Fast Loop Error: {e}")

def main():
    global dhan, feed
    logging.info("🚀 Fortress Paper Trader Starting...")
    
    dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
    
    t_slow = threading.Thread(target=slow_loop)
    t_slow.daemon = True
    t_slow.start()
    
    t_candle = threading.Thread(target=check_candle_loop)
    t_candle.daemon = True
    t_candle.start()
    
    # Subscribe to Futures Zones
    instruments = []
    try:
        with open(ZONES_FILE, 'r') as f:
            zones = json.load(f)
            subscribed_ids = set()
            for z in zones:
                if 'security_id' in z and z.get('status', 'ACTIVE') == 'ACTIVE':
                    sid = z['security_id']
                    if sid not in subscribed_ids:
                        instruments.append((dhan.NSE_FNO, sid)) 
                        subscribed_ids.add(sid)
                        logging.info(f"➕ Subscribing to Zone: {z['symbol']} ({sid})")
    except Exception as e:
         logging.error(f"Zone Load Error: {e}")

    if instruments:
        logging.info(f"📡 Connecting to Live Feed with {len(instruments)} instruments...")
        # No callbacks in init, handled by Subclass overrides
        feed = LiveFeed(CLIENT_ID, ACCESS_TOKEN, instruments=instruments, version='v2')
        feed.run_forever()
    else:
        logging.warning("⚠️ No instruments to subscribe. Waiting...")
        while True:
            time.sleep(1)

if __name__ == "__main__":
    main()
