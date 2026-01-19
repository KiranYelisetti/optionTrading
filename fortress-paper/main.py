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
from core.db import FortressDB 
from probe_dhan_methods import print_methods

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global State
dhan = None
feed = None 
running = True

# Initialize Modules
broker = VirtualBroker(log_file=TRADE_LOG_FILE)
strategy = FortressStrategy(zones_file=None) # Don't load file
db = FortressDB()
SCRIP_MASTER_DF = None

# Load Zones from Supabase
try:
    active_zones = db.get_active_zones()
    if active_zones:
        strategy.zones = active_zones
        logging.info(f"✅ Loaded {len(active_zones)} Zones from Supabase.")
    else:
        logging.warning("⚠️ No Active Zones found in Supabase.")
except Exception as e:
    logging.error(f"❌ Failed to load zones from DB: {e}")

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
        # We need to match this against 'SEM_TRADING_SYMBOL'
        
        row = SCRIP_MASTER_DF[SCRIP_MASTER_DF['SEM_TRADING_SYMBOL'] == sym]
        if not row.empty:
            sec_id = str(row.iloc[0]['SEM_SECURITY_ID'])
            # Exchange Segment: NSE_FNO = 2
            tokens_to_sub.append((dhan.NSE_FNO, sec_id))
            logging.info(f"➕ Dynamically Subscribing: {sym} ({sec_id})")
        else:
            logging.warning(f"❌ ID Lookup Failed for {sym}")

    if tokens_to_sub:
        feed.subscribe_symbols(tokens_to_sub)

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
    watch_list = {} # {security_id: symbol}
    if strategy.zones:
        for z in strategy.zones:
            if 'security_id' in z:
                watch_list[z['security_id']] = z['symbol']
    
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
                        interval='5' # 5-Minute Data
                    )
                    
                    if res.get('status') == 'success' and res.get('data'):
                         data = res.get('data')
                         if not data:
                             continue
                             
                         latest = data[-1]
                         c_time = latest.get('start_time') or latest.get('time')
                         if not c_time:
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
                             
                             # Helper to get Expiry String
                             def get_expiry_str():
                                 today = datetime.date.today()
                                 days = (3 - today.weekday() + 7) % 7
                                 return (today + datetime.timedelta(days=days)).strftime("%d %b").upper()
                             
                             expiry_str = get_expiry_str()
                             
                             trade_res = None

                             if signal == "BUY_PUT_SPREAD":
                                 # Bull Put Spread (Credit Strategy)
                                 sell_strike = atm_strike 
                                 buy_strike = atm_strike - width
                                 
                                 sym_buy = f"{underlying} {expiry_str} {buy_strike} PE"
                                 sym_sell = f"{underlying} {expiry_str} {sell_strike} PE"
                                 
                                 leg1 = {'symbol': sym_buy, 'qty': 50, 'price': 0, 'side': 'BUY'} 
                                 leg2 = {'symbol': sym_sell, 'qty': 50, 'price': 0, 'side': 'SELL'}
                                 
                                 trade_res = broker.execute_spread(leg1, leg2)
                                 subscribe_to_legs(leg1['symbol'], leg2['symbol'])
                                 
                             elif signal == "SELL_CALL_SPREAD":
                                 # Bear Call Spread (Credit Strategy)
                                 sell_strike = atm_strike
                                 buy_strike = atm_strike + width
                                 
                                 sym_buy = f"{underlying} {expiry_str} {buy_strike} CE"
                                 sym_sell = f"{underlying} {expiry_str} {sell_strike} CE"
                                 
                                 leg1 = {'symbol': sym_buy, 'qty': 50, 'price': 0, 'side': 'BUY'}
                                 leg2 = {'symbol': sym_sell, 'qty': 50, 'price': 0, 'side': 'SELL'}
                                 
                                 trade_res = broker.execute_spread(leg1, leg2)
                                 subscribe_to_legs(leg1['symbol'], leg2['symbol'])
                             
                             # Log to DB
                             if trade_res:
                                 db.log_trade({
                                     "symbol": symbol,
                                     "action": signal,
                                     "price": candle['close'],
                                     "timestamp": datetime.datetime.now().isoformat(),
                                     "details": str(trade_res)
                                 })
                                 
                except Exception as e_inner:
                    logging.error(f"Error checking candle for {symbol}: {e_inner}")
                    
        except Exception as e:
            logging.error(f"Candle Loop Error: {e}")
            
        # Align to next minute start for precision
        sleep_time = 60 - (time.time() % 60)
        time.sleep(sleep_time) 

class LiveFeed(DhanFeed):
    """
    Custom Feed Handler to intercept messages.
    """
    def process_ticker(self, data):
        # Decode using Parent Logic
        res = super().process_ticker(data)
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
        # recorder.log_tick(tick_data) # Skip high freq logging to DB for now
        
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

    # Load Scrip Master (Critical for Subscription)
    load_scrip_master()
    
    t_slow = threading.Thread(target=slow_loop)
    t_slow.daemon = True
    t_slow.start()
    
    t_candle = threading.Thread(target=check_candle_loop)
    t_candle.daemon = True
    t_candle.start()
    
    # Subscribe to Futures Zones
    instruments = []
    if strategy.zones:
        subscribed_ids = set()
        for z in strategy.zones:
            if 'security_id' in z and z.get('status', 'ACTIVE') == 'ACTIVE':
                sid = z['security_id']
                if sid not in subscribed_ids:
                    instruments.append((dhan.NSE_FNO, sid)) 
                    subscribed_ids.add(sid)
                    logging.info(f"➕ Subscribing to Zone: {z['symbol']} ({sid})")
    else:
         logging.warning("⚠️ No Zones available for subscription.")

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
