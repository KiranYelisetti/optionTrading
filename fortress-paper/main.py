import logging
import threading
import time
import datetime
import json
from dhanhq import dhanhq, DhanFeed
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
    global running
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
    Background Task: Fetches 1-minute candle every minute.
    Checks for Sweep Entry.
    """
    global running, dhan
    logging.info(f"🕯️ Candle Check Loop Started (1m Interval)")
    
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
                        to_date=to_date
                    )
                    
                    if res.get('status') == 'success' and res.get('data'):
                         data = res.get('data')
                         latest = data[-1]
                         
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
            
        time.sleep(60) 

def subscribe_to_legs(leg1_symbol, leg2_symbol):
    """
    Subscribes to Option Legs for MTM Tracking.
    Requires fetching Security ID.
    """
    global feed, dhan
    if not feed:
        return

    # TODO: Implement Lookup using dhan.fetch_security_list or cached master.
    # For now, we log intent.
    logging.info(f"📝 Need subscription for: {leg1_symbol}, {leg2_symbol}")

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
