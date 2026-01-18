import logging
import threading
import time
from dhanhq import dhanhq
from config import CLIENT_ID, ACCESS_TOKEN, ZONES_FILE, DB_PATH, LOG_FILE_PATH, TRADE_LOG_FILE
from core.virtual_broker import VirtualBroker
from core.strategy import FortressStrategy
from core.data_recorder import DataRecorder

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global State
dhan = None
feed = None # Placeholder for Feed Object
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
            # 1. Fetch Option Chain (Example: NIFTY)
            # Note: You need to specify ExchangeSegment, Expiry, etc. based on Dhan Docs
            # This is pseudo-code for the API call
            # chain_data = dhan.option_chain(exchange_segment=dhan.NSE_FNO, instrument_type='OPTIDX', symbol='NIFTY', expiry='...')
            
            # For Safety, we wrap in try-except and just log placeholder for now
            # logging.info("Fetching Option Chain...")
            # chain_data = [] # ... fetch logic ...
            
            # recorder.log_option_chain(chain_data)
            # strategy.update_market_sentiment(chain_data)
            
            pass # Replace with actual API calls
            
        except Exception as e:
            logging.error(f"Slow Loop Error: {e}")
            
        time.sleep(180) # 3 Minutes

def on_market_update(tick_data):
    """
    Fast Loop: WebSocket Callback.
    1. Log Data (SQLite)
    2. Check Entry (Strategy)
    3. Execute (Virtual Broker)
    """
    try:
        # tick_data structure depends on Dhan Feed
        # logging.debug(f"Tick: {tick_data}")
        
        # 1. Log to DB
        recorder.log_tick(tick_data)
        
        # 2. Check Entry
        signal_data = strategy.check_entry(tick_data)
        
        if signal_data:
            signal = signal_data['action']
            atm_strike = signal_data['atm_strike']
            underlying = signal_data['underlying']
            zone_id = signal_data['zone_id']
            
            symbol = tick_data['symbol']
            ltp = tick_data['ltp']
            logging.info(f"⚡ Signal {signal} on {symbol} @ {ltp} (ATM: {atm_strike}) [Zone: {zone_id}]")
            
            # Execute Hedged Spread
            # Dynamic Strike Width: 200 for Nifty, 500 for BankNifty?
            # Keeping it simple 200/500 based on Name
            
            width = 500 if "BANKNIFTY" in underlying else 200
            
            if signal == "BUY_PUT_SPREAD":
                # Bullish: Sell ATM PE, Buy OTM PE (Credit Spread? No, Bull Put Spread is Credit)
                # Wait. "BUY_PUT_SPREAD" naming in strategy was confusing.
                # Strategy said: Demand -> Expect UP.
                # Bullish Strategy: Bull Put Spread (Sell PE, Buy Lower PE).
                
                sell_leg_strike = atm_strike
                buy_leg_strike = atm_strike - width
                
                leg1 = {'symbol': f"{underlying} {buy_leg_strike} PE", 'qty': 50, 'price': 0, 'side': 'BUY'} 
                leg2 = {'symbol': f"{underlying} {sell_leg_strike} PE", 'qty': 50, 'price': 0, 'side': 'SELL'}
                broker.execute_spread(leg1, leg2) # Note: Broker executes leg1 then leg2. Buy then Sell (Hedged).
                
            elif signal == "SELL_CALL_SPREAD":
                # Bearish: Sell ATM CE, Buy Higher CE (Bear Call Spread)
                
                sell_leg_strike = atm_strike
                buy_leg_strike = atm_strike + width
                
                leg1 = {'symbol': f"{underlying} {buy_leg_strike} CE", 'qty': 50, 'price': 0, 'side': 'BUY'}
                leg2 = {'symbol': f"{underlying} {sell_leg_strike} CE", 'qty': 50, 'price': 0, 'side': 'SELL'}
                broker.execute_spread(leg1, leg2)

        # 3. Check Risk/MTM
        # We need a way to get current market prices for all open positions
        # Ideally, we subscribe to them. For now, we assume tick_data might contain them or we fetch map.
        # current_prices = { ... } 
        # mtm = broker.get_mtm(current_prices)
        # Check Global Target/Stop (Implemented in broker or here)

    except Exception as e:
        logging.error(f"Fast Loop Error: {e}")

def main():
    global dhan
    logging.info("🚀 Fortress Paper Trader Starting...")
    
    # Connect to Dhan
    dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
    
def check_candle_loop():
    """
    Background Task: Fetches 1-minute candle every minute.
    Checks for Sweep Entry.
    """
    global running, dhan
    logging.info(f"🕯️ Candle Check Loop Started (1m Interval)")
    
    # Symbols to watch
    import json
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
        # Align to minute boundary
        # time.sleep(60 - time.time() % 60) 
        
        try:
            # We iterate over unique instruments to watch
            for sec_id, symbol in watch_list.items():
                # Fetch last 1-min candle
                # interval=1
                # We need "Intraday" chart
                to_date = datetime.datetime.now().strftime('%Y-%m-%d')
                from_date = to_date # Today
                
                # Fetching...
                # Note: This is synchronous and might block. In prod, use async or seperate threads per symbol.
                try:
                    res = dhan.intraday_minute_data(
                        security_id=sec_id,
                        exchange_segment=dhan.NSE_FNO,
                        instrument_type='FUTIDX',
                        from_date=from_date,
                        to_date=to_date
                    )
                    
                    if res.get('status') == 'success' and res.get('data'):
                         data = res.get('data')
                         # Last candle is the latest completed one?
                         # Usually last item in list is current forming candle? Or completed?
                         # check timestamp. 
                         # We'll take the LAST item as "Latest Candle".
                         # If it's forming, Close changes.
                         # Ideally we want the one that JUST closed.
                         # But let's check the latest available data point.
                         
                         latest = data[-1]
                         # Format for strategy
                         candle = {
                             'symbol': symbol,
                             'high': float(latest.get('high', 0)),
                             'low': float(latest.get('low', 0)),
                             'close': float(latest.get('close', 0)),
                             'open': float(latest.get('open', 0))
                         }
                         
                         # Get Sentiment from Strategy (Slow Loop updates it)
                         sentiment = strategy.market_sentiment_flag
                         
                         signal_data = strategy.check_entry(candle, sentiment)
                         
                         if signal_data:
                             signal = signal_data['action']
                             atm_strike = signal_data['atm_strike']
                             underlying = signal_data['underlying']
                             reason = signal_data['reason']
                             
                             logging.info(f"⚡ Signal {signal} on {symbol} (Reason: {reason})")
                             
                             # Execute Specific Spreads (ATM+/-50/200) as per Fortress Sweep
                             
                             if signal == "BUY_PUT_SPREAD":
                                 # Bullish: Sell ATM-50 PE, Buy ATM-200 PE
                                 sell_strike = atm_strike - 50
                                 buy_strike = atm_strike - 200
                                 
                                 leg1 = {'symbol': f"{underlying} {buy_strike} PE", 'qty': 50, 'price': 0, 'side': 'BUY'} 
                                 leg2 = {'symbol': f"{underlying} {sell_strike} PE", 'qty': 50, 'price': 0, 'side': 'SELL'}
                                 broker.execute_spread(leg1, leg2)
                                 
                             elif signal == "SELL_CALL_SPREAD":
                                 # Bearish: Sell ATM+50 CE, Buy ATM+200 CE
                                 sell_strike = atm_strike + 50
                                 buy_strike = atm_strike + 200
                                 
                                 leg1 = {'symbol': f"{underlying} {buy_strike} CE", 'qty': 50, 'price': 0, 'side': 'BUY'}
                                 leg2 = {'symbol': f"{underlying} {sell_strike} CE", 'qty': 50, 'price': 0, 'side': 'SELL'}
                                 broker.execute_spread(leg1, leg2)
                                 
                except Exception as e_inner:
                    logging.error(f"Error checking candle for {symbol}: {e_inner}")
                    
        except Exception as e:
            logging.error(f"Candle Loop Error: {e}")
            
        time.sleep(60) # check every minute

def on_market_update(tick_data):
    """
    Fast Loop: Log Data & Check Stops (Risk Management).
    Entry is now handled by check_candle_loop.
    """
    try:
        recorder.log_tick(tick_data)
        
        # Update Broker with Real-time Prices
        if 'ltp' in tick_data and 'symbol' in tick_data:
            broker.update_ltp(tick_data['symbol'], float(tick_data['ltp']))
        
        # 3. Check Risk/MTM (Stop Loss Trigger)
        risk_status = broker.check_risk()
        if risk_status:
            logging.warning(f"⚠️ RISK TRIGGER: {risk_status}. Closing All.")
            broker.close_all_positions(reason=risk_status)
            # Maybe stop the loop or strategy?
            # running = False ?
            
        # Display MTM occasionally?
        # logging.info(f"MTM: {broker.get_mtm()}") 
        
    except Exception as e:
        logging.error(f"Fast Loop Error: {e}")

# ... (main function) ...
# We need to handle Dynamic Subscription.
# This requires access to the `feed` object, which is currently a placeholder or local var.
# Ideally make `feed` global or accessible.


def main():
    global dhan
    logging.info("🚀 Fortress Paper Trader Starting...")
    
    # Connect to Dhan
    dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
    
    # Start Slow Loop (Option Chain)
    t_slow = threading.Thread(target=slow_loop)
    t_slow.daemon = True
    t_slow.start()
    
    # NEW: Start Candle Check Loop (Strategy Entry)
    t_candle = threading.Thread(target=check_candle_loop)
    t_candle.daemon = True
    t_candle.start()
    
    # Keep Main Thread Alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping...")
        recorder.close()

if __name__ == "__main__":
    main()
