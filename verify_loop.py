import sys
import os
import json
import logging

# Setup Paths
# Setup Paths
# Add 'fortress-paper' to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'fortress-paper')))

from core.strategy import FortressStrategy
from core.virtual_broker import VirtualBroker
from config import ZONES_FILE, TRADE_LOG_FILE

# Setup Logging
logging.basicConfig(level=logging.INFO)

def verify_system():
    print("🚀 Starting Logic Verification...")
    
    # 1. Load Strategy
    print(f"📂 Loading Zones from {ZONES_FILE}...")
    strategy = FortressStrategy(zones_file=ZONES_FILE)
    
    if not strategy.zones:
        print("❌ No zones loaded. Run analyzer.py first.")
        return

    # 2. Pick a Zone to Test
    test_zone = strategy.zones[0]
    symbol = test_zone['symbol']
    z_type = test_zone['type']
    low = test_zone['range_low']
    high = test_zone['range_high']
    mid = (low + high) / 2
    
    print(f"🧪 Testing against Zone: {test_zone['id']} ({symbol} {z_type} {low}-{high})")
    
    # 3. Mock Candle (Sweep Scenario)
    # Zone High = High Level
    # We want High > Level (Sweep) and Close < Level (Rejection)
    
    sweep_high = high + 20
    sweep_close = high - 5
    
    mock_candle = {
        'symbol': symbol,
        'high': sweep_high,
        'low': low,
        'close': sweep_close,
        'open': low
    }
    
    print(f"🔹 Mock Candle: {mock_candle} (Zone High: {high})")
    
    # 4. Check Strategy Entry
    # Set Sentiment to Favorable manually for testing
    expected_sentiment = "BULLISH" if z_type == "DEMAND" else "BEARISH"
    # Actually for Bullish, expected sweep is Low < Level, Close > Level.
    if z_type == "DEMAND":
        mock_candle['low'] = low - 20
        mock_candle['close'] = low + 5
        sweep_high = high # irrelevant
    
    # strategy.market_sentiment_flag is not used directly, passed as arg
    print(f"🔹 Forced Sentiment: {expected_sentiment}")
    
    signal = strategy.check_entry(mock_candle, expected_sentiment)
    
    if signal:
        print(f"✅ Signal Generated: {signal}")
        
        # 5. Verify Legs
        action = signal['action']
        atm = signal['atm_strike']
        underlying = signal['underlying']
        
        print(f"   -> Action: {action}")
        print(f"   -> ATM: {atm}")
        print(f"   -> Underlying: {underlying}")
        
        # 6. Simulate Execution (Virtual Broker)
        broker = VirtualBroker(log_file=TRADE_LOG_FILE)
        
        width = 500 if "BANKNIFTY" in underlying else 200
        
        if action == "BUY_PUT_SPREAD":
             leg1 = {'symbol': f"{underlying} {atm-width} PE", 'qty': 50, 'price': 10, 'side': 'BUY'} 
             leg2 = {'symbol': f"{underlying} {atm} PE", 'qty': 50, 'price': 20, 'side': 'SELL'}
             broker.execute_spread(leg1, leg2)
             
        elif action == "SELL_CALL_SPREAD":
             leg1 = {'symbol': f"{underlying} {atm+width} CE", 'qty': 50, 'price': 10, 'side': 'BUY'}
             leg2 = {'symbol': f"{underlying} {atm} CE", 'qty': 50, 'price': 20, 'side': 'SELL'}
             broker.execute_spread(leg1, leg2)
             
    else:
        print("❌ No Signal Generated. Check Logic.")

if __name__ == "__main__":
    verify_system()
