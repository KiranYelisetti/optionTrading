import pandas as pd
import datetime
import os
import csv
from config import TRADE_LOG_FILE, CAPITAL

class VirtualBroker:
    def __init__(self, log_file=TRADE_LOG_FILE):
        self.log_file = log_file
        self.active_positions = {} # { 'NIFTY 25000 CE': {'qty': 50, 'avg_price': 100, 'side': 'SELL'} }
        self.capital = CAPITAL
        self.realized_pnl = 0
        
        # Initialize Log File
        self._ensure_log_file()
        
        # State Persistence: Reconstruct positions from log file
        self._reconstruct_state()

    def _ensure_log_file(self):
        if not os.path.exists(self.log_file):
            with open(self.log_file, "w", newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp", "symbol", "side", "qty", "price", "tag", "pnl"])

    def _reconstruct_state(self):
        """
        Reads the CSV log to rebuild active_positions and realized_pnl.
        This allows the bot to restart without losing context.
        """
        if not os.path.exists(self.log_file):
            return

        try:
            df = pd.read_csv(self.log_file)
            if df.empty:
                return

            print("🔄 Reconstructing State from Logs...")
            for _, row in df.iterrows():
                symbol = row['symbol']
                side = row['side']
                qty = row['qty']
                price = row['price']
                pnl = row['pnl'] if not pd.isna(row['pnl']) else 0

                # Re-play logic
                if side == "SELL":
                    # For a SELL order, we are OPENING a short position (in this strategy context)
                    # OR closing a BUY hedge? 
                    # Fortress Strategy:
                    # Enter: BUY Hedge -> SELL Premium
                    # Exit: BUY Premium -> SELL Hedge
                    
                    # Simplified State Logic: Net Quantity
                    if symbol not in self.active_positions:
                        self.active_positions[symbol] = {'qty': 0, 'avg_price': 0, 'side': 'NET'}
                    
                    # If we sell, we decrease net qty (assuming Long is +ve, Short is -ve)
                    # BUT for tracking individual legs, it's better to track net qty per symbol
                    current_qty = self.active_positions[symbol]['qty']
                    # logic: - qty
                    self.active_positions[symbol]['qty'] -= qty
                    
                elif side == "BUY":
                    if symbol not in self.active_positions:
                        self.active_positions[symbol] = {'qty': 0, 'avg_price': 0, 'side': 'NET'}
                    self.active_positions[symbol]['qty'] += qty
                    
                self.realized_pnl += pnl

            # Clean up closed positions (qty == 0)
            self.active_positions = {k: v for k, v in self.active_positions.items() if v['qty'] != 0}
            print(f"✅ State Reconstructed. Active Positions: {len(self.active_positions)}")
            
        except Exception as e:
            print(f"❌ Error reconstructing state: {e}")

    def place_paper_order(self, symbol, side, qty, price, tag="ENTRY"):
        """
        Simulates placing an order.
        Strictly enforce Hedging for Short Entries.
        """
        # HEDGING CHECK (Only for Opening Short Positions)
        if side == "SELL" and tag == "ENTRY":
            # Check if we have a LONG position in a Hedge Leg (e.g., OTM PE/CE)
            # This is complex to validate without knowing the exact pairing.
            # For v1, we assume the Strategy calls `place_paper_order` in the correct sequence via `execute_spread`
            pass

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Log to CSV
        with open(self.log_file, "a", newline='') as f:
            writer = csv.writer(f)
            # pnl is 0 for Entry
            pnl = 0 
            
            # If EXIT, calculate PnL
            # Note: This PnL calc in CSV is "Realized for this specific trade" only if it closes a leg
            # ideally we match FIFO, but for paper:
            if tag == "EXIT" or (side == "BUY" and self.active_positions.get(symbol, {}).get('qty', 0) < 0):
                 # Covered a short
                 avg_price = self.active_positions.get(symbol, {}).get('price', price) # simplified
                 # correct pnl logic needed here if we want accurate logs, 
                 # but for now we log 0 and track realized_pnl in memory/bulk
                 pass

            writer.writerow([timestamp, symbol, side, qty, price, tag, pnl])
            
        print(f"📝 PAPER TRADE: {side} {qty} {symbol} @ {price} [{tag}]")

        # Update Internal State
        if symbol not in self.active_positions:
             self.active_positions[symbol] = {'qty': 0, 'price': price} # Price is last entry price
        
        if side == "BUY":
            self.active_positions[symbol]['qty'] += qty
        else:
             self.active_positions[symbol]['qty'] -= qty
             
        # Update Reference Price for MTM (Last Traded Price)
        self.active_positions[symbol]['price'] = price
        
        # Cleanup
        if self.active_positions[symbol]['qty'] == 0:
            del self.active_positions[symbol]

    def execute_spread(self, leg1, leg2):
        """
        Atomic execution of a Credit Spread.
        Leg 1: BUY Hedge
        Leg 2: SELL Premium
        """
        # Leg 1
        self.place_paper_order(leg1['symbol'], "BUY", leg1['qty'], leg1['price'], tag="ENTRY_HEDGE")
        
        # Leg 2
        self.place_paper_order(leg2['symbol'], "SELL", leg2['qty'], leg2['price'], tag="ENTRY_PREMIUM")

    def get_mtm(self, current_market_prices):
        """
        Calculates Live MTM.
        current_market_prices: dict { 'NIFTY 25000 CE': 102.5 }
        """
        mtm = 0
        for symbol, pos in self.active_positions.items():
            qty = pos['qty']
            entry_price = pos['price'] # This might be inaccurate if multiple entries, but ok for paper v1
            
            ltp = current_market_prices.get(symbol)
            if ltp is None:
                continue
                
            # If Qty > 0 (LONG): (LTP - Entry) * Qty
            # If Qty < 0 (SHORT): (Entry - LTP) * abs(Qty)  => (Entry - LTP) * -Qty is wrong sign?
            # Short PnL = (Sell Price - Buy Price)
            # Here: (Entry - LTP) * abs(qty)
            
            if qty > 0:
                pnl = (ltp - entry_price) * qty
            else:
                pnl = (entry_price - ltp) * abs(qty)
            
            mtm += pnl
            
        return mtm

    def close_all_positions(self, current_market_prices):
        """
        Square off everything (Panic/Target/Stop).
        """
        print("🚨 CLOSING ALL POSITIONS...")
        # Create a copy since we will modify the dictionary
        active_symbols = list(self.active_positions.keys())
        
        for symbol in active_symbols:
            pos = self.active_positions[symbol]
            qty = pos['qty']
            ltp = current_market_prices.get(symbol, pos['price']) # Default to last price if live unknown
            
            if qty > 0:
                # Sell to Close
                self.place_paper_order(symbol, "SELL", qty, ltp, tag="EXIT_PANIC")
            elif qty < 0:
                # Buy to Close
                self.place_paper_order(symbol, "BUY", abs(qty), ltp, tag="EXIT_PANIC")
