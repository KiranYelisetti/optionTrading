import pandas as pd
import datetime
import os
import csv
from config import TRADE_LOG_FILE, CAPITAL

class VirtualBroker:
    def __init__(self, log_file=TRADE_LOG_FILE):
        self.log_file = log_file
        self.active_positions = {} # { 'NIFTY 25000 CE': {'qty': 50, 'price': 100, 'side': 'SELL', 'ltp': 100} }
        self.capital = CAPITAL
        self.realized_pnl = 0
        
        # Risk Config
        self.daily_target = 1000.0
        self.daily_sl = -750.0 
        
        # Initialize Log File
        self._ensure_log_file()
        
        # State Persistence
        self._reconstruct_state()

    def _ensure_log_file(self):
        if not os.path.exists(self.log_file):
            with open(self.log_file, "w", newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp", "symbol", "side", "qty", "price", "tag", "pnl"])

    def _reconstruct_state(self):
        """
        Reads the CSV log to rebuild active_positions and realized_pnl.
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

                if symbol not in self.active_positions:
                    # Init with basic struct
                    self.active_positions[symbol] = {'qty': 0, 'price': 0, 'ltp': 0}
                
                if side == "BUY":
                    self.active_positions[symbol]['qty'] += qty
                elif side == "SELL":
                    self.active_positions[symbol]['qty'] -= qty
                    
                self.realized_pnl += pnl

            # Clean up closed
            self.active_positions = {k: v for k, v in self.active_positions.items() if v['qty'] != 0}
            print(f"✅ State Reconstructed. Active Positions: {len(self.active_positions)}")
            
        except Exception as e:
            print(f"❌ Error reconstructing state: {e}")

    def place_paper_order(self, symbol, side, qty, price, tag="ENTRY"):
        """
        Simulates placing an order.
        """
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(self.log_file, "a", newline='') as f:
            writer = csv.writer(f)
            pnl = 0 
            writer.writerow([timestamp, symbol, side, qty, price, tag, pnl])
            
        print(f"📝 PAPER TRADE: {side} {qty} {symbol} @ {price} [{tag}]")

        if symbol not in self.active_positions:
             self.active_positions[symbol] = {'qty': 0, 'price': price, 'ltp': price}
        
        if side == "BUY":
            self.active_positions[symbol]['qty'] += qty
        else:
             self.active_positions[symbol]['qty'] -= qty
             
        self.active_positions[symbol]['price'] = price # Update last entry price
        
        if self.active_positions[symbol]['qty'] == 0:
            del self.active_positions[symbol]

    def execute_spread(self, leg1, leg2):
        """
        Atomic execution of a Credit Spread.
        """
        self.place_paper_order(leg1['symbol'], "BUY", leg1['qty'], leg1['price'], tag="ENTRY_HEDGE")
        self.place_paper_order(leg2['symbol'], "SELL", leg2['qty'], leg2['price'], tag="ENTRY_PREMIUM")

    def update_ltp(self, symbol, ltp):
        """
        Updates LTP for a position.
        """
        if symbol in self.active_positions:
            self.active_positions[symbol]['ltp'] = ltp

    def get_mtm(self):
        """
        Calculates Live MTM using stored LTP.
        """
        mtm = 0
        for symbol, pos in self.active_positions.items():
            qty = pos['qty']
            entry_price = pos['price'] 
            ltp = pos.get('ltp', entry_price) 
            
            if qty > 0: # Long
                pnl = (ltp - entry_price) * qty
            else: # Short
                pnl = (entry_price - ltp) * abs(qty)
            
            mtm += pnl
        return mtm
        
    def check_risk(self):
        """
        Checks Global MTM against Risk Limits.
        """
        # Global Limit Check
        current_mtm = self.get_mtm()
        total_pnl = self.realized_pnl + current_mtm
        
        # Logging occasionally useful
        # print(f"DEBUG: PnL {total_pnl} (Realized: {self.realized_pnl}, MTM: {current_mtm})")
        
        if total_pnl >= self.daily_target:
            return "TARGET_HIT"
        elif total_pnl <= self.daily_sl:
            return "SL_HIT"
        return None

    def close_all_positions(self, reason="RISK_EXIT"):
        """
        Square off everything.
        """
        print(f"🚨 CLOSING ALL POSITIONS ({reason})...")
        active_symbols = list(self.active_positions.keys())
        
        for symbol in active_symbols:
            pos = self.active_positions[symbol]
            qty = pos['qty']
            ltp = pos.get('ltp', pos['price'])
            
            if qty > 0:
                self.place_paper_order(symbol, "SELL", qty, ltp, tag=f"EXIT_{reason}")
            elif qty < 0:
                self.place_paper_order(symbol, "BUY", abs(qty), ltp, tag=f"EXIT_{reason}")
