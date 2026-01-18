import pandas as pd

class FortressStrategy:
    def __init__(self, zones_file):
        self.zones = self._load_zones(zones_file)
        self.market_sentiment_flag = "NEUTRAL" # BULLISH, BEARISH, NEUTRAL
        
    def _load_zones(self, zones_file):
        import json
        try:
            with open(zones_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"❌ Error loading zones: {e}")
            return []

    def update_market_sentiment(self, option_chain_data):
        """
        Slow Loop: Analyzes Option Chain to set Sentiment.
        """
        # Logic: 
        # Calculate PCR (Put OI / Call OI)
        # Check Change in OI for ATM + 5 strikes
        
        try:
            total_ce_oi = sum(item['oi'] for item in option_chain_data if item['option_type'] == 'CALL')
            total_pe_oi = sum(item['oi'] for item in option_chain_data if item['option_type'] == 'PUT')
            
            if total_ce_oi == 0: 
                pcr = 1.0 
            else:
                pcr = total_pe_oi / total_ce_oi
                
            # PCR Interpretation
            # > 1.0 => Bullish (More Puts written)
            # < 0.7 => Bearish (More Calls written)
            
            if pcr > 1.2:
                self.market_sentiment_flag = "BULLISH"
            elif pcr < 0.7:
                self.market_sentiment_flag = "BEARISH"
            else:
                self.market_sentiment_flag = "NEUTRAL"
                
            print(f"🧠 Strategy Update: PCR={pcr:.2f}, Sentiment={self.market_sentiment_flag}")
            
        except Exception as e:
            print(f"❌ Strategy Error: {e}")

    def get_atm_strike(self, price, symbol):
        """
        Returns ATM strike.
        NIFTY: Round to 50
        BANKNIFTY: Round to 100
        """
        if "BANKNIFTY" in symbol:
            return round(price / 100) * 100
        else:
            return round(price / 50) * 50

    def check_entry(self, candle, oi_sentiment):
        """
        Checks for Liquidity Sweep Entry on Candle Close.
        candle: {'symbol': 'NIFTY...', 'high': 26000, 'low': 25900, 'close': 25950}
        oi_sentiment: 'BULLISH', 'BEARISH', or 'NEUTRAL'
        """
        symbol = candle['symbol']
        close = candle['close']
        high = candle['high']
        low = candle['low']
        
        # Underlying Root
        underlying_root = symbol.split('-')[0] # NIFTY / BANKNIFTY
        
        for zone in self.zones:
            if zone['symbol'] != symbol:
                continue
                
            # Logic: Sweep & Reject (The Trap)
            
            # 1. Bearish Trap (Supply Zone / PDH)
            if zone['type'] == 'SUPPLY':
                level = zone['range_high'] # Key Defense Level
                # range_low = zone['range_low']
                
                # Condition: Price swept ABOVE Level, but Closed BELOW Level (or inside zone)
                # Strict Sweep: High > Level AND Close < Level
                if high > level and close < level:
                     # Check OI Confirmation
                     if oi_sentiment == "BEARISH":
                         atm = self.get_atm_strike(close, symbol)
                         return {
                             "action": "SELL_CALL_SPREAD",
                             "atm_strike": atm,
                             "underlying": underlying_root,
                             "zone_id": zone['id'],
                             "reason": "Bearish Sweep + OI Confirmed"
                         }

            # 2. Bullish Trap (Demand Zone / PDL)
            elif zone['type'] == 'DEMAND':
                level = zone['range_low'] # Key Defense Level
                
                # Condition: Price swept BELOW Level, but Closed ABOVE Level
                if low < level and close > level:
                     # Check OI Confirmation
                     if oi_sentiment == "BULLISH":
                         atm = self.get_atm_strike(close, symbol)
                         return {
                             "action": "BUY_PUT_SPREAD",
                             "atm_strike": atm,
                             "underlying": underlying_root,
                             "zone_id": zone['id'],
                             "reason": "Bullish Sweep + OI Confirmed"
                         }
                                 
        return None
