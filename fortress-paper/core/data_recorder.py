import sqlite3
import datetime
import os
import json

class DataRecorder:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self._initialize_db()

    def _initialize_db(self):
        """Initialize SQLite database and create tables if they don't exist."""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        
        # Table for Tick Data
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME,
                symbol TEXT,
                ltp REAL,
                volume INTEGER,
                oi REAL,
                received_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Table for Option Chain Snapshots (Slow Loop)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS option_chain_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME,
                symbol TEXT,
                expiry TEXT,
                strike_price REAL,
                option_type TEXT,
                oi REAL,
                change_in_oi REAL,
                ltp REAL,
                volume INTEGER,
                iv REAL
            )
        ''')
        
        self.conn.commit()
        print(f"✅ DataRecorder connected to {self.db_path}")

    def log_tick(self, tick_data):
        """
        Log a single tick to the database.
        Expected tick_data format: {'symbol': '...', 'ltp': 100.5, 'volume': 500, 'oi': 12000, 'time': '...'}
        """
        try:
            timestamp = tick_data.get('time', datetime.datetime.now())
            symbol = tick_data.get('symbol')
            ltp = tick_data.get('ltp')
            volume = tick_data.get('volume', 0)
            oi = tick_data.get('oi', 0)

            self.cursor.execute('''
                INSERT INTO ticks (timestamp, symbol, ltp, volume, oi)
                VALUES (?, ?, ?, ?, ?)
            ''', (timestamp, symbol, ltp, volume, oi))
            
            # Simple batch commit or commit every tick? 
            # SQLite is fast, but for high freq, batching is better. 
            # For paper trading v1, auto-commit is safer to avoid data loss on crash.
            self.conn.commit() 
            
        except Exception as e:
            print(f"❌ Error logging tick: {e}")

    def log_option_chain(self, chain_data):
        """
        Log a snapshot of the option chain.
        chain_data: List of dicts representing options.
        """
        try:
            # Prepare batch insert
            data_tuples = []
            timestamp = datetime.datetime.now()
            
            for item in chain_data:
                data_tuples.append((
                    timestamp,
                    item.get('symbol'),
                    item.get('expiry'),
                    item.get('strike_price'),
                    item.get('option_type'), # CE/PE
                    item.get('oi'),
                    item.get('change_in_oi'),
                    item.get('ltp'),
                    item.get('volume'),
                    item.get('iv')
                ))

            self.cursor.executemany('''
                INSERT INTO option_chain_snapshots (timestamp, symbol, expiry, strike_price, option_type, oi, change_in_oi, ltp, volume, iv)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_tuples)
            
            self.conn.commit()
            print(f"📊 Logged {len(data_tuples)} option chain records.")
            
        except Exception as e:
            print(f"❌ Error logging option chain: {e}")

    def close(self):
        if self.conn:
            self.conn.close()
