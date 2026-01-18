import os

# Dhan API Credentials
# In a real scenario, consider using environment variables for security
CLIENT_ID = "YOUR_CLIENT_ID" 
ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"

# Try to load from local file if exists (for development)
try:
    with open("dhantoken.txt", "r") as f:
        ACCESS_TOKEN = f.read().strip()
    CLIENT_ID = "1103466045" # Default from verify_dhan.py
except FileNotFoundError:
    pass

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "market_data.db")
ZONES_FILE = os.path.join(DATA_DIR, "zones.json")
TRADE_LOG_FILE = os.path.join(DATA_DIR, "trade_logs.csv") # Backup/Human readable

# Risk Management
CAPITAL = 100000
TARGET_DAILY_PERCENT = 0.01  # 1%
STOP_DAILY_PERCENT = 0.0075  # 0.75%
MAX_LOSS_PER_DAY = CAPITAL * STOP_DAILY_PERCENT
TARGET_PROFIT_PER_DAY = CAPITAL * TARGET_DAILY_PERCENT

# Strategy Settings
TIME_FRAME_HTF = "4H"
TIME_FRAME_LTF = "5m"
# VIX Filter: No trades if VIX > 18 or Spike > 5%
MAX_VIX = 18.0 
MAX_VIX_SPIKE_PERCENT = 5.0

# Scrip Master & Analysis
SCRIP_MASTER_CSV = os.path.join(DATA_DIR, "scrip_master.csv")
INDEX_SYMBOL = "NIFTY" # For Filtering

