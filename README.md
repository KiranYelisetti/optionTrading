# Fortress Sweep Paper Trader - User Manual

## 🚀 Overview
The **Fortress Sweep** system is an automated Paper Trading bot that executes the "Sweep & Reject" strategy on Nifty and BankNifty Futures. It uses 15-minute structure (PDH/PDL/Order Blocks) and 1-minute candle confirmations to enter sniper trades.

## 🛠️ Setup & Configuration

### 1. Credentials
Ensure `fortress-paper/config.py` has your **real** Dhan credentials.
> [!IMPORTANT]
> **API Permissions**: Your Access Token MUST have "Data APIs" (Live Market Feed) enabled. If you see `HTTP 400` errors, regenerate the token.

```python
# fortress-paper/config.py
CLIENT_ID = "YOUR_CLIENT_ID" (or API Key)
ACCESS_TOKEN = "YOUR_LONG_JWT_TOKEN"
```

### 2. Daily Routine
Every morning (before 9:15 AM), run the Analyzer to generate new zones.

```bash
python fortress-paper/core/analyzer.py
```
This updates `fortress-paper/data/zones.json` with today's PDH, PDL, and Order Blocks.

### 3. Running the Trader
Start the main engine:

```bash
python fortress-paper/main.py
```
*Note: The system now checks **5-minute candles** for entry triggers to reduce noise, as per the refined strategy.*

## 🖥️ Monitoring
The bot runs in the terminal and logs to files.

- **Terminal**: Shows Real-time connection status (`✅ Live Feed Connected via v2!`) and Trade Signals (`⚡ Signal BUY_PUT_SPREAD...`).
- **Logs**: `fortress-paper/data/app.log` (Detailed system logs).
- **Trades**: `fortress-paper/data/trade_logs.csv` (Trade ledger).

## 📊 Risk Management
The bot automatically tracks Mark-to-Market (MTM) for all open positions.

- **Daily Target**: ₹1,000 (Auto-Square Off)
- **Stop Loss**: -₹750 (Auto-Square Off)

> [!NOTE]
> **Option Pricing**: MTM for Option legs currently relies on entry price due to the `subscribe_to_legs` gap. The implementation logic is ready but requires a Securities Master lookup to be fully real-time.

## 🕒 External Scheduling (Reliable Cron)
GitHub Actions Free Tier can be delayed. To run exactly on time (e.g., via [cron-job.org](https://cron-job.org)):

**API Endpoint**:
`POST https://api.github.com/repos/KiranYelisetti/optionTrading/dispatches`

**Headers**:
- `Accept`: `application/vnd.github.v3+json`
- `Authorization`: `Bearer YOUR_GITHUB_PAT` (Create a Personal Access Token with `repo` scope)
- `Content-Type`: `application/json`

**Body**:
```json
{
  "event_type": "market-monitor"
}
```

## 📁 Project Structure
- `main.py`: Core Engine (Dual Loop: Candle Check + Live Feed).
- `core/analyzer.py`: Market Structure Analysis (15m Data).
- `core/virtual_broker.py`: Paper Trading Logic & Risk Manager.
- `core/strategy.py`: "Sweep & Reject" Pattern Logic.
