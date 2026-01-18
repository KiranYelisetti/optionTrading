import requests
import logging
from config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

def send_telegram_alert(message):
    """
    Sends a message to the configured Telegram Chat.
    """
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("⚠️ Telegram credentials missing. Skipping alert.")
        return

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown"
        }
        res = requests.post(url, json=payload, timeout=10)
        res.raise_for_status()
        logging.info("📢 Telegram Alert Sent successfully.")
    except Exception as e:
        logging.error(f"❌ Failed to send Telegram alert: {e}")
