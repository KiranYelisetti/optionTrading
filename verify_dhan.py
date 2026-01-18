import logging
from dhanhq import dhanhq

# Configure logging
logging.basicConfig(level=logging.INFO)

CLIENT_ID = "1103466045"
TOKEN_FILE = "dhantoken.txt"

def get_access_token():
    try:
        with open(TOKEN_FILE, "r") as f:
            token = f.read().strip()
        return token
    except FileNotFoundError:
        logging.error(f"Token file {TOKEN_FILE} not found.")
        return None

def verify_authentication():
    access_token = get_access_token()
    if not access_token:
        return

    try:
        logging.info(f"Using Client ID: {CLIENT_ID}")
        dhan = dhanhq(CLIENT_ID, access_token)
        
        # Try to fetch fund limits as a simple verification
        logging.info("Fetching fund limits to verify authentication...")
        funds = dhan.get_fund_limits()
        
        if funds['status'] == 'success':
            logging.info("Authentication SUCCESSFUL!")
            logging.info(f"Funds Status: {funds['status']}")
        else:
            logging.error(f"Authentication FAILED or Error fetching funds: {funds}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    verify_authentication()
