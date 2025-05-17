import os
import json
import pickle
from google.oauth2 import service_account
from googleapiclient.discovery import build
import requests
from datetime import datetime

# Constants
SPREADSHEET_ID = '1sSHPrWZ1e6OnpImdzIGbaSSKaRFavw0wN622QnBbYwM'
SHEET_NAME = 'balance'
RANGE = 'A1:O3'  # Adjust range to cover all your data
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
PREVIOUS_STATE_FILE = 'previous_state.pickle'

def get_service():
    """Create and return Google Sheets service object."""
    credentials = service_account.Credentials.from_service_account_file(
        'service-account.json', scopes=SCOPES)
    return build('sheets', 'v4', credentials=credentials)

def get_sheet_data(service):
    """Fetch data from Google Sheet."""
    sheet = service.spreadsheets()
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f'{SHEET_NAME}!{RANGE}'
    ).execute()
    return result.get('values', [])

def send_space_alert(webhook_url, changes):
    """Send alert to Google Space."""
    message = "🔔 *Stock Balance Changes Detected*\n\n"
    for spec, old_val, new_val in changes:
        message += f"• {spec}: {old_val} → {new_val}\n"
    
    message += f"\n_Updated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_"
    
    payload = {
        "text": message
    }
    
    response = requests.post(webhook_url, json=payload)
    return response.status_code == 200

def load_previous_state():
    """Load previous state from file."""
    if os.path.exists(PREVIOUS_STATE_FILE):
        with open(PREVIOUS_STATE_FILE, 'rb') as f:
            return pickle.load(f)
    return None

def save_current_state(state):
    """Save current state to file."""
    with open(PREVIOUS_STATE_FILE, 'wb') as f:
        pickle.dump(state, f)

def detect_changes(previous_data, current_data):
    """Detect changes between previous and current data."""
    if not previous_data:
        return []
    
    changes = []
    # Skip header row and compare the balance row
    prev_row = previous_data[1]
    curr_row = current_data[1]
    headers = current_data[0]
    
    for i in range(len(prev_row)):
        if prev_row[i] != curr_row[i]:
            changes.append((headers[i], prev_row[i], curr_row[i]))
    
    return changes

def main():
    # Get webhook URL from environment variable
    webhook_url = os.environ.get('SPACE_WEBHOOK_URL')
    if not webhook_url:
        raise ValueError("SPACE_WEBHOOK_URL environment variable not set")

    # Initialize the Sheets API service
    service = get_service()
    
    # Get current sheet data
    current_data = get_sheet_data(service)
    
    # Load previous state
    previous_data = load_previous_state()
    
    # Detect changes
    changes = detect_changes(previous_data, current_data)
    
    # If changes detected, send alert
    if changes:
        success = send_space_alert(webhook_url, changes)
        if success:
            print("Alert sent successfully")
        else:
            print("Failed to send alert")
    else:
        print("No changes detected")
    
    # Save current state
    save_current_state(current_data)

if __name__ == '__main__':
    main() 