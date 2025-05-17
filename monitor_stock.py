import os
import json
import pickle
from google.oauth2 import service_account
from googleapiclient.discovery import build
import requests
from datetime import datetime

# Constants
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')
if not SPREADSHEET_ID:
    raise ValueError("SPREADSHEET_ID environment variable not set")
    
SHEET_NAME = 'balance'
RANGE = 'A1:O3'  # Adjust range to cover all your data
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# Set up data directory for state persistence
DATA_DIR = os.path.join(os.getenv('GITHUB_WORKSPACE', os.getcwd()), '.data')
os.makedirs(DATA_DIR, exist_ok=True)
PREVIOUS_STATE_FILE = os.path.join(DATA_DIR, 'previous_state.pickle')

# Delete previous state file if it exists
if os.path.exists(PREVIOUS_STATE_FILE):
    print(f"Deleting existing state file: {PREVIOUS_STATE_FILE}")
    os.remove(PREVIOUS_STATE_FILE)

def get_service():
    """Create and return Google Sheets service object."""
    credentials = service_account.Credentials.from_service_account_file(
        'service-account.json', scopes=SCOPES)
    return build('sheets', 'v4', credentials=credentials)

def get_sheet_data(service):
    """Fetch data from Google Sheet."""
    print("Fetching data from sheet...")
    sheet = service.spreadsheets()
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f'{SHEET_NAME}!{RANGE}'
    ).execute()
    data = result.get('values', [])
    print(f"Fetched data: {data}")
    return data

def send_space_alert(webhook_url, changes=None, initial_state=None):
    """Send alert to Google Space."""
    if initial_state:
        message = "📊 *Initial Stock Balance State*\n\n"
        print(f"Preparing initial state message with data: {initial_state}")
        if len(initial_state) >= 2:  # Ensure we have headers and data
            headers = initial_state[0]
            values = initial_state[1]
            for i in range(len(headers)):
                message += f"• {headers[i]}: {values[i]}\n"
    else:
        message = "🔔 *Stock Balance Changes Detected*\n\n"
        print(f"Preparing changes message with changes: {changes}")
        for spec, old_val, new_val in changes:
            message += f"• {spec}: {old_val} → {new_val}\n"
    
    message += f"\n_Updated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_"
    
    payload = {
        "text": message
    }
    
    print(f"Sending webhook request to: {webhook_url[:20]}...")
    print(f"Payload: {payload}")
    response = requests.post(webhook_url, json=payload)
    print(f"Webhook response status: {response.status_code}")
    print(f"Webhook response text: {response.text}")
    return response.status_code == 200

def load_previous_state():
    """Load previous state from file."""
    print(f"Looking for previous state file at: {PREVIOUS_STATE_FILE}")
    if os.path.exists(PREVIOUS_STATE_FILE):
        print("Previous state file found, loading...")
        with open(PREVIOUS_STATE_FILE, 'rb') as f:
            data = pickle.load(f)
            print(f"Loaded previous state: {data}")
            return data
    print("No previous state file found")
    return None

def save_current_state(state):
    """Save current state to file."""
    print(f"Saving current state to: {PREVIOUS_STATE_FILE}")
    print(f"State to save: {state}")
    with open(PREVIOUS_STATE_FILE, 'wb') as f:
        pickle.dump(state, f)
    print("State saved successfully")

def detect_changes(previous_data, current_data):
    """Detect changes between previous and current data."""
    if not previous_data:
        print("No previous data available")
        return []
    
    changes = []
    # Skip header row and compare the balance row
    prev_row = previous_data[1]
    curr_row = current_data[1]
    headers = current_data[0]
    
    for i in range(len(prev_row)):
        if prev_row[i] != curr_row[i]:
            changes.append((headers[i], prev_row[i], curr_row[i]))
    
    print(f"Detected changes: {changes}")
    return changes

def main():
    # Get webhook URL from environment variable
    webhook_url = os.environ.get('SPACE_WEBHOOK_URL')
    if not webhook_url:
        raise ValueError("SPACE_WEBHOOK_URL environment variable not set")
    print(f"Using webhook URL: {webhook_url[:20]}...")

    # Initialize the Sheets API service
    print("Initializing Google Sheets service...")
    service = get_service()
    
    # Get current sheet data
    current_data = get_sheet_data(service)
    
    # Load previous state
    previous_data = load_previous_state()
    
    if not previous_data:
        # First run - send initial state alert
        print("No previous state found, sending initial state alert...")
        success = send_space_alert(webhook_url, initial_state=current_data)
        if success:
            print("Initial state alert sent successfully")
        else:
            print("Failed to send initial state alert")
    else:
        # Check for changes
        print("Checking for changes...")
        changes = detect_changes(previous_data, current_data)
        if changes:
            success = send_space_alert(webhook_url, changes=changes)
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