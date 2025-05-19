import os
import json
import pickle
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import requests
from datetime import datetime

def mask_sensitive_data(data):
    """Mask sensitive data for logging."""
    if isinstance(data, list):
        return "[...]"
    if isinstance(data, str) and len(data) > 20:
        return f"{data[:3]}...{data[-3:]}"
    return data

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

class APIError(Exception):
    """Custom exception for API related errors."""
    pass

def get_service():
    """Create and return Google Sheets service object."""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            'service-account.json', scopes=SCOPES)
        return build('sheets', 'v4', credentials=credentials)
    except Exception as e:
        print(f"Error initializing Google Sheets service: {str(e)}")
        raise APIError("Failed to initialize Google Sheets service")

def get_sheet_data(service):
    """Fetch data from Google Sheet."""
    print("Fetching data from sheet...")
    try:
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{SHEET_NAME}!{RANGE}'
        ).execute()
        data = result.get('values', [])
        
        # Validate data structure
        if not data or len(data) < 2:
            raise APIError("Invalid data structure received from Google Sheets")
        
        if len(data[0]) != len(data[1]):  # Check if headers and values match
            raise APIError("Mismatch between headers and values in sheet data")
            
        print("Data fetched successfully")
        return data
    except HttpError as e:
        print(f"Google Sheets API error: {str(e)}")
        raise APIError("Failed to fetch data from Google Sheets")
    except Exception as e:
        print(f"Unexpected error fetching sheet data: {str(e)}")
        raise APIError("Unexpected error while fetching sheet data")

def send_space_alert(webhook_url, changes, current_data):
    """Send alert to Google Space."""
    try:
        message = "🔔 *Stock Balance Changes Detected*\n\n"
        print("Preparing changes message")
        message += "*Changes:*\n"
        for spec, old_val, new_val in changes:
            message += f"• {spec}: {old_val} → {new_val}\n"
        
        message += "\n*Current Stock Levels:*\n"
        headers = current_data[0]
        values = current_data[1]
        for i in range(len(headers)):
            # Skip 'Specification' header if it exists
            if headers[i].lower() != 'specification':
                message += f"• {headers[i]}: {values[i]}\n"
        
        message += f"\n_Updated at: {datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')}_"
        
        payload = {
            "text": message
        }
        
        print("Sending webhook request...")
        response = requests.post(webhook_url, json=payload, timeout=10)  # Add timeout
        response.raise_for_status()  # Raise exception for bad status codes
        print(f"Webhook response status: {response.status_code}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error sending alert to Google Space: {str(e)}")
        return False

def load_previous_state():
    """Load previous state from file."""
    print("Checking for previous state file")
    try:
        if os.path.exists(PREVIOUS_STATE_FILE):
            print(f"Loading previous state from {PREVIOUS_STATE_FILE}")
            with open(PREVIOUS_STATE_FILE, 'rb') as f:
                data = pickle.load(f)
                if not data or len(data) < 2:
                    print("Invalid state data found, treating as no previous state")
                    return None
                print(f"Previous state loaded successfully: {data[1]}")  # Print the actual values
                return data
        print("No previous state file found")
        return None
    except Exception as e:
        print(f"Error loading previous state: {str(e)}")
        return None

def save_current_state(state):
    """Save current state to file."""
    if not state or len(state) < 2:
        print("Invalid state data, skipping save")
        return
        
    print(f"Saving current state: {state[1]}")  # Print the values being saved
    try:
        os.makedirs(os.path.dirname(PREVIOUS_STATE_FILE), exist_ok=True)
        with open(PREVIOUS_STATE_FILE, 'wb') as f:
            pickle.dump(state, f)
        print(f"State saved successfully to {PREVIOUS_STATE_FILE}")
    except Exception as e:
        print(f"Error saving state: {str(e)}")
        raise APIError("Failed to save state file")

def detect_changes(previous_data, current_data):
    """Detect changes between previous and current data."""
    if not previous_data:
        print("No previous data available")
        return []
    
    try:
        changes = []
        # Skip header row and compare the balance row
        prev_row = previous_data[1]
        curr_row = current_data[1]
        headers = current_data[0]
        
        # Validate data lengths
        if len(prev_row) != len(curr_row) or len(headers) != len(curr_row):
            print(f"Data length mismatch - Previous: {len(prev_row)}, Current: {len(curr_row)}, Headers: {len(headers)}")
            raise APIError("Data structure mismatch while comparing states")
        
        print("\nComparing states:")
        print(f"Previous state: {prev_row}")
        print(f"Current state:  {curr_row}\n")
        
        # Compare each value and convert to same type before comparison
        for i in range(len(prev_row)):
            # Convert both values to strings for comparison to avoid type mismatches
            prev_val = str(prev_row[i]).strip()
            curr_val = str(curr_row[i]).strip()
            
            if prev_val != curr_val:
                changes.append((headers[i], prev_row[i], curr_row[i]))
                print(f"Change detected in {headers[i]}: {prev_val} → {curr_val}")
        
        if changes:
            print(f"Detected {len(changes)} changes")
        else:
            print("No changes detected in stock balance")
        return changes
    except Exception as e:
        print(f"Error detecting changes: {str(e)}")
        raise APIError("Failed to compare states")

def main():
    try:
        # Get webhook URL from environment variable
        webhook_url = os.environ.get('SPACE_WEBHOOK_URL')
        if not webhook_url:
            raise ValueError("SPACE_WEBHOOK_URL environment variable not set")
        print("Webhook URL configured")

        # Initialize the Sheets API service
        print("Initializing Google Sheets service...")
        service = get_service()
        
        # Get current sheet data
        current_data = get_sheet_data(service)
        
        # Load previous state
        previous_data = load_previous_state()
        
        if not previous_data:
            # First run - just save the initial state without sending alert
            print("No previous state found, initializing state file...")
            save_current_state(current_data)
            print("Initial state saved successfully")
        else:
            # Check for changes
            print("Checking for changes...")
            changes = detect_changes(previous_data, current_data)
            if changes:
                print("Changes detected, sending alert...")
                if send_space_alert(webhook_url, changes=changes, current_data=current_data):
                    save_current_state(current_data)
                else:
                    raise APIError("Failed to send change alert")
            else:
                print("No changes detected, updating state file...")
                save_current_state(current_data)

    except APIError as e:
        print(f"API Error: {str(e)}")
        # Don't update state file on API errors to maintain consistency
        exit(1)
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        exit(1)

if __name__ == '__main__':
    main() 