import os
import json
import pickle
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import requests
from datetime import datetime
import pytz

# Constants for Stock Balance
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')
if not SPREADSHEET_ID:
    raise ValueError("SPREADSHEET_ID environment variable not set")
    
STOCK_SHEET_NAME = 'balance'
STOCK_RANGE = 'A1:O3'  # Range covers A-O columns (Specification through TOTAL including uncategorised)

PARTS_SHEET_NAME = 'parts'
PARTS_RANGE = 'A1:H3'  # Adjust range to cover all parts data

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SERVICE_ACCOUNT_FILE = 'service-account.json'

# Set up data directory for state persistence
DATA_DIR = os.path.join(os.getenv('GITHUB_WORKSPACE', os.getcwd()), '.data')
os.makedirs(DATA_DIR, exist_ok=True)

# Separate state files for stock and parts
STOCK_STATE_FILE = os.path.join(DATA_DIR, 'previous_stock_state.pickle')
PARTS_STATE_FILE = os.path.join(DATA_DIR, 'previous_parts_state.pickle')

class APIError(Exception):
    """Custom exception for API related errors."""
    pass

def get_service():
    """Create and return Google Sheets service object."""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        return build('sheets', 'v4', credentials=credentials)
    except Exception as e:
        print(f"Error initializing Google Sheets service: {str(e)}")
        raise APIError("Failed to initialize Google Sheets service")

def get_sheet_data(service, sheet_name, range_name):
    """Fetch data from Google Sheet."""
    print(f"Fetching data from sheet {sheet_name}...")
    try:
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{sheet_name}!{range_name}'
        ).execute()
        data = result.get('values', [])
        
        # Validate data structure
        min_rows = 2 if sheet_name == STOCK_SHEET_NAME else 3
        if not data or len(data) < min_rows:
            raise APIError(f"Invalid data structure received from Google Sheets for {sheet_name}")
            
        print(f"Data fetched successfully from {sheet_name}")
        return data
    except HttpError as e:
        print(f"Google Sheets API error: {str(e)}")
        raise APIError(f"Failed to fetch data from Google Sheets for {sheet_name}")
    except Exception as e:
        print(f"Unexpected error fetching sheet data: {str(e)}")
        raise APIError(f"Unexpected error while fetching data from {sheet_name}")

def load_previous_state(state_file):
    """Load previous state from file."""
    print(f"Checking for previous state file {state_file}")
    try:
        if os.path.exists(state_file):
            print(f"Loading previous state from {state_file}")
            with open(state_file, 'rb') as f:
                data = pickle.load(f)
                min_rows = 2 if state_file == STOCK_STATE_FILE else 3
                if not data or len(data) < min_rows:
                    print("Invalid state data found, treating as no previous state")
                    return None
                print("Previous state loaded successfully")
                return data
        print("No previous state file found")
        return None
    except Exception as e:
        print(f"Error loading previous state: {str(e)}")
        return None

def save_current_state(state, state_file):
    """Save current state to file."""
    min_rows = 2 if state_file == STOCK_STATE_FILE else 3
    if not state or len(state) < min_rows:
        print("Invalid state data, skipping save")
        return
        
    print(f"Saving current state to {state_file}")
    try:
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        with open(state_file, 'wb') as f:
            pickle.dump(state, f)
        print(f"State saved successfully to {state_file}")
    except Exception as e:
        print(f"Error saving state: {str(e)}")
        raise APIError("Failed to save state file")

def detect_stock_changes(previous_data, current_data):
    """Detect changes between previous and current stock data."""
    if not previous_data:
        print("No previous stock data available")
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
            raise APIError("Data structure mismatch while comparing stock states")
        
        print("\nComparing stock states...")
        
        # Compare each value and convert to same type before comparison
        for i in range(len(prev_row)):
            # Convert both values to strings for comparison to avoid type mismatches
            prev_val = str(prev_row[i]).strip()
            curr_val = str(curr_row[i]).strip()
            
            if prev_val != curr_val:
                changes.append((headers[i], prev_row[i], curr_row[i]))
                print(f"Change detected in {headers[i]}")
        
        if changes:
            print(f"Detected {len(changes)} stock changes")
        else:
            print("No changes detected in stock balance")
        return changes
    except Exception as e:
        print(f"Error detecting stock changes: {str(e)}")
        raise APIError("Failed to compare stock states")

def detect_parts_changes(previous_data, current_data):
    """Detect changes between previous and current parts data."""
    if not previous_data:
        print("No previous parts data available")
        return []
    
    try:
        changes = []
        # Get part headers from row 2 (starting from column C which is index 2)
        part_headers = []
        if len(current_data) > 1 and len(current_data[1]) > 2:
            part_headers = current_data[1][2:]  # Skip empty cell and PARTS TYPE
        
        # Get previous values from row 1 (starting from column C which is index 2)
        prev_values = []
        if len(previous_data) > 0 and len(previous_data[0]) > 2:
            prev_values = previous_data[0][2:]  # Skip DATE and TOTAL WEIGHTS
        
        # Get current values from row 1 (starting from column C which is index 2)
        curr_values = []
        if len(current_data) > 0 and len(current_data[0]) > 2:
            curr_values = current_data[0][2:]  # Skip DATE and TOTAL WEIGHTS
        
        # Validate data structure
        if len(part_headers) != len(curr_values):
            print(f"Warning: Mismatch between parts ({len(part_headers)}) and values ({len(curr_values)})")
            # Use the shorter length for comparison
            compare_length = min(len(part_headers), len(curr_values))
            # Trim the arrays to the same length
            part_headers = part_headers[:compare_length]
            curr_values = curr_values[:compare_length]
            prev_values = prev_values[:compare_length] if len(prev_values) > compare_length else prev_values
        
        # If previous values array is shorter than current, pad it
        if len(prev_values) < len(curr_values):
            print(f"Warning: Previous values array ({len(prev_values)}) shorter than current ({len(curr_values)})")
            # Pad with empty strings
            prev_values = prev_values + [''] * (len(curr_values) - len(prev_values))
        # If previous values array is longer, trim it
        elif len(prev_values) > len(curr_values):
            print(f"Warning: Previous values array ({len(prev_values)}) longer than current ({len(curr_values)})")
            prev_values = prev_values[:len(curr_values)]
            
        print("\nComparing parts states...")
        
        # Compare each value and detect changes
        for i in range(len(part_headers)):
            if i >= len(prev_values) or i >= len(curr_values):
                print(f"Warning: Index {i} out of bounds. Skipping comparison.")
                continue
                
            # Convert both values to strings for comparison to avoid type mismatches
            prev_val = str(prev_values[i]).strip()
            curr_val = str(curr_values[i]).strip()
            
            if prev_val != curr_val:
                changes.append((part_headers[i], prev_values[i], curr_values[i]))
                print(f"Change detected in {part_headers[i]}")
        
        # Also check if total weight changed
        if len(previous_data[0]) > 1 and len(current_data[0]) > 1:
            prev_total = str(previous_data[0][1]).strip()  # TOTAL WEIGHTS
            curr_total = str(current_data[0][1]).strip()   # TOTAL WEIGHTS
            
            if prev_total != curr_total:
                changes.append(("TOTAL WEIGHTS", previous_data[0][1], current_data[0][1]))
                print("Change detected in TOTAL WEIGHTS")
        
        if changes:
            print(f"Detected {len(changes)} parts changes")
        else:
            print("No changes detected in parts weights")
        return changes
    except Exception as e:
        print(f"Error detecting parts changes: {str(e)}")
        print("Attempting to reset parts state file for next run...")
        # Save current state to recover from this error
        save_current_state(current_data, PARTS_STATE_FILE)
        print("Parts state file updated with current data. Next run should work correctly.")
        # Return empty changes to avoid further errors
        return []

def format_stock_section(stock_changes, stock_data):
    """Format the stock section of the alert message."""
    section = ""
    
    # Add stock changes if any
    if stock_changes:
        section += "*Stock Balance Changes:*\n"
        for spec, old_val, new_val in stock_changes:
            # Try to convert values to numbers and append 'pieces'
            try:
                # Use singular 'piece' if value is 1
                old_val_num = float(old_val) if str(old_val).strip().replace(',', '').isdigit() else None
                new_val_num = float(new_val) if str(new_val).strip().replace(',', '').isdigit() else None
                
                if old_val_num is not None:
                    old_suffix = " piece" if old_val_num == 1 else " pieces"
                    old_val_str = f"{old_val_num:,.0f}{old_suffix}"
                else:
                    old_val_str = str(old_val)
                    
                if new_val_num is not None:
                    new_suffix = " piece" if new_val_num == 1 else " pieces"
                    new_val_str = f"{new_val_num:,.0f}{new_suffix}"
                else:
                    new_val_str = str(new_val)
                
                section += f"• {spec}: {old_val_str} → {new_val_str}\n"
            except (ValueError, TypeError):
                section += f"• {spec}: {old_val} → {new_val}\n"
        section += "\n"
    
    # Always add current stock levels
    section += "*Current Stock Levels:*\n"
    headers = stock_data[0]
    values = stock_data[1]
    for i in range(len(headers)):
        # Skip 'Specification' header if it exists
        if headers[i].lower() != 'specification':
            try:
                # Try to convert value to number and calculate bags and pieces
                val = values[i]
                if str(val).strip().replace(',', '').isdigit():
                    total_pieces = int(float(val))
                    bags = total_pieces // 20
                    remaining_pieces = total_pieces % 20
                    
                    # Use proper singular/plural forms
                    bags_text = "1 bag" if bags == 1 else f"{bags:,} bags"
                    pieces_text = "1 piece" if remaining_pieces == 1 else f"{remaining_pieces} pieces"
                    
                    if bags > 0 and remaining_pieces > 0:
                        formatted_val = f"{bags_text}, {pieces_text}"
                    elif bags > 0:
                        formatted_val = bags_text
                    else:
                        formatted_val = pieces_text
                else:
                    formatted_val = str(val)
                section += f"• {headers[i]}: {formatted_val}\n"
            except (ValueError, TypeError):
                section += f"• {headers[i]}: {values[i]}\n"
    
    return section

def format_parts_section(parts_changes, parts_data):
    """Format the parts section of the alert message."""
    section = ""
    
    # Add parts changes if any
    if parts_changes:
        section += "*Parts Weight Changes:*\n"
        for part, old_val, new_val in parts_changes:
            # Try to convert values to numbers with weight suffix
            try:
                # Check if values are numeric
                if str(old_val).strip().replace('.', '', 1).isdigit():
                    old_val_num = float(old_val)
                    # Use "kg" for all weights as it's a unit, not a count
                    old_val_str = f"{old_val_num:,.2f} kg"
                else:
                    old_val_str = str(old_val)
                    
                if str(new_val).strip().replace('.', '', 1).isdigit():
                    new_val_num = float(new_val)
                    new_val_str = f"{new_val_num:,.2f} kg"
                else:
                    new_val_str = str(new_val)
                    
                section += f"• {part}: {old_val_str} → {new_val_str}\n"
            except (ValueError, TypeError):
                section += f"• {part}: {old_val} → {new_val}\n"
        section += "\n"
    
    # Always add current parts weights
    section += "*Current Parts Weights:*\n"
    
    # Get part headers from row 2 (starting from column C which is index 2)
    part_headers = []
    if len(parts_data) > 1 and len(parts_data[1]) > 2:
        part_headers = parts_data[1][2:]  # Skip empty cell and PARTS TYPE
    
    # Get values from row 1 (starting from column C which is index 2)
    values = []
    if len(parts_data) > 0 and len(parts_data[0]) > 2:
        values = parts_data[0][2:]  # Skip DATE and TOTAL WEIGHTS in row 1
    
    # Map values to headers
    for i in range(min(len(part_headers), len(values))):
        try:
            # Format weight values
            val = values[i]
            if str(val).strip().replace('.', '', 1).isdigit():
                # "kg" is always singular as it's a unit
                formatted_val = f"{float(val):,.2f} kg"
            else:
                formatted_val = str(val)
            section += f"• {part_headers[i]}: {formatted_val}\n"
        except (ValueError, TypeError, IndexError) as e:
            print(f"Error formatting part {i}: {str(e)}")
            section += f"• {part_headers[i] if i < len(part_headers) else 'Unknown'}: {values[i] if i < len(values) else 'N/A'}\n"
    
    # Add total weight if available
    if len(parts_data) > 0 and len(parts_data[0]) > 1:
        try:
            total_weight = parts_data[0][1]
            # Only add the total weight if it's a valid number and not a header itself
            if str(total_weight).strip().replace('.', '', 1).isdigit():
                formatted_total = f"{float(total_weight):,.2f} kg"
                section += f"• TOTAL: {formatted_total}\n"
            elif str(total_weight).lower() != "total weights":
                section += f"• TOTAL: {total_weight}\n"
        except (ValueError, TypeError, IndexError) as e:
            print(f"Error formatting total weight: {str(e)}")
    
    return section

def send_combined_alert(webhook_url, stock_changes, stock_data, parts_changes, parts_data):
    """Send combined alert to Google Space."""
    try:
        # Only proceed if there are actual changes
        if not stock_changes and not parts_changes:
            print("No changes detected in either stock or parts. No alert needed.")
            return True
        
        message = "🔔 *Inventory Changes Detected*\n\n"
        print("Preparing combined changes message")
        
        # Add stock section if there are stock changes or if parts had changes
        if stock_changes or parts_changes:
            message += format_stock_section(stock_changes, stock_data)
            message += "\n"
        
        # Add parts section if there are parts changes or if stock had changes
        if parts_changes or stock_changes:
            message += format_parts_section(parts_changes, parts_data)
        
        # Get current time in WAT
        wat_tz = pytz.timezone('Africa/Lagos')
        current_time = datetime.now(pytz.UTC).astimezone(wat_tz)
        message += f"\n_Updated at: {current_time.strftime('%Y-%m-%d %I:%M:%S %p')} WAT_"
        
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
        
        # Get current stock data
        stock_data = get_sheet_data(service, STOCK_SHEET_NAME, STOCK_RANGE)
        
        # Get current parts data
        parts_data = get_sheet_data(service, PARTS_SHEET_NAME, PARTS_RANGE)
        
        # Load previous states
        previous_stock_data = load_previous_state(STOCK_STATE_FILE)
        previous_parts_data = load_previous_state(PARTS_STATE_FILE)
        
        # Initialize flags for state updates
        stock_state_needs_update = True
        parts_state_needs_update = True
        
        # Check for changes in stock data
        stock_changes = []
        if not previous_stock_data:
            print("No previous stock state found, initializing stock state file...")
        else:
            print("Checking for stock changes...")
            stock_changes = detect_stock_changes(previous_stock_data, stock_data)
        
        # Check for changes in parts data
        parts_changes = []
        if not previous_parts_data:
            print("No previous parts state found, initializing parts state file...")
        else:
            print("Checking for parts changes...")
            parts_changes = detect_parts_changes(previous_parts_data, parts_data)
        
        # Send combined alert if there are any changes
        if stock_changes or parts_changes:
            print("Changes detected, sending combined alert...")
            if send_combined_alert(webhook_url, stock_changes, stock_data, parts_changes, parts_data):
                print("Alert sent successfully, updating state files...")
            else:
                print("Failed to send alert, but will still update state files...")
        else:
            print("No changes detected in either stock or parts, updating state files...")
        
        # Always update both state files at the end
        if stock_state_needs_update:
            save_current_state(stock_data, STOCK_STATE_FILE)
        if parts_state_needs_update:
            save_current_state(parts_data, PARTS_STATE_FILE)

    except APIError as e:
        print(f"API Error: {str(e)}")
        # Don't exit with error to avoid GitHub Actions failure
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        # Don't exit with error to avoid GitHub Actions failure

if __name__ == '__main__':
    main() 