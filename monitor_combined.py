import os
import json
import pickle
import random
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import requests
from datetime import datetime
import pytz
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, retry_if_exception
import pybreaker
from cryptography.fernet import Fernet
import subprocess

# Constants for Stock Balance
SPECIFICATION_SHEET_ID = os.environ.get('SPECIFICATION_SHEET_ID')
INVENTORY_SHEET_ID = os.environ.get('INVENTORY_ETL_SPREADSHEET_ID')
if not INVENTORY_SHEET_ID:
    raise ValueError("INVENTORY_ETL_SPREADSHEET_ID environment variable not set")
if not SPECIFICATION_SHEET_ID:
    raise ValueError("SPECIFICATION_SHEET_ID environment variable not set")
    
STOCK_SHEET_NAME = 'Balance'
STOCK_RANGE = 'A1:EX5'  # Range covers A-EX columns (154 columns) with 5 rows for multi-row headers

INVENTORY_SHEET_NAME = 'summary'  # The sheet name from the inventory tracking spreadsheet
INVENTORY_RANGE = 'A:BZ'  # Get all columns since we're finding them by name (extends beyond Z for 53+ columns)

# Parts are now included in the Balance sheet (Wings, Laps, Breast, Fillet, Bones)
# No separate parts sheet needed

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SERVICE_ACCOUNT_FILE = 'service-account.json'

# Baseline stock count values (2-Jan-2026)
# Read from environment variables (GitHub secrets) or fall back to local config
def load_baseline_config():
    """Load baseline values from env vars or local config file."""
    baseline = {
        'wc_qty': os.environ.get('BASELINE_WC_QTY'),
        'wc_weight': os.environ.get('BASELINE_WC_WEIGHT'),
        'gizzard_packs': os.environ.get('BASELINE_GIZZARD_PACKS'),
        'gizzard_weight': os.environ.get('BASELINE_GIZZARD_WEIGHT')
    }

    # If env vars not set, try to load from local config file
    if not all(baseline.values()):
        config_file = os.path.join(os.path.dirname(__file__), 'baseline_config.json')
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                config = json.load(f)
                baseline['wc_qty'] = baseline['wc_qty'] or config.get('BASELINE_WC_QTY')
                baseline['wc_weight'] = baseline['wc_weight'] or config.get('BASELINE_WC_WEIGHT')
                baseline['gizzard_packs'] = baseline['gizzard_packs'] or config.get('BASELINE_GIZZARD_PACKS')
                baseline['gizzard_weight'] = baseline['gizzard_weight'] or config.get('BASELINE_GIZZARD_WEIGHT')

    return {
        'wc_qty': float(baseline['wc_qty']) if baseline['wc_qty'] else 0,
        'wc_weight': float(baseline['wc_weight']) if baseline['wc_weight'] else 0,
        'gizzard_packs': float(baseline['gizzard_packs']) if baseline['gizzard_packs'] else 0,
        'gizzard_weight': float(baseline['gizzard_weight']) if baseline['gizzard_weight'] else 0
    }

BASELINE = load_baseline_config()
BASELINE_WC_QTY = BASELINE['wc_qty']
BASELINE_WC_WEIGHT = BASELINE['wc_weight']
BASELINE_GIZZARD_PACKS = BASELINE['gizzard_packs']
BASELINE_GIZZARD_WEIGHT = BASELINE['gizzard_weight']

# Set up data directory for state persistence
DATA_DIR = os.getenv('GITHUB_WORKSPACE', os.getcwd())
ENCRYPTED_STATES_DIR = os.path.join(DATA_DIR, 'encrypted_states')
os.makedirs(ENCRYPTED_STATES_DIR, exist_ok=True)

# Encrypted state files
BALANCE_STATE_FILE = os.path.join(ENCRYPTED_STATES_DIR, 'balance_state.enc')  # Combined stock and parts
WHOLE_CHICKEN_DIFF_STATE_FILE = os.path.join(ENCRYPTED_STATES_DIR, 'whole_chicken_diff_state.enc')
GIZZARD_PACKS_DIFF_STATE_FILE = os.path.join(ENCRYPTED_STATES_DIR, 'gizzard_packs_diff_state.enc')
GIZZARD_WEIGHT_DIFF_STATE_FILE = os.path.join(ENCRYPTED_STATES_DIR, 'gizzard_weight_diff_state.enc')
FAILED_WEBHOOKS_FILE = os.path.join(ENCRYPTED_STATES_DIR, 'failed_webhooks.enc')
STATE_READ_FAILURE_ALERT_FILE = os.path.join(ENCRYPTED_STATES_DIR, 'state_read_failure_alert.json')

# Circuit breaker for webhook calls
webhook_circuit_breaker = pybreaker.CircuitBreaker(
    fail_max=5,           # Open circuit after 5 consecutive failures
    reset_timeout=60,     # Try again after 60 seconds
    exclude=[requests.exceptions.HTTPError]  # Don't count 4xx errors as failures
)

# Encryption/Decryption Functions
def get_encryption_key():
    """Get the encryption key from environment variable."""
    key = os.environ.get('STATE_ENCRYPTION_KEY')
    if not key:
        raise ValueError("STATE_ENCRYPTION_KEY environment variable not set")
    return key.encode()

def encrypt_state_data(data):
    """Encrypt state data using Fernet."""
    try:
        key = get_encryption_key()
        fernet = Fernet(key)
        serialized_data = pickle.dumps(data)
        encrypted_data = fernet.encrypt(serialized_data)
        return encrypted_data
    except Exception as e:
        print(f"Error encrypting state data: {str(e)}")
        raise

def decrypt_state_data(encrypted_data):
    """Decrypt state data using Fernet."""
    try:
        key = get_encryption_key()
        fernet = Fernet(key)
        decrypted_data = fernet.decrypt(encrypted_data)
        data = pickle.loads(decrypted_data)
        return data
    except Exception as e:
        print(f"Error decrypting state data: {str(e)}")
        raise

def save_state_read_failure_alert(failed_files, error_message):
    """Save state read failure alert for email notification."""
    try:
        alert_data = {
            'timestamp': datetime.now(pytz.UTC).astimezone(pytz.timezone('Africa/Lagos')).isoformat(),
            'event': 'state_decryption_failed',
            'failed_files': failed_files,
            'error_message': str(error_message),
            'run_id': os.environ.get('GITHUB_RUN_NUMBER', 'unknown'),
            'action_required': 'Check STATE_ENCRYPTION_KEY secret and encrypted state files'
        }

        with open(STATE_READ_FAILURE_ALERT_FILE, 'w', encoding='utf-8') as f:
            json.dump(alert_data, f, ensure_ascii=False, indent=2)

        print(f"State read failure alert saved to: {STATE_READ_FAILURE_ALERT_FILE}")
        return True
    except Exception as e:
        print(f"Error saving state read failure alert: {str(e)}")
        return False

def commit_encrypted_state_files():
    """Commit encrypted state files to repository."""
    try:
        # Configure git
        subprocess.run(['git', 'config', 'user.name', 'github-actions[bot]'],
                      cwd=DATA_DIR, check=True)
        subprocess.run(['git', 'config', 'user.email', 'github-actions[bot]@users.noreply.github.com'],
                      cwd=DATA_DIR, check=True)

        # Force add encrypted state files (despite being in .gitignore)
        subprocess.run(['git', 'add', '-f', 'encrypted_states/'], cwd=DATA_DIR, check=True)

        # Check if there are changes to commit
        result = subprocess.run(['git', 'diff', '--cached', '--exit-code'],
                               cwd=DATA_DIR, capture_output=True)

        if result.returncode != 0:  # There are changes to commit
            commit_message = f"Update encrypted state files - Run {os.environ.get('GITHUB_RUN_NUMBER', 'unknown')}"
            subprocess.run(['git', 'commit', '-m', commit_message], cwd=DATA_DIR, check=True)
            subprocess.run(['git', 'push'], cwd=DATA_DIR, check=True)
            print("Encrypted state files committed and pushed successfully")
        else:
            print("No changes to encrypted state files - nothing to commit")

        return True
    except subprocess.CalledProcessError as e:
        print(f"Error committing encrypted state files: {str(e)}")
        return False
    except Exception as e:
        print(f"Unexpected error committing encrypted state files: {str(e)}")
        return False

class APIError(Exception):
    """Custom exception for API related errors."""
    pass

def parse_balance_data(data):
    """
    Parse the multi-row header structure from the Balance sheet.

    Structure:
    - Row 0 (index 0): Product names (WHOLE CHICKEN - BELOW 1KG, GIZZARD, WINGS, etc.)
    - Row 1 (index 1): "TOTAL" labels
    - Row 2 (index 2): Grade names (Grade A, Grade B, Grade C, Grade D)
    - Row 3 (index 3): Balance data (the actual numbers)
    - Row 4 (index 4): Metric names (Qty, Weight(kg), Packs)

    Returns: List of column objects with structure:
    [{
        'col_index': int,
        'product': str,
        'grade': str,
        'metric': str,
        'value': str
    }, ...]
    """
    if not data or len(data) < 5:
        return []

    row_product = data[0]  # Row 1: Product names
    row_total = data[1]    # Row 2: TOTAL labels
    row_grade = data[2]    # Row 3: Grade names
    row_data = data[3]     # Row 4: Balance data
    row_metric = data[4]   # Row 5: Metric names

    parsed_columns = []
    current_product = ""
    current_grade = ""

    # Iterate over the longest row (usually row_data or row_metric)
    max_cols = max(len(row_product), len(row_grade), len(row_data), len(row_metric))

    for i in range(max_cols):
        # Track current product
        if i < len(row_product) and row_product[i] and row_product[i].strip():
            current_product = row_product[i].strip()

        # Skip DATE and NOTES columns
        if current_product in ['DATE', 'NOTES']:
            continue

        # Track current grade (grade cells span multiple columns, so we need to remember)
        grade_cell = row_grade[i].strip() if i < len(row_grade) and row_grade[i] else ""
        if grade_cell:
            current_grade = grade_cell

        # Get metric
        metric = row_metric[i].strip() if i < len(row_metric) and row_metric[i] else ""
        value = row_data[i].strip() if i < len(row_data) and row_data[i] else "0"

        # Include columns that have product, grade (current), and metric
        if current_product and current_grade and metric:
            parsed_columns.append({
                'col_index': i,
                'product': current_product,
                'grade': current_grade,
                'metric': metric,
                'value': value
            })

    return parsed_columns

def get_product_categories(data):
    """
    Get list of unique product categories from Balance sheet.

    Returns: Dict with product categories and their column ranges:
    {
        'WHOLE CHICKEN': {
            'weights': ['BELOW 1KG', '1KG', '1.1KG', ...],
            'col_start': int,
            'col_end': int
        },
        'GIZZARD': {...},
        'WINGS': {...},
        ...
    }
    """
    if not data or len(data) < 1:
        return {}

    row_product = data[0]
    products = {}
    current_product = ""
    product_start = 0

    for i in range(len(row_product)):
        if row_product[i] and row_product[i].strip():
            product_name = row_product[i].strip()

            # Skip DATE and NOTES
            if product_name in ['DATE', 'NOTES']:
                continue

            # Save previous product if exists
            if current_product and current_product not in products:
                products[current_product] = {
                    'col_start': product_start,
                    'col_end': i - 1
                }

            # Start tracking new product
            current_product = product_name
            product_start = i

    # Save the last product
    if current_product:
        products[current_product] = {
            'col_start': product_start,
            'col_end': len(row_product) - 1
        }

    # Separate whole chicken weights from product name
    whole_chicken_products = {}
    other_products = {}

    for product, info in products.items():
        if 'WHOLE CHICKEN' in product:
            # Extract weight from product name
            weight = product.replace('WHOLE CHICKEN - ', '').strip()
            if 'WHOLE CHICKEN' not in whole_chicken_products:
                whole_chicken_products['WHOLE CHICKEN'] = {'weights': [], 'ranges': {}}
            whole_chicken_products['WHOLE CHICKEN']['weights'].append(weight)
            whole_chicken_products['WHOLE CHICKEN']['ranges'][weight] = info
        else:
            other_products[product] = info

    # Combine results
    result = {}
    if whole_chicken_products:
        result.update(whole_chicken_products)
    result.update(other_products)

    return result

def is_rate_limit_error(exception):
    """Check if the exception is a rate limit error"""
    if isinstance(exception, HttpError):
        return exception.resp.status in [429, 500, 502, 503]
    if isinstance(exception, Exception):
        error_str = str(exception).lower()
        return any(term in error_str for term in ['quota', 'rate limit', 'too many requests', '429'])
    return False

def should_retry_webhook(exception):
    """Determine if webhook should be retried based on error type"""
    if isinstance(exception, requests.exceptions.HTTPError):
        # Don't retry 4xx client errors (permanent failures)
        if 400 <= exception.response.status_code < 500:
            return False
        # Retry 5xx server errors (temporary failures)
        return 500 <= exception.response.status_code < 600
    # Retry network errors, timeouts, etc.
    if isinstance(exception, (requests.exceptions.ConnectionError,
                             requests.exceptions.Timeout,
                             requests.exceptions.RequestException)):
        return True
    return False

def save_failed_webhook(payload, error_msg, webhook_url):
    """Save failed webhook to encrypted dead letter queue for manual review"""
    try:
        failed_webhook = {
            'payload': payload,
            'webhook_url': webhook_url,
            'error': str(error_msg),
            'timestamp': datetime.now(pytz.UTC).astimezone(pytz.timezone('Africa/Lagos')).isoformat(),
            'attempts': 5,
            'status': 'failed'
        }

        # Load existing failed webhooks or start with empty list
        existing_webhooks = []
        if os.path.exists(FAILED_WEBHOOKS_FILE):
            try:
                with open(FAILED_WEBHOOKS_FILE, 'rb') as f:
                    encrypted_data = f.read()
                    existing_webhooks = decrypt_state_data(encrypted_data)
                    if not isinstance(existing_webhooks, list):
                        existing_webhooks = []
            except Exception as e:
                print(f"Warning: Could not load existing failed webhooks: {str(e)}")
                existing_webhooks = []

        # Append new failed webhook
        existing_webhooks.append(failed_webhook)

        # Encrypt and save updated list
        encrypted_data = encrypt_state_data(existing_webhooks)
        os.makedirs(os.path.dirname(FAILED_WEBHOOKS_FILE), exist_ok=True)
        with open(FAILED_WEBHOOKS_FILE, 'wb') as f:
            f.write(encrypted_data)

        print(f"Failed webhook saved to encrypted dead letter queue: {FAILED_WEBHOOKS_FILE}")
        return True
    except Exception as e:
        print(f"Error saving failed webhook to encrypted dead letter queue: {str(e)}")
        return False

def check_failed_webhooks():
    """Check and report on failed webhooks in the encrypted dead letter queue"""
    try:
        if not os.path.exists(FAILED_WEBHOOKS_FILE):
            print("✅ No failed webhooks in queue")
            return

        # Load and decrypt failed webhooks
        with open(FAILED_WEBHOOKS_FILE, 'rb') as f:
            encrypted_data = f.read()
            failed_webhooks = decrypt_state_data(encrypted_data)

        if isinstance(failed_webhooks, list) and len(failed_webhooks) > 0:
            failed_count = len(failed_webhooks)
            print(f"⚠️  Warning: {failed_count} failed webhooks found in encrypted dead letter queue")
            print(f"Review failed webhooks at: {FAILED_WEBHOOKS_FILE}")
        else:
            print("✅ No failed webhooks in queue")

    except Exception as e:
        print(f"Error checking encrypted failed webhooks: {str(e)}")
        # Save alert for state read failure if decryption fails
        save_state_read_failure_alert(['failed_webhooks.enc'], str(e))


def clear_failed_webhooks():
    """Remove failed webhooks file after successful delivery"""
    try:
        if os.path.exists(FAILED_WEBHOOKS_FILE):
            os.remove(FAILED_WEBHOOKS_FILE)
            print("Cleared failed webhook queue")
    except Exception as e:
        print(f"Error clearing failed webhook queue: {str(e)}")


@retry(
    retry=retry_if_exception_type((HttpError, Exception)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    before_sleep=lambda retry_state: print(f"Rate limit hit, retrying in {retry_state.next_action.sleep} seconds... (attempt {retry_state.attempt_number})")
)
def robust_api_call(api_func, *args, **kwargs):
    """Execute API call with robust retry logic"""
    try:
        return api_func(*args, **kwargs)
    except Exception as e:
        if is_rate_limit_error(e):
            # Add jitter to prevent thundering herd
            time.sleep(random.uniform(0.5, 2.0))
        raise

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
        def _fetch_data():
            sheet = service.spreadsheets()
            result = sheet.values().get(
                spreadsheetId=SPECIFICATION_SHEET_ID,
                range=f'{sheet_name}!{range_name}'
            ).execute()
            return result.get('values', [])

        data = robust_api_call(_fetch_data)

        # Validate data structure for multi-row headers (5 rows: product, total, grade, data, metric)
        min_rows = 5
        if not data or len(data) < min_rows:
            raise APIError(f"Invalid data structure received from Google Sheets for {sheet_name} (expected {min_rows} rows, got {len(data) if data else 0})")

        print(f"Data fetched successfully from {sheet_name} ({len(data)} rows, {len(data[0]) if data else 0} columns)")
        return data
    except HttpError as e:
        print(f"Google Sheets API error: {str(e)}")
        raise APIError(f"Failed to fetch data from Google Sheets for {sheet_name}")
    except Exception as e:
        print(f"Unexpected error fetching sheet data: {str(e)}")
        raise APIError(f"Unexpected error while fetching data from {sheet_name}")

def load_previous_state(state_file):
    """Load previous state from encrypted file."""
    print(f"Checking for previous encrypted state file {state_file}")
    try:
        if os.path.exists(state_file):
            print(f"Loading and decrypting previous state from {state_file}")
            with open(state_file, 'rb') as f:
                encrypted_data = f.read()
                data = decrypt_state_data(encrypted_data)

                # Check if this is a difference state file (contains single value)
                if 'diff_state' in state_file:
                    # Difference state files contain single numeric values
                    if not isinstance(data, (int, float)) and data is not None:
                        print("Invalid difference state data found, treating as no previous state")
                        return None
                else:
                    # Balance state file expects 5 rows (multi-row header structure)
                    min_rows = 5
                    if not data or len(data) < min_rows:
                        print(f"Invalid state data found (expected {min_rows} rows, got {len(data) if data else 0}), treating as no previous state")
                        return None
                print("Previous state loaded and decrypted successfully")
                return data
        print("No previous encrypted state file found")
        return None
    except Exception as e:
        print(f"Error loading/decrypting previous state: {str(e)}")
        # Save alert for state read failure
        filename = os.path.basename(state_file)
        save_state_read_failure_alert([filename], str(e))
        return None

def save_current_state(state, state_file):
    """Save current state to encrypted file."""
    # Check if this is a difference state file (contains single value)
    if 'diff_state' in state_file:
        # Difference state files contain single numeric values
        if not isinstance(state, (int, float)) and state is not None:
            print("Invalid difference state data, skipping save")
            return
    else:
        # Balance state file expects 5 rows (multi-row header structure)
        min_rows = 5
        if not state or len(state) < min_rows:
            print(f"Invalid state data (expected {min_rows} rows, got {len(state) if state else 0}), skipping save")
            return

    print(f"Encrypting and saving current state to {state_file}")
    try:
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        encrypted_data = encrypt_state_data(state)
        with open(state_file, 'wb') as f:
            f.write(encrypted_data)
        print(f"State encrypted and saved successfully to {state_file}")
    except Exception as e:
        print(f"Error encrypting/saving state: {str(e)}")
        raise APIError("Failed to save encrypted state file")

def detect_balance_changes(previous_data, current_data):
    """
    Detect changes between previous and current balance data (stock and parts combined).
    Returns structured changes with product, grade, and metric information.
    """
    if not previous_data:
        print("No previous balance data available")
        return []

    try:
        # Validate data structure
        if len(previous_data) < 5 or len(current_data) < 5:
            print(f"Invalid data structure - Previous: {len(previous_data)} rows, Current: {len(current_data)} rows")
            print("Resetting previous balance state file to match new structure.")
            save_current_state(current_data, BALANCE_STATE_FILE)
            return []

        # Check if column count changed significantly (schema change)
        prev_col_count = len(previous_data[0]) if previous_data else 0
        curr_col_count = len(current_data[0]) if current_data else 0

        if abs(prev_col_count - curr_col_count) > 10:  # Allow small differences, but reset on major changes
            print(f"Schema change detected - Previous: {prev_col_count} columns, Current: {curr_col_count} columns")
            print("Resetting balance state file to match new structure.")
            save_current_state(current_data, BALANCE_STATE_FILE)
            return []

        print("\nComparing balance states...")

        # Parse both datasets
        prev_columns = parse_balance_data(previous_data)
        curr_columns = parse_balance_data(current_data)

        # Create dictionaries for easy comparison
        prev_dict = {}
        for col in prev_columns:
            key = f"{col['product']}|{col['grade']}|{col['metric']}"
            prev_dict[key] = col['value']

        curr_dict = {}
        for col in curr_columns:
            key = f"{col['product']}|{col['grade']}|{col['metric']}"
            curr_dict[key] = col['value']

        # Detect changes
        changes = []
        for key in curr_dict:
            curr_val = str(curr_dict[key]).strip()
            prev_val = str(prev_dict.get(key, "")).strip()

            if prev_val != curr_val:
                # Parse key back into components
                product, grade, metric = key.split('|')
                
                # Skip Weight(kg) changes for whole chicken (weight is calculated from qty)
                if 'WHOLE CHICKEN' in product and metric == 'Weight(kg)':
                    continue
                
                changes.append({
                    'product': product,
                    'grade': grade,
                    'metric': metric,
                    'old_value': prev_val,
                    'new_value': curr_val
                })
                print(f"Change detected: {product} - {grade} - {metric}: {prev_val} → {curr_val}")

        if changes:
            print(f"Detected {len(changes)} balance changes")
        else:
            print("No changes detected in balance")

        return changes
    except Exception as e:
        print(f"Error detecting balance changes: {str(e)}")
        raise APIError("Failed to compare balance states")

# Old detect_parts_changes function removed - parts are now handled by detect_balance_changes

def detect_chicken_difference_changes(previous_chicken_diff, current_chicken_diff):
    """Detect changes between previous and current whole chicken inventory balance difference."""
    if previous_chicken_diff is None:
        print("No previous whole chicken difference data available")
        return []
    
    try:
        changes = []
        
        print("\nComparing whole chicken difference states...")
        
        if current_chicken_diff is not None and previous_chicken_diff != current_chicken_diff:
            changes.append(('Whole Chicken Balance Difference', previous_chicken_diff, current_chicken_diff))
            print(f"Change detected in Whole Chicken Balance Difference")
        
        if changes:
            print(f"Detected {len(changes)} whole chicken difference changes")
        else:
            print("No changes detected in whole chicken inventory balance difference")
        return changes
    except Exception as e:
        print(f"Error detecting whole chicken difference changes: {str(e)}")
        raise APIError("Failed to compare whole chicken difference states")

def detect_gizzard_difference_changes(previous_gizzard_packs_diff, current_gizzard_packs_diff,
                                     previous_gizzard_weight_diff, current_gizzard_weight_diff):
    """Detect changes between previous and current gizzard inventory balance differences.
    Handles both packs and weight differences.
    Returns: List of tuples (change_type, old_value, new_value)
    """
    changes = []

    try:
        print("\nComparing gizzard difference states...")

        # Check packs difference changes
        if previous_gizzard_packs_diff is not None and current_gizzard_packs_diff is not None:
            # Use small tolerance for floating point comparison
            if abs(previous_gizzard_packs_diff - current_gizzard_packs_diff) >= 0.01:
                changes.append(('Gizzard Packs Balance Difference', previous_gizzard_packs_diff, current_gizzard_packs_diff))
                print(f"Change detected in Gizzard Packs Balance Difference")

        # Check weight difference changes
        if previous_gizzard_weight_diff is not None and current_gizzard_weight_diff is not None:
            # Use small tolerance for floating point comparison
            if abs(previous_gizzard_weight_diff - current_gizzard_weight_diff) >= 0.01:
                changes.append(('Gizzard Weight Balance Difference', previous_gizzard_weight_diff, current_gizzard_weight_diff))
                print(f"Change detected in Gizzard Weight Balance Difference")

        if changes:
            print(f"Detected {len(changes)} gizzard difference changes")
        else:
            print("No changes detected in gizzard inventory balance differences")
        return changes
    except Exception as e:
        print(f"Error detecting gizzard difference changes: {str(e)}")
        raise APIError("Failed to compare gizzard difference states")

def get_inventory_balance(service):
    """Fetch and calculate inventory balance from the inflow/release sheet."""
    try:
        def _fetch_inventory_data():
            result = service.spreadsheets().values().get(
                spreadsheetId=INVENTORY_SHEET_ID,
                range=f'{INVENTORY_SHEET_NAME}!{INVENTORY_RANGE}'
            ).execute()
            return result.get('values', [])
        
        data = robust_api_call(_fetch_inventory_data)
        if not data:
            print("No data found in inventory sheet, using baseline")
            return BASELINE_WC_QTY if BASELINE_WC_QTY > 0 else None

        # Get the header row to find the column indices
        if len(data) < 2:  # Need at least header row and one data row
            print("Not enough rows in inventory sheet, using baseline")
            return BASELINE_WC_QTY if BASELINE_WC_QTY > 0 else None
            
        headers = data[0]
        try:
            balance_col_index = headers.index('whole_chicken_quantity_stock_balance')
            year_month_col_index = headers.index('year_month')
        except ValueError as e:
            print(f"Could not find required column in inventory sheet: {str(e)}")
            return None
            
        # Get current year-month in YYYY-MM format
        current_date = datetime.now(pytz.UTC).astimezone(pytz.timezone('Africa/Lagos'))
        current_year_month = current_date.strftime('%Y-%m')
        
        # Find the row for the current month
        data_rows = data[1:]  # Skip header row
        current_month_row = None
        
        for row in data_rows:
            if len(row) > year_month_col_index and row[year_month_col_index] == current_year_month:
                current_month_row = row
                break
        
        if not current_month_row:
            print(f"Warning: No data found for current month ({current_year_month})")
            # Sort by year_month in descending order to get the most recent record as fallback
            sorted_data = sorted(data_rows, 
                               key=lambda x: x[year_month_col_index] if len(x) > year_month_col_index else '', 
                               reverse=True)
            if sorted_data:
                current_month_row = sorted_data[0]
                print(f"Using most recent available data from {current_month_row[year_month_col_index]}")
            else:
                print("No data rows found, using baseline")
                return BASELINE_WC_QTY if BASELINE_WC_QTY > 0 else None

        if len(current_month_row) > balance_col_index:
            try:
                balance = float(current_month_row[balance_col_index])
                # Add baseline stock count to the ETL balance
                balance = balance + BASELINE_WC_QTY
                return balance
            except (ValueError, TypeError):
                print("Invalid balance value in inventory sheet")
                # Return baseline if ETL value is invalid
                return BASELINE_WC_QTY if BASELINE_WC_QTY > 0 else None
        # Return baseline if no balance column data
        return BASELINE_WC_QTY if BASELINE_WC_QTY > 0 else None
    except Exception as e:
        print(f"Error fetching inventory balance: {str(e)}")
        # Return baseline on error so comparison can still work
        return BASELINE_WC_QTY if BASELINE_WC_QTY > 0 else None

def get_gizzard_inventory_balance(service):
    """Fetch gizzard packs and weight balance from the inventory sheet.
    Returns: Tuple of (packs_balance, weight_balance)
    """
    try:
        def _fetch_gizzard_data():
            result = service.spreadsheets().values().get(
                spreadsheetId=INVENTORY_SHEET_ID,
                range=f'{INVENTORY_SHEET_NAME}!{INVENTORY_RANGE}'
            ).execute()
            return result.get('values', [])

        data = robust_api_call(_fetch_gizzard_data)
        if not data:
            print("No data found in inventory sheet for gizzard, using baseline")
            return BASELINE_GIZZARD_PACKS, BASELINE_GIZZARD_WEIGHT

        # Get the header row to find the column indices
        if len(data) < 2:  # Need at least header row and one data row
            print("Not enough rows in inventory sheet for gizzard, using baseline")
            return BASELINE_GIZZARD_PACKS, BASELINE_GIZZARD_WEIGHT

        headers = data[0]

        # Try to find both packs and weight columns
        gizzard_packs_col = None
        gizzard_weight_col = None
        year_month_col = None

        try:
            year_month_col = headers.index('year_month')
        except ValueError:
            print("Could not find year_month column in inventory sheet, using baseline")
            return BASELINE_GIZZARD_PACKS, BASELINE_GIZZARD_WEIGHT

        # Check for gizzard quantity/packs column
        try:
            gizzard_packs_col = headers.index('gizzard_quantity_stock_balance')
            print("Found gizzard_quantity_stock_balance column")
        except ValueError:
            print("gizzard_quantity_stock_balance column not found (this is OK if not tracked)")

        # Check for gizzard weight column
        try:
            gizzard_weight_col = headers.index('gizzard_weight_stock_balance')
            print("Found gizzard_weight_stock_balance column")
        except ValueError:
            print("Warning: gizzard_weight_stock_balance column not found")

        # Get current year-month in YYYY-MM format
        current_date = datetime.now(pytz.UTC).astimezone(pytz.timezone('Africa/Lagos'))
        current_year_month = current_date.strftime('%Y-%m')

        # Find the row for the current month
        data_rows = data[1:]  # Skip header row
        current_month_row = None

        for row in data_rows:
            if len(row) > year_month_col and row[year_month_col] == current_year_month:
                current_month_row = row
                break

        if not current_month_row:
            print(f"Warning: No data found for current month ({current_year_month}) for gizzard")
            # Sort by year_month in descending order to get the most recent record as fallback
            sorted_data = sorted(data_rows,
                               key=lambda x: x[year_month_col] if len(x) > year_month_col else '',
                               reverse=True)
            if sorted_data:
                current_month_row = sorted_data[0]
                print(f"Using most recent available data from {current_month_row[year_month_col]} for gizzard")
            else:
                print("No data rows found for gizzard, using baseline")
                return BASELINE_GIZZARD_PACKS, BASELINE_GIZZARD_WEIGHT

        # Extract packs balance
        packs_balance = 0
        if gizzard_packs_col is not None and len(current_month_row) > gizzard_packs_col:
            try:
                packs_balance = float(current_month_row[gizzard_packs_col])
            except (ValueError, TypeError):
                print("Invalid gizzard packs balance value in inventory sheet")
                packs_balance = 0

        # Extract weight balance
        weight_balance = 0
        if gizzard_weight_col is not None and len(current_month_row) > gizzard_weight_col:
            try:
                weight_balance = float(current_month_row[gizzard_weight_col])
            except (ValueError, TypeError):
                print("Invalid gizzard weight balance value in inventory sheet")
                weight_balance = 0

        # Add baseline stock count to the ETL balance
        packs_balance = packs_balance + BASELINE_GIZZARD_PACKS
        weight_balance = weight_balance + BASELINE_GIZZARD_WEIGHT

        return packs_balance, weight_balance

    except Exception as e:
        print(f"Error fetching gizzard inventory balance: {str(e)}")
        # Return baseline on error so comparison can still work
        if BASELINE_GIZZARD_PACKS > 0 or BASELINE_GIZZARD_WEIGHT > 0:
            return BASELINE_GIZZARD_PACKS, BASELINE_GIZZARD_WEIGHT
        return None, None

def calculate_total_pieces(stock_data):
    """
    Calculate total whole chicken pieces from stock data across ALL grades and weight ranges.
    Uses the new multi-row header structure.
    """
    try:
        # Parse the balance data
        parsed_columns = parse_balance_data(stock_data)

        total = 0

        # Sum all whole chicken Qty columns across all grades and weight ranges
        for col in parsed_columns:
            product = col['product']
            metric = col['metric']
            value = col['value']

            # Only count whole chicken quantities
            if 'WHOLE CHICKEN' in product and metric == 'Qty':
                try:
                    qty = float(value) if value else 0
                    total += qty
                except (ValueError, TypeError):
                    continue

        return int(total)
    except Exception as e:
        print(f"Error calculating total pieces: {str(e)}")
        return None

def calculate_current_differences(stock_data, inventory_balance, gizzard_inventory_packs, gizzard_inventory_weight):
    """Calculate current inventory balance differences.
    Returns: Tuple of (whole_chicken_diff, gizzard_packs_diff, gizzard_weight_diff)
    """
    try:
        # Calculate whole chicken difference
        total_pieces = calculate_total_pieces(stock_data)
        whole_chicken_diff = None
        if total_pieces is not None and inventory_balance is not None:
            whole_chicken_diff = int(total_pieces - inventory_balance)

        # Calculate gizzard differences - sum all gizzard packs and weight across all grades
        parsed_columns = parse_balance_data(stock_data)
        current_gizzard_packs = 0
        current_gizzard_weight = 0
        gizzard_packs_diff = None
        gizzard_weight_diff = None

        for col in parsed_columns:
            product = col['product']
            metric = col['metric']
            value = col['value']

            # Sum all gizzard packs
            if product == 'GIZZARD' and metric == 'Packs':
                try:
                    packs = float(value) if value else 0
                    current_gizzard_packs += packs
                except (ValueError, TypeError):
                    continue

            # Sum all gizzard weights
            if product == 'GIZZARD' and metric == 'Weight(kg)':
                try:
                    weight = float(value) if value else 0
                    current_gizzard_weight += weight
                except (ValueError, TypeError):
                    continue

        # Calculate packs difference
        if current_gizzard_packs > 0 and gizzard_inventory_packs is not None:
            gizzard_packs_diff = current_gizzard_packs - gizzard_inventory_packs

        # Calculate weight difference
        if current_gizzard_weight > 0 and gizzard_inventory_weight is not None:
            gizzard_weight_diff = current_gizzard_weight - gizzard_inventory_weight

        return whole_chicken_diff, gizzard_packs_diff, gizzard_weight_diff
    except Exception as e:
        print(f"Error calculating current differences: {str(e)}")
        return None, None, None

def format_change_description(change, include_product=True):
    """
    Format a single change object into readable text.
    Change format: {'product': str, 'grade': str, 'metric': str, 'old_value': str, 'new_value': str}
    Uses abbreviations for mobile-friendly display: WC (Whole Chicken), GA (Grade A), GB (Grade B), etc.

    Args:
        change: Dictionary containing product, grade, metric, old_value, new_value
        include_product: If False, omit product name (useful when grouping by product)
    """
    product = change['product']
    grade = change['grade']
    metric = change['metric']
    old_val = change['old_value']
    new_val = change['new_value']

    # Extract weight range for whole chicken
    weight_range = ""
    product_display = ""

    if include_product:
        if 'WHOLE CHICKEN' in product:
            weight_range = product.replace('WHOLE CHICKEN - ', '')
            product_display = f"WC-{weight_range} "  # Abbreviated: WC instead of Whole Chicken
        else:
            product_display = f"{product.title()} "
    else:
        # When not including product, just show weight range for WC
        if 'WHOLE CHICKEN' in product:
            weight_range = product.replace('WHOLE CHICKEN - ', '')
            product_display = f"{weight_range} "
        else:
            # For other products (Gizzard, Wings, etc.), no prefix needed since it's in the group header
            product_display = ""

    # Format grade display with abbreviations
    grade_clean = grade.replace('(Standard Bird)', '').replace('(Standard Gizzard)', '').replace('(Standard Wings)', '').replace('(Standard Laps)', '').replace('(Standard Breast)', '').replace('(Standard Fillet)', '').replace('(Standard Bones)', '').strip()

    # Abbreviate grades: Grade A -> GA, Grade B -> GB, etc.
    if grade_clean == 'Grade A':
        grade_display = 'GA'
    elif grade_clean == 'Grade B':
        grade_display = 'GB'
    elif grade_clean == 'Grade C':
        grade_display = 'GC'
    elif grade_clean == 'Grade D':
        grade_display = 'GD'
    else:
        grade_display = grade_clean

    # Abbreviate metrics: Qty -> Q, Weight(kg) -> W(kg), Packs -> P
    if metric == 'Qty':
        metric_display = 'Q'
    elif metric == 'Weight(kg)':
        metric_display = 'W(kg)'
    elif metric == 'Packs':
        metric_display = 'P'
    else:
        metric_display = metric

    # Format values based on metric
    try:
        if metric == 'Qty':
            # Quantity - format as pieces (abbreviated: pc/pcs)
            old_num = float(old_val) if old_val else 0
            new_num = float(new_val) if new_val else 0
            old_suffix = "pc" if abs(old_num) == 1 else "pcs"
            new_suffix = "pc" if abs(new_num) == 1 else "pcs"
            old_str = f"{int(old_num):,}{old_suffix}"
            new_str = f"{int(new_num):,}{new_suffix}"
        elif metric == 'Weight(kg)':
            # Weight - format as kg
            old_num = float(old_val) if old_val else 0
            new_num = float(new_val) if new_val else 0
            old_str = f"{old_num:,.2f}kg"
            new_str = f"{new_num:,.2f}kg"
        elif metric == 'Packs':
            # Packs - format as packs (abbreviated: pk/pks)
            old_num = float(old_val) if old_val else 0
            new_num = float(new_val) if new_val else 0
            old_suffix = "pk" if abs(old_num) == 1 else "pks"
            new_suffix = "pk" if abs(new_num) == 1 else "pks"
            old_str = f"{old_num:,.1f}{old_suffix}"
            new_str = f"{new_num:,.1f}{new_suffix}"
        else:
            old_str = str(old_val)
            new_str = str(new_val)
    except (ValueError, TypeError):
        old_str = str(old_val)
        new_str = str(new_val)

    # Build the text and ensure it's not empty
    text = f"• {product_display}{grade_display} {metric_display}: {old_str}→{new_str}".strip()

    # Fallback if text is somehow empty (shouldn't happen, but defensive)
    if not text or text == "•":
        text = f"• Change: {old_str}→{new_str}"

    return text

def get_weight_per_piece(category_name):
    """Get weight per piece for a given category and whether it's an approximation."""
    category_lower = category_name.lower()
    
    if 'below' in category_lower and '1kg' in category_lower:
        return 0.7, True, "0.7kg/piece"
    elif 'above' in category_lower and '2kg' in category_lower:
        return 2.0, True, "2kg/piece"
    elif 'uncategorised' in category_lower:
        return 1.4, True, "1.4kg/piece"
    else:
        # Extract numeric weight from category name for exact categories
        try:
            # Handle categories like "1Kg", "1.1Kg", "1.2Kg", etc.
            if 'kg' in category_lower:
                weight_str = category_lower.replace('kg', '').strip()
                weight = float(weight_str)
                return weight, False, None
        except (ValueError, TypeError):
            pass
    
    # Default fallback (shouldn't happen with proper data)
    return 1.0, True, "1kg/piece"

def build_card_alert(balance_changes, balance_data, inventory_balance, gizzard_inventory_packs, gizzard_inventory_weight, chicken_difference_changes, gizzard_difference_changes):
    """Build a comprehensive Google Chat card with all inventory information."""

    # Get current time
    wat_tz = pytz.timezone('Africa/Lagos')
    current_time = datetime.now(pytz.UTC).astimezone(wat_tz)
    timestamp = current_time.strftime('%Y-%m-%d %I:%M:%S %p WAT')

    parsed_columns = parse_balance_data(balance_data)
    total_pieces = calculate_total_pieces(balance_data)

    # Calculate severity level for color coding
    chicken_discrepancy = 0
    gizzard_packs_discrepancy = 0
    gizzard_weight_discrepancy = 0

    if inventory_balance is not None and total_pieces > 0:
        chicken_discrepancy = abs(int(total_pieces - inventory_balance))

    # Calculate current gizzard values from Balance sheet
    current_gizzard_packs = sum(float(col['value']) for col in parsed_columns
                                 if col['product'] == 'GIZZARD' and col['metric'] == 'Packs')
    current_gizzard_weight = sum(float(col['value']) for col in parsed_columns
                                  if col['product'] == 'GIZZARD' and col['metric'] == 'Weight(kg)')

    if gizzard_inventory_packs is not None and current_gizzard_packs > 0:
        gizzard_packs_discrepancy = abs(current_gizzard_packs - gizzard_inventory_packs)

    if gizzard_inventory_weight is not None and current_gizzard_weight > 0:
        gizzard_weight_discrepancy = abs(current_gizzard_weight - gizzard_inventory_weight)

    # Determine overall severity for header color
    # HIGH: >100 chicken pieces or >100 gizzard packs or >50kg gizzard weight discrepancy
    # MEDIUM: >50 chicken pieces or >50 gizzard packs or >20kg gizzard weight discrepancy
    # LOW: anything else with changes
    severity = "LOW"
    if chicken_discrepancy > 100 or gizzard_packs_discrepancy > 100 or gizzard_weight_discrepancy > 50:
        severity = "HIGH"
    elif chicken_discrepancy > 50 or gizzard_packs_discrepancy > 50 or gizzard_weight_discrepancy > 20:
        severity = "MEDIUM"

    # Build card sections
    sections = []

    # Section 0: Baseline Reference (Stock Count)
    if BASELINE_WC_QTY > 0 or BASELINE_GIZZARD_PACKS > 0:
        baseline_widgets = [
            {"decoratedText": {"text": f"WC: <b>{int(BASELINE_WC_QTY):,}pcs</b> / <b>{BASELINE_WC_WEIGHT:,.2f}kg</b>"}},
            {"decoratedText": {"text": f"Gizzard: <b>{int(BASELINE_GIZZARD_PACKS)}pks</b> / <b>{BASELINE_GIZZARD_WEIGHT:.2f}kg</b>"}}
        ]
        sections.append({
            "header": "📦 Baseline (Stock Count)",
            "widgets": baseline_widgets
        })

    # Section 1: Changes Summary
    if balance_changes or chicken_difference_changes or gizzard_difference_changes:
        change_widgets = []

        # Balance changes summary - grouped by product
        if balance_changes:
            change_count_text = f"🔄 {len(balance_changes)} balance change(s) detected"
            change_widgets.append({
                "decoratedText": {
                    "text": f"<b>{change_count_text}</b>"
                }
            })

            # Group changes by product type
            grouped_changes = {}
            for change in balance_changes:
                product = change['product']

                # Determine product group
                if 'WHOLE CHICKEN' in product:
                    group = 'WC'
                elif product == 'GIZZARD':
                    group = 'Gizzard'
                elif product == 'WINGS':
                    group = 'Wings'
                elif product == 'LAPS':
                    group = 'Laps'
                elif product == 'BREAST':
                    group = 'Breast'
                elif product == 'FILLET':
                    group = 'Fillet'
                elif product == 'BONES':
                    group = 'Bones'
                else:
                    group = product.title()

                if group not in grouped_changes:
                    grouped_changes[group] = []
                grouped_changes[group].append(change)

            # Display changes by group
            max_changes = 50  # Show up to 50 changes total
            changes_shown = 0

            # Order: WC, Gizzard, Wings, Laps, Breast, Fillet, Bones, Others
            product_order = ['WC', 'Gizzard', 'Wings', 'Laps', 'Breast', 'Fillet', 'Bones']

            for product_group in product_order:
                if product_group not in grouped_changes:
                    continue
                if changes_shown >= max_changes:
                    break

                # Add product group header
                change_widgets.append({
                    "decoratedText": {
                        "text": f"<b>{product_group} Changes:</b>"
                    }
                })

                # Add changes for this group
                for change in grouped_changes[product_group]:
                    if changes_shown >= max_changes:
                        break
                    change_widgets.append({
                        "decoratedText": {
                            "text": format_change_description(change, include_product=False)
                        }
                    })
                    changes_shown += 1

            # Handle any remaining groups not in product_order
            for product_group in grouped_changes:
                if product_group in product_order:
                    continue
                if changes_shown >= max_changes:
                    break

                change_widgets.append({
                    "decoratedText": {
                        "text": f"<b>{product_group} Changes:</b>"
                    }
                })

                for change in grouped_changes[product_group]:
                    if changes_shown >= max_changes:
                        break
                    change_widgets.append({
                        "decoratedText": {
                            "text": format_change_description(change, include_product=False)
                        }
                    })
                    changes_shown += 1

            if len(balance_changes) > max_changes:
                change_widgets.append({
                    "decoratedText": {
                        "text": f"<i>...and {len(balance_changes) - max_changes} more changes</i>"
                    }
                })

        # Difference changes
        difference_changes = []
        if chicken_difference_changes:
            difference_changes.extend(chicken_difference_changes)
        if gizzard_difference_changes:
            difference_changes.extend(gizzard_difference_changes)

        if difference_changes:
            change_widgets.append({"divider": {}})
            change_widgets.append({
                "decoratedText": {
                    "text": "<b>Inventory Balance Diff Changes:</b>"
                }
            })

            for change_type, old_val, new_val in difference_changes:
                if 'Chicken' in change_type:
                    old_suffix = "pc" if abs(old_val) == 1 else "pcs"
                    new_suffix = "pc" if abs(new_val) == 1 else "pcs"
                    # Abbreviate: WC Balance Diff instead of Whole Chicken Balance Difference
                    change_text = f"WC Balance Diff: {old_val:,}{old_suffix}→{new_val:,}{new_suffix}"
                elif 'Packs' in change_type:
                    # Gizzard Packs Balance Diff
                    change_text = f"Gizzard Packs Diff: {old_val:,.1f}pks→{new_val:,.1f}pks"
                else:
                    # Gizzard Weight Balance Diff
                    change_text = f"Gizzard Weight Diff: {old_val:,.2f}kg→{new_val:,.2f}kg"

                change_widgets.append({
                    "decoratedText": {
                        "text": change_text,
                        "startIcon": {"knownIcon": "STAR"}
                    }
                })

        sections.append({
            "header": "⚠️ Changes Detected",
            "widgets": change_widgets
        })

    # Section 2: Whole Chicken Comparison
    comparison_widgets = []

    if inventory_balance is not None and total_pieces > 0:
        difference = int(total_pieces - inventory_balance)

        comparison_widgets.append({
            "decoratedText": {
                "text": f"Spec Sheet: <b>{total_pieces:,}pcs</b>"
            }
        })

        comparison_widgets.append({
            "decoratedText": {
                "text": f"Inventory: <b>{int(inventory_balance):,}pcs</b>"
            }
        })

        if difference == 0:
            comparison_widgets.append({
                "decoratedText": {
                    "text": "<font color=\"#0F9D58\">✅ <b>Stock balance matches inventory records</b></font>",
                    "startIcon": {"knownIcon": "CONFIRMATION_NUMBER_ICON"}
                }
            })
        else:
            # Color code based on severity
            abs_diff = abs(difference)
            if abs_diff > 100:
                color = "#EA4335"  # Red for high severity
                icon = "BOOKMARK"
            elif abs_diff > 50:
                color = "#FBBC04"  # Yellow for medium severity
                icon = "DESCRIPTION"
            else:
                color = "#FF6D00"  # Orange for low severity
                icon = "DESCRIPTION"

            sign = "+" if difference > 0 else ""
            comparison_widgets.append({
                "decoratedText": {
                    "text": f"<font color=\"{color}\">⚠️ <b>Diff: {sign}{difference:,}pcs</b></font>",
                    "startIcon": {"knownIcon": icon}
                }
            })

    if comparison_widgets:
        sections.append({
            "header": "📊 WC vs Inventory",
            "widgets": comparison_widgets
        })

    # Section 3: Gizzard Comparison (Packs and Weight)
    gizzard_widgets = []

    # Packs comparison
    if gizzard_inventory_packs is not None and current_gizzard_packs > 0:
        packs_difference = current_gizzard_packs - gizzard_inventory_packs

        gizzard_widgets.append({
            "decoratedText": {
                "text": "<b>Packs Comparison:</b>"
            }
        })

        gizzard_widgets.append({
            "decoratedText": {
                "text": f"Spec Sheet: <b>{current_gizzard_packs:,.1f}pks</b>"
            }
        })

        gizzard_widgets.append({
            "decoratedText": {
                "text": f"Inventory: <b>{gizzard_inventory_packs:,.1f}pks</b>"
            }
        })

        if abs(packs_difference) < 0.01:
            gizzard_widgets.append({
                "decoratedText": {
                    "text": "<font color=\"#0F9D58\">✅ <b>Packs balance matches inventory records</b></font>",
                    "startIcon": {"knownIcon": "CONFIRMATION_NUMBER_ICON"}
                }
            })
        else:
            # Color code based on severity
            abs_diff = abs(packs_difference)
            if abs_diff > 100:
                color = "#EA4335"  # Red for high severity (>100 packs)
                icon = "BOOKMARK"
            elif abs_diff > 50:
                color = "#FBBC04"  # Yellow for medium severity (>50 packs)
                icon = "DESCRIPTION"
            else:
                color = "#FF6D00"  # Orange for low severity
                icon = "DESCRIPTION"

            sign = "+" if packs_difference > 0 else ""
            gizzard_widgets.append({
                "decoratedText": {
                    "text": f"<font color=\"{color}\">⚠️ <b>Diff: {sign}{packs_difference:,.1f}pks</b></font>",
                    "startIcon": {"knownIcon": icon}
                }
            })

        gizzard_widgets.append({"divider": {}})

    # Weight comparison
    if gizzard_inventory_weight is not None and current_gizzard_weight > 0:
        weight_difference = current_gizzard_weight - gizzard_inventory_weight

        gizzard_widgets.append({
            "decoratedText": {
                "text": "<b>Weight Comparison:</b>"
            }
        })

        gizzard_widgets.append({
            "decoratedText": {
                "text": f"Spec Sheet: <b>{current_gizzard_weight:,.1f}kg</b>"
            }
        })

        gizzard_widgets.append({
            "decoratedText": {
                "text": f"Inventory: <b>{gizzard_inventory_weight:,.1f}kg</b>"
            }
        })

        if abs(weight_difference) < 0.01:
            gizzard_widgets.append({
                "decoratedText": {
                    "text": "<font color=\"#0F9D58\">✅ <b>Weight balance matches inventory records</b></font>",
                    "startIcon": {"knownIcon": "CONFIRMATION_NUMBER_ICON"}
                }
            })
        else:
            # Color code based on severity
            abs_diff = abs(weight_difference)
            if abs_diff > 50:
                color = "#EA4335"  # Red for high severity (>50kg)
                icon = "BOOKMARK"
            elif abs_diff > 20:
                color = "#FBBC04"  # Yellow for medium severity (>20kg)
                icon = "DESCRIPTION"
            else:
                color = "#FF6D00"  # Orange for low severity
                icon = "DESCRIPTION"

            sign = "+" if weight_difference > 0 else ""
            gizzard_widgets.append({
                "decoratedText": {
                    "text": f"<font color=\"{color}\">⚠️ <b>Diff: {sign}{weight_difference:,.1f}kg</b></font>",
                    "startIcon": {"knownIcon": icon}
                }
            })

    if gizzard_widgets:
        sections.append({
            "header": "🍗 Gizzard vs Inventory",
            "widgets": gizzard_widgets
        })

    # Section 4: Whole Chicken Details
    chicken_widgets = build_whole_chicken_widgets(balance_data)
    if chicken_widgets:
        sections.append({
            "header": "📦 WC Stock Levels",
            "widgets": chicken_widgets
        })

    # Section 5: Gizzard & Parts Details
    parts_widgets = build_gizzard_and_parts_widgets(balance_data)
    if parts_widgets:
        sections.append({
            "header": "📦 Parts Stock Levels",
            "widgets": parts_widgets
        })

    # Add View Specification Sheet button to the last section
    spec_sheet_url = f"https://docs.google.com/spreadsheets/d/{SPECIFICATION_SHEET_ID}/edit#gid=0"

    # Get the last section and add the button to it
    if sections:
        sections[-1]["widgets"].append({
            "buttonList": {
                "buttons": [{
                    "text": "🔗 View Specification Sheet",
                    "onClick": {
                        "openLink": {
                            "url": spec_sheet_url
                        }
                    }
                }]
            }
        })

    # Build the complete card with severity-based header color
    header_config = {
        "title": "🔔 Kaduna Inventory Alert",
        "subtitle": f"Updated: {timestamp}"
    }

    # Add severity indicator to subtitle
    if severity == "HIGH":
        header_config["subtitle"] = f"🔴 HIGH PRIORITY | {timestamp}"
    elif severity == "MEDIUM":
        header_config["subtitle"] = f"🟡 MEDIUM PRIORITY | {timestamp}"
    else:
        header_config["subtitle"] = f"Updated: {timestamp}"

    card = {
        "cardsV2": [{
            "cardId": "kaduna-inventory-alert",
            "card": {
                "header": header_config,
                "sections": sections
            }
        }]
    }

    return card

def build_whole_chicken_widgets(balance_data):
    """Build widgets for whole chicken details section."""
    widgets = []
    parsed_columns = parse_balance_data(balance_data)

    # Group whole chicken data by weight range
    weight_ranges = {}
    for col in parsed_columns:
        if 'WHOLE CHICKEN' in col['product']:
            weight = col['product'].replace('WHOLE CHICKEN - ', '')
            if weight not in weight_ranges:
                weight_ranges[weight] = []
            weight_ranges[weight].append(col)

    # Sort weight ranges
    weight_order = ['BELOW 1KG', '1KG', '1.1KG', '1.2KG', '1.3KG', '1.4KG', '1.5KG', '1.6KG', '1.7KG', '1.8KG', '1.9KG', '2KG ABOVE', 'UNCATEGORISED']

    total_qty = 0
    total_weight_kg = 0

    # Filter to only weights with data
    weights_with_data = [w for w in weight_order if w in weight_ranges]

    for idx, weight in enumerate(weights_with_data):
        # Get weight per piece for this category (used to calculate weight from qty)
        weight_per_piece, is_approx, approx_text = get_weight_per_piece(weight)

        # Group by grade
        grades = {'Grade A (Standard Bird)': {}, 'Grade B': {}, 'Grade C': {}, 'Grade D': {}}
        for col in weight_ranges[weight]:
            grade = col['grade']
            metric = col['metric']
            value = col['value']
            if grade not in grades:
                grades[grade] = {}
            grades[grade][metric] = value

        # Build combined text for this weight category
        grade_lines = [f"<b>{weight}</b>"]

        # Display each grade
        for grade_name in grades:
            grade_data = grades[grade_name]
            if not grade_data:
                continue

            grade_display = grade_name.replace('(Standard Bird)', '').strip()
            qty = float(grade_data.get('Qty', 0))
            weight_kg = qty * weight_per_piece  # Calculate weight: qty × weight per piece

            total_qty += qty
            total_weight_kg += weight_kg

            # Format qty as bags + pieces (compact format)
            bags = int(qty // 20)
            remaining = int(qty % 20)

            if bags > 0 and remaining > 0:
                qty_display = f"{bags}bags+{remaining}pcs"
            elif bags > 0:
                bags_suffix = "bag" if bags == 1 else "bags"
                qty_display = f"{bags}{bags_suffix}"
            else:
                pcs_suffix = "pc" if remaining == 1 else "pcs"
                qty_display = f"{remaining}{pcs_suffix}"

            grade_lines.append(f"{grade_display}: {qty_display} ({weight_kg:,.1f}kg)")

        # Combine all lines into single widget using textParagraph for proper multi-line rendering
        combined_text = "\n".join(grade_lines)
        widgets.append({
            "textParagraph": {
                "text": combined_text
            }
        })

        # Add divider after each weight category for better readability
        widgets.append({"divider": {}})

    # Add totals
    total_tonnes = total_weight_kg / 1000
    widgets.append({"divider": {}})
    widgets.append({
        "decoratedText": {
            "text": f"<b>TOTAL: {int(total_qty):,} pieces (≈ {total_weight_kg:,.1f} kg / {total_tonnes:.1f} tonnes)</b>"
        }
    })

    return widgets

def build_gizzard_and_parts_widgets(balance_data):
    """Build widgets for gizzard and parts details section."""
    widgets = []
    parsed_columns = parse_balance_data(balance_data)

    # Products to display (in order)
    products_order = ['GIZZARD', 'WINGS', 'LAPS', 'BREAST', 'FILLET', 'BONES']

    # First pass: identify which products have data
    products_with_data = []
    for product_name in products_order:
        product_cols = [col for col in parsed_columns if col['product'] == product_name]
        if product_cols:
            # Check if product has any data (packs > 0 or weight > 0)
            has_any_data = False
            for col in product_cols:
                if col['metric'] == 'Packs':
                    try:
                        if float(col['value']) > 0:
                            has_any_data = True
                            break
                    except (ValueError, TypeError):
                        pass
                elif col['metric'] == 'Weight(kg)':
                    try:
                        if float(col['value']) > 0:
                            has_any_data = True
                            break
                    except (ValueError, TypeError):
                        pass
            if has_any_data:
                products_with_data.append(product_name)

    # Second pass: display products with data and add dividers
    for prod_idx, product_name in enumerate(products_with_data):
        # Get data for this product
        product_cols = [col for col in parsed_columns if col['product'] == product_name]

        # Group by grade dynamically from actual data
        grades = {}
        for col in product_cols:
            grade = col['grade']
            metric = col['metric']
            value = col['value']
            if grade not in grades:
                grades[grade] = {}
            grades[grade][metric] = value

        # Sort grades for consistent display (A before B before C before D)
        grade_order = [g for g in ['Grade A (Standard Gizzard)', 'Grade A (Standard Wings)', 'Grade A (Standard Laps)',
                                   'Grade A (Standard Breast)', 'Grade A (Standard Fillet)', 'Grade A (Standard Bones)',
                                   'Grade A', 'Grade B', 'Grade C', 'Grade D'] if g in grades]
        # Add any remaining grades not in the predefined order
        grade_order.extend([g for g in sorted(grades.keys()) if g not in grade_order])

        # Build combined text for this product
        product_lines = [f"<b>{product_name.title()}</b>"]

        # Display each grade
        for grade_name in grade_order:
            grade_data = grades.get(grade_name, {})
            if not grade_data:
                continue

            grade_display = (grade_name.replace('(Standard Gizzard)', '')
                            .replace('(Standard Wings)', '')
                            .replace('(Standard Laps)', '')
                            .replace('(Standard Breast)', '')
                            .replace('(Standard Fillet)', '')
                            .replace('(Standard Bones)', '').strip())

            packs = float(grade_data.get('Packs', 0)) if grade_data.get('Packs') else 0
            weight = float(grade_data.get('Weight(kg)', 0)) if grade_data.get('Weight(kg)') else 0

            # Only show grades with actual data
            if packs > 0 or weight > 0:
                packs_suffix = "pk" if packs == 1 else "pks"
                packs_text = f"{packs:,.1f}{packs_suffix}"
                product_lines.append(f"{grade_display}: {packs_text} ({weight:,.1f}kg)")

        # Combine all lines into single widget using textParagraph for proper multi-line rendering
        combined_text = "\n".join(product_lines)
        widgets.append({
            "textParagraph": {
                "text": combined_text
            }
        })

        # Add divider after each product for better readability
        widgets.append({"divider": {}})

    return widgets

def send_combined_alert(webhook_url, balance_changes, balance_data, inventory_balance=None, gizzard_inventory_packs=None, gizzard_inventory_weight=None, chicken_difference_changes=None, gizzard_difference_changes=None):
    """Send combined alert to Google Space as a single card message."""
    try:
        # Only proceed if there are actual changes
        if not balance_changes and not chicken_difference_changes and not gizzard_difference_changes:
            print("No changes detected. No alert needed.")
            return True

        print("Preparing to send card alert message...")

        # Build the card
        card = build_card_alert(
            balance_changes, balance_data, inventory_balance,
            gizzard_inventory_packs, gizzard_inventory_weight,
            chicken_difference_changes, gizzard_difference_changes
        )

        # Send card message
        @webhook_circuit_breaker
        @retry(
            retry=retry_if_exception(should_retry_webhook),
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=1, max=30),
            before_sleep=lambda retry_state: print(f"Webhook failed, retrying in {retry_state.next_action.sleep} seconds... (attempt {retry_state.attempt_number})")
        )
        def _send_webhook(payload):
            print(f"Sending card webhook request...")
            response = requests.post(webhook_url, json=payload, timeout=10)
            if not response.ok:
                response_text = response.text.strip()
                if response_text:
                    preview = (response_text[:1000] + '…') if len(response_text) > 1000 else response_text
                    print(f"Webhook response body: {preview}")
                raise requests.exceptions.HTTPError(
                    f"Status {response.status_code}: {response.reason}",
                    response=response
                )
            response.raise_for_status()
            print(f"Webhook response status: {response.status_code}")
            return response

        try:
            print("\nSending card alert...")
            _send_webhook(card)
            print("✅ Card alert sent successfully")
            clear_failed_webhooks()
            return True
        except Exception as e:
            error_detail = str(e)
            if isinstance(e, requests.exceptions.HTTPError) and getattr(e, "response", None) is not None:
                body = e.response.text.strip()
                if body:
                    error_detail = f"{error_detail} | response body: {body[:1000]}"
                    if len(body) > 1000:
                        error_detail += "…"
            print(f"❌ Failed to send card alert: {error_detail}")
            # Save to dead letter queue
            save_failed_webhook(card, error_detail, webhook_url)
            return False

    except pybreaker.CircuitBreakerOpenException:
        print("Circuit breaker is open - webhook service appears to be down")
        return False
    except Exception as e:
        print(f"Error in send_combined_alert: {str(e)}")
        return False

def main():
    try:
        # Get webhook URL from environment variable
        webhook_url = os.environ.get('SPACE_WEBHOOK_URL')
        if not webhook_url:
            raise ValueError("SPACE_WEBHOOK_URL environment variable not set")
        print("Webhook URL configured")

        # Check for any previously failed webhooks
        check_failed_webhooks()

        # Clear any previous state read failure alert file first
        if os.path.exists(STATE_READ_FAILURE_ALERT_FILE):
            os.remove(STATE_READ_FAILURE_ALERT_FILE)

        print("✅ Using encrypted state files for reliable change detection")

        # Initialize the Sheets API service
        print("Initializing Google Sheets service...")
        service = get_service()
        
        # Get current balance data (includes stock and parts in one sheet)
        balance_data = get_sheet_data(service, STOCK_SHEET_NAME, STOCK_RANGE)

        # Get inventory balance for comparison
        inventory_balance = get_inventory_balance(service)

        # Get gizzard inventory balance for comparison (returns tuple: packs, weight)
        gizzard_inventory_packs, gizzard_inventory_weight = get_gizzard_inventory_balance(service)

        # Load previous states
        previous_balance_data = load_previous_state(BALANCE_STATE_FILE)
        previous_chicken_diff = load_previous_state(WHOLE_CHICKEN_DIFF_STATE_FILE)
        previous_gizzard_packs_diff = load_previous_state(GIZZARD_PACKS_DIFF_STATE_FILE)
        previous_gizzard_weight_diff = load_previous_state(GIZZARD_WEIGHT_DIFF_STATE_FILE)

        # Calculate current differences (returns: chicken_diff, gizzard_packs_diff, gizzard_weight_diff)
        current_chicken_diff, current_gizzard_packs_diff, current_gizzard_weight_diff = calculate_current_differences(
            balance_data, inventory_balance, gizzard_inventory_packs, gizzard_inventory_weight
        )

        # Check for changes in balance data
        balance_changes = []
        if not previous_balance_data:
            print("No previous balance state found, initializing balance state file...")
        else:
            print("Checking for balance changes...")
            balance_changes = detect_balance_changes(previous_balance_data, balance_data)

        # Check for changes in whole chicken inventory balance differences
        chicken_difference_changes = []
        if previous_chicken_diff is None:
            print("No previous whole chicken difference state found, initializing state file...")
        else:
            print("Checking for whole chicken difference changes...")
            chicken_difference_changes = detect_chicken_difference_changes(previous_chicken_diff, current_chicken_diff)

        # Check for changes in gizzard inventory balance differences (both packs and weight)
        gizzard_difference_changes = []
        if previous_gizzard_packs_diff is None and previous_gizzard_weight_diff is None:
            print("No previous gizzard difference state found, initializing state files...")
        else:
            print("Checking for gizzard difference changes...")
            gizzard_difference_changes = detect_gizzard_difference_changes(
                previous_gizzard_packs_diff, current_gizzard_packs_diff,
                previous_gizzard_weight_diff, current_gizzard_weight_diff
            )

        # Send combined alert if there are any changes
        if balance_changes or chicken_difference_changes or gizzard_difference_changes:
            print("Changes detected, sending combined alert...")
            if send_combined_alert(webhook_url, balance_changes, balance_data, inventory_balance,
                                 gizzard_inventory_packs, gizzard_inventory_weight,
                                 chicken_difference_changes, gizzard_difference_changes):
                print("Alert sent successfully, updating state files...")
            else:
                print("Failed to send alert, but will still update state files...")
        else:
            print("No changes detected, updating state files...")

        # Always update all state files at the end
        save_current_state(balance_data, BALANCE_STATE_FILE)
        save_current_state(current_chicken_diff, WHOLE_CHICKEN_DIFF_STATE_FILE)
        save_current_state(current_gizzard_packs_diff, GIZZARD_PACKS_DIFF_STATE_FILE)
        save_current_state(current_gizzard_weight_diff, GIZZARD_WEIGHT_DIFF_STATE_FILE)

        # Commit encrypted state files to repository
        print("Committing encrypted state files to repository...")
        commit_encrypted_state_files()

    except APIError as e:
        print(f"API Error: {str(e)}")
        # Don't exit with error to avoid GitHub Actions failure
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        # Don't exit with error to avoid GitHub Actions failure

if __name__ == '__main__':
    main() 