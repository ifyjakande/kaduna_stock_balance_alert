#!/usr/bin/env python3
"""Daily Inventory Log - Records end-of-day Whole Chicken inventory levels to Google Sheets."""

import os
import time
import random
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import gspread
import pytz
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Constants
DAILY_LOG_SPREADSHEET_ID = '1lWIJbTCiNFrTYEcBsN1vRS5970qzW-OCsZCejI3HWkg'
SPECIFICATION_SHEET_ID = os.environ.get('SPECIFICATION_SHEET_ID')

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = 'service-account.json'

# Balance sheet configuration
STOCK_SHEET_NAME = 'Balance'
STOCK_RANGE = 'A1:EX5'
LOG_SHEET_NAME = 'Daily Inventory Log'

# Lagos timezone (WAT = UTC+1)
WAT_TZ = pytz.timezone('Africa/Lagos')

# Header formatting colors (hex: #2E5494 for background, #FFFFFF for text)
HEADER_BG_COLOR = {'red': 0.18, 'green': 0.33, 'blue': 0.58}
HEADER_TEXT_COLOR = {'red': 1.0, 'green': 1.0, 'blue': 1.0}


class DailyLogError(Exception):
    """Custom exception for daily log errors."""
    pass


def is_rate_limit_error(exception):
    """Check if the exception is a rate limit error."""
    if isinstance(exception, HttpError):
        return exception.resp.status in [429, 500, 502, 503]
    if isinstance(exception, Exception):
        error_str = str(exception).lower()
        return any(term in error_str for term in ['quota', 'rate limit', 'too many requests', '429'])
    return False


@retry(
    retry=retry_if_exception_type((HttpError, Exception)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    before_sleep=lambda retry_state: print(f"Rate limit hit, retrying in {retry_state.next_action.sleep} seconds...")
)
def robust_api_call(api_func, *args, **kwargs):
    """Execute API call with robust retry logic."""
    try:
        return api_func(*args, **kwargs)
    except Exception as e:
        if is_rate_limit_error(e):
            time.sleep(random.uniform(0.5, 2.0))
        raise


def get_credentials():
    """Create credentials for Google Sheets access."""
    try:
        return service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    except Exception as e:
        raise DailyLogError(f"Failed to create credentials: {str(e)}")


def get_services():
    """Create and return Google Sheets service objects."""
    credentials = get_credentials()
    sheets_service = build('sheets', 'v4', credentials=credentials)
    gspread_client = gspread.Client(auth=credentials)
    return sheets_service, gspread_client


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

    row_product = data[0]
    row_grade = data[2]
    row_data = data[3]
    row_metric = data[4]

    parsed_columns = []
    current_product = ""
    current_grade = ""

    max_cols = max(len(row_product), len(row_grade), len(row_data), len(row_metric))

    for i in range(max_cols):
        if i < len(row_product) and row_product[i] and row_product[i].strip():
            current_product = row_product[i].strip()

        if current_product in ['DATE', 'NOTES']:
            continue

        grade_cell = row_grade[i].strip() if i < len(row_grade) and row_grade[i] else ""
        if grade_cell:
            current_grade = grade_cell

        metric = row_metric[i].strip() if i < len(row_metric) and row_metric[i] else ""
        value = row_data[i].strip() if i < len(row_data) and row_data[i] else "0"

        if current_product and current_grade and metric:
            parsed_columns.append({
                'col_index': i,
                'product': current_product,
                'grade': current_grade,
                'metric': metric,
                'value': value
            })

    return parsed_columns


def get_weight_per_piece(category_name):
    """Get weight per piece for a given category."""
    category_lower = category_name.lower()

    if 'below' in category_lower and '1kg' in category_lower:
        return 0.7
    elif 'above' in category_lower and '2kg' in category_lower:
        return 2.0
    elif 'uncategorised' in category_lower:
        return 1.4
    else:
        try:
            if 'kg' in category_lower:
                weight_str = category_lower.replace('kg', '').strip()
                return float(weight_str)
        except (ValueError, TypeError):
            pass

    return 1.0


def calculate_whole_chicken_weight_kg(balance_data):
    """
    Calculate total Whole Chicken weight in kg from Balance sheet data.
    Uses qty * weight_per_piece for each weight category.
    """
    parsed_columns = parse_balance_data(balance_data)
    total_weight_kg = 0.0

    for col in parsed_columns:
        if 'WHOLE CHICKEN' in col['product'] and col['metric'] == 'Qty':
            weight_range = col['product'].replace('WHOLE CHICKEN - ', '').replace('WHOLE CHICKEN -', '').strip()
            weight_per_piece = get_weight_per_piece(weight_range)
            try:
                qty = float(col['value']) if col['value'] else 0
                total_weight_kg += qty * weight_per_piece
            except (ValueError, TypeError):
                continue

    return total_weight_kg


def get_balance_sheet_data(sheets_service):
    """Fetch Balance sheet data from Specification Sheet."""

    def _fetch_data():
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPECIFICATION_SHEET_ID,
            range=f'{STOCK_SHEET_NAME}!{STOCK_RANGE}'
        ).execute()
        return result.get('values', [])

    data = robust_api_call(_fetch_data)

    if not data or len(data) < 5:
        raise DailyLogError("Invalid data structure from Balance sheet")

    return data


def find_existing_entry_for_date(gspread_client, target_date):
    """Check if an entry for the given date already exists. Returns (row_number, entry_id) or (None, None)."""
    try:
        spreadsheet = gspread_client.open_by_key(DAILY_LOG_SPREADSHEET_ID)

        try:
            worksheet = spreadsheet.worksheet(LOG_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            return None, None

        # Get all data (Date is column B, Entry ID is column A)
        all_data = worksheet.get_all_values()

        # Skip first 3 rows (title, description, header) - data starts at row 4 (index 3)
        for idx, row in enumerate(all_data[3:], start=4):
            if len(row) >= 2 and row[1] == target_date:
                try:
                    entry_id = int(row[0])
                    return idx, entry_id
                except (ValueError, TypeError):
                    continue

        return None, None

    except Exception as e:
        raise DailyLogError(f"Failed to check existing entries: {str(e)}")


def get_next_entry_id(gspread_client):
    """Get the next Entry ID by reading existing entries."""
    try:
        spreadsheet = gspread_client.open_by_key(DAILY_LOG_SPREADSHEET_ID)

        try:
            worksheet = spreadsheet.worksheet(LOG_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            return 1

        entry_ids = worksheet.col_values(1)

        # Skip first 3 rows (title, description, header) - data starts at row 4
        numeric_ids = []
        for val in entry_ids[3:]:
            try:
                numeric_ids.append(int(val))
            except (ValueError, TypeError):
                continue

        return max(numeric_ids) + 1 if numeric_ids else 1

    except Exception as e:
        raise DailyLogError(f"Failed to get next Entry ID: {str(e)}")


def format_date_components(dt):
    """Format date into required components."""
    return {
        'date': dt.strftime('%d-%b-%Y'),
        'year': dt.strftime('%Y'),
        'month': dt.strftime('%B')
    }


def update_log_entry(gspread_client, row_number, entry_data):
    """Update an existing log entry at the specified row."""
    try:
        spreadsheet = gspread_client.open_by_key(DAILY_LOG_SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(LOG_SHEET_NAME)

        row = [
            entry_data['entry_id'],
            entry_data['date'],
            entry_data['year'],
            entry_data['month'],
            entry_data['state'],
            entry_data['inventory_tonnes'],
            entry_data['below_10_tonnes']
        ]

        def _update_row():
            worksheet.update(values=[row], range_name=f'A{row_number}:G{row_number}')

        robust_api_call(_update_row)
        return True

    except Exception as e:
        raise DailyLogError(f"Failed to update log entry: {str(e)}")


def append_log_entry(gspread_client, entry_data):
    """Append a new log entry to the Daily Log sheet."""
    try:
        spreadsheet = gspread_client.open_by_key(DAILY_LOG_SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(LOG_SHEET_NAME)

        row = [
            entry_data['entry_id'],
            entry_data['date'],
            entry_data['year'],
            entry_data['month'],
            entry_data['state'],
            entry_data['inventory_tonnes'],
            entry_data['below_10_tonnes']
        ]

        def _append_row():
            worksheet.append_row(row, value_input_option='USER_ENTERED')

        robust_api_call(_append_row)
        return True

    except Exception as e:
        raise DailyLogError(f"Failed to append log entry: {str(e)}")


def ensure_sheet_formatting(gspread_client, sheets_service):
    """Ensure the log sheet exists with proper headers and formatting matching the template."""
    try:
        spreadsheet = gspread_client.open_by_key(DAILY_LOG_SPREADSHEET_ID)
        needs_formatting = False
        headers = ['Entry ID', 'Date', 'Year', 'Month', 'State',
                  'Inventory Level (tonnes)', 'Below 10 Tonnes']

        try:
            worksheet = spreadsheet.worksheet(LOG_SHEET_NAME)
            # Check row 3 for headers (reliable check unaffected by merged cells in row 1)
            header_row = worksheet.row_values(3)
            if header_row and len(header_row) >= 1 and header_row[0] == 'Entry ID':
                return
            # Headers missing - write title/description/headers to rows 1-3 only
            # NEVER clear the sheet to avoid destroying accumulated data
            needs_formatting = True
        except gspread.exceptions.WorksheetNotFound:
            needs_formatting = True
            worksheet = spreadsheet.add_worksheet(title=LOG_SHEET_NAME, rows=1000, cols=10)

        if needs_formatting:
            # Write all row data in one atomic call using raw Sheets API
            # (gspread's deprecated update() is unreliable with merged cells)
            sheets_service.spreadsheets().values().update(
                spreadsheetId=DAILY_LOG_SPREADSHEET_ID,
                range=f"'{LOG_SHEET_NAME}'!A1:G3",
                valueInputOption='RAW',
                body={'values': [
                    ['PULLUS PURCHASE - Daily Inventory Log', '', '', '', '', '', ''],
                    ['Record daily inventory levels. "Below 10 Tonnes" auto-calculates. Data aggregates to Monthly Scorecards.', '', '', '', '', '', ''],
                    headers
                ]}
            ).execute()

            sheet_id = worksheet.id
            requests = [
                # Merge cells for title row (A1:G1)
                {
                    'mergeCells': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 1,
                            'startColumnIndex': 0,
                            'endColumnIndex': 7
                        },
                        'mergeType': 'MERGE_ALL'
                    }
                },
                # Format title row (Row 1) - Blue background #2E5494, white text
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 1,
                            'startColumnIndex': 0,
                            'endColumnIndex': 7
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': HEADER_BG_COLOR,
                                'textFormat': {
                                    'foregroundColor': HEADER_TEXT_COLOR,
                                    'bold': True,
                                    'fontSize': 14
                                },
                                'horizontalAlignment': 'CENTER',
                                'verticalAlignment': 'MIDDLE'
                            }
                        },
                        'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)'
                    }
                },
                # Merge cells for description row (A2:G2)
                {
                    'mergeCells': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 1,
                            'endRowIndex': 2,
                            'startColumnIndex': 0,
                            'endColumnIndex': 7
                        },
                        'mergeType': 'MERGE_ALL'
                    }
                },
                # Format description row (Row 2) - Gray italic text
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 1,
                            'endRowIndex': 2,
                            'startColumnIndex': 0,
                            'endColumnIndex': 7
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {
                                    'foregroundColor': {'red': 0.4, 'green': 0.4, 'blue': 0.4},
                                    'italic': True,
                                    'fontSize': 10
                                },
                                'horizontalAlignment': 'CENTER'
                            }
                        },
                        'fields': 'userEnteredFormat(textFormat,horizontalAlignment)'
                    }
                },
                # Format header row (Row 3) - Blue background #2E5494, white bold text
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 2,
                            'endRowIndex': 3,
                            'startColumnIndex': 0,
                            'endColumnIndex': 7
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': HEADER_BG_COLOR,
                                'textFormat': {
                                    'foregroundColor': HEADER_TEXT_COLOR,
                                    'bold': True,
                                    'fontSize': 11
                                },
                                'horizontalAlignment': 'CENTER',
                                'verticalAlignment': 'MIDDLE'
                            }
                        },
                        'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)'
                    }
                },
                # Freeze first 3 rows
                {
                    'updateSheetProperties': {
                        'properties': {
                            'sheetId': sheet_id,
                            'gridProperties': {'frozenRowCount': 3}
                        },
                        'fields': 'gridProperties.frozenRowCount'
                    }
                },
                # Set column widths (A=100, B=130, C=80, D=110, E=100, F=190, G=140)
                {'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': 1}, 'properties': {'pixelSize': 100}, 'fields': 'pixelSize'}},
                {'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'COLUMNS', 'startIndex': 1, 'endIndex': 2}, 'properties': {'pixelSize': 130}, 'fields': 'pixelSize'}},
                {'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'COLUMNS', 'startIndex': 2, 'endIndex': 3}, 'properties': {'pixelSize': 80}, 'fields': 'pixelSize'}},
                {'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'COLUMNS', 'startIndex': 3, 'endIndex': 4}, 'properties': {'pixelSize': 110}, 'fields': 'pixelSize'}},
                {'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'COLUMNS', 'startIndex': 4, 'endIndex': 5}, 'properties': {'pixelSize': 100}, 'fields': 'pixelSize'}},
                {'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'COLUMNS', 'startIndex': 5, 'endIndex': 6}, 'properties': {'pixelSize': 190}, 'fields': 'pixelSize'}},
                {'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'COLUMNS', 'startIndex': 6, 'endIndex': 7}, 'properties': {'pixelSize': 140}, 'fields': 'pixelSize'}},
                # Set row height for title row
                {
                    'updateDimensionProperties': {
                        'range': {
                            'sheetId': sheet_id,
                            'dimension': 'ROWS',
                            'startIndex': 0,
                            'endIndex': 1
                        },
                        'properties': {
                            'pixelSize': 40
                        },
                        'fields': 'pixelSize'
                    }
                }
            ]

            def _format_sheet():
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=DAILY_LOG_SPREADSHEET_ID,
                    body={'requests': requests}
                ).execute()

            robust_api_call(_format_sheet)

    except Exception as e:
        print(f"Warning: Could not apply sheet formatting: {str(e)}")


def main():
    """Main entry point for Daily Inventory Log."""
    try:
        if not SPECIFICATION_SHEET_ID:
            raise DailyLogError("SPECIFICATION_SHEET_ID environment variable not set")

        sheets_service, gspread_client = get_services()
        ensure_sheet_formatting(gspread_client, sheets_service)

        current_time = datetime.now(pytz.UTC).astimezone(WAT_TZ)
        date_components = format_date_components(current_time)

        balance_data = get_balance_sheet_data(sheets_service)
        total_weight_kg = calculate_whole_chicken_weight_kg(balance_data)
        inventory_tonnes = round(total_weight_kg / 1000, 2)
        below_10_tonnes = "Yes" if inventory_tonnes < 10 else "No"

        # Check if entry for today already exists
        existing_row, existing_entry_id = find_existing_entry_for_date(gspread_client, date_components['date'])

        if existing_row:
            # Update existing entry for today
            entry_data = {
                'entry_id': existing_entry_id,
                'date': date_components['date'],
                'year': date_components['year'],
                'month': date_components['month'],
                'state': 'Kaduna',
                'inventory_tonnes': inventory_tonnes,
                'below_10_tonnes': below_10_tonnes
            }
            update_log_entry(gspread_client, existing_row, entry_data)
            print(f"Daily Inventory Log completed - Entry #{existing_entry_id} updated for {date_components['date']}")
        else:
            # Create new entry
            entry_id = get_next_entry_id(gspread_client)
            entry_data = {
                'entry_id': entry_id,
                'date': date_components['date'],
                'year': date_components['year'],
                'month': date_components['month'],
                'state': 'Kaduna',
                'inventory_tonnes': inventory_tonnes,
                'below_10_tonnes': below_10_tonnes
            }
            append_log_entry(gspread_client, entry_data)
            print(f"Daily Inventory Log completed - Entry #{entry_id} added for {date_components['date']}")

    except DailyLogError as e:
        print(f"Daily Log Error: {str(e)}")
        raise
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise


if __name__ == '__main__':
    main()
