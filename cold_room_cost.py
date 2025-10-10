import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import os
import time
import random
from typing import Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

GOOGLE_SHEETS_SCOPE = ['https://www.googleapis.com/auth/spreadsheets']

class DataProcessingError(Exception):
    """Custom exception for data processing errors"""
    pass

def is_rate_limit_error(exception):
    """Check if the exception is a rate limit error"""
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
    before_sleep=lambda retry_state: print(f"Rate limit hit, retrying in {retry_state.next_action.sleep} seconds... (attempt {retry_state.attempt_number})")
)
def robust_sheets_operation(operation_func, *args, **kwargs):
    """Execute sheets operation with robust retry logic"""
    try:
        return operation_func(*args, **kwargs)
    except Exception as e:
        if is_rate_limit_error(e):
            time.sleep(random.uniform(0.5, 2.0))
        raise

def get_credentials(credentials_file: str) -> service_account.Credentials:
    """Create and return credentials for Google Sheets access"""
    try:
        return service_account.Credentials.from_service_account_file(
            credentials_file,
            scopes=GOOGLE_SHEETS_SCOPE
        )
    except Exception as e:
        raise DataProcessingError(f"Failed to create credentials: {str(e)}")

def read_summary_sheet(service: Any, spreadsheet_id: str) -> pd.DataFrame:
    """Read the summary sheet from the ETL spreadsheet"""
    try:
        print("\nReading summary sheet from ETL spreadsheet...")

        def _get_summary_data():
            return service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range='summary!A:ZZ'
            ).execute()

        result = robust_sheets_operation(_get_summary_data)
        values = result.get('values', [])

        if not values:
            raise DataProcessingError("No data found in summary sheet")

        headers = values[0]
        data = values[1:]
        df = pd.DataFrame(data, columns=headers)

        print(f"Loaded {len(df)} rows from summary sheet")
        return df
    except Exception as e:
        raise DataProcessingError(f"Failed to read summary sheet: {str(e)}")

def filter_data_from_jan_2025(df: pd.DataFrame) -> pd.DataFrame:
    """Filter data for Jan 2025 onwards"""
    try:
        print("\nFiltering data for Jan 2025 onwards...")

        # Convert year_month to datetime for filtering
        df['date_filter'] = pd.to_datetime(df['year_month'], format='%Y-%m')

        # Filter for Jan 2025 onwards
        jan_2025 = pd.to_datetime('2025-01', format='%Y-%m')
        filtered_df = df[df['date_filter'] >= jan_2025].copy()

        # Sort by date in ascending order (oldest first) for calculations
        filtered_df = filtered_df.sort_values('date_filter')

        print(f"Filtered to {len(filtered_df)} rows from Jan 2025 onwards")
        return filtered_df
    except Exception as e:
        raise DataProcessingError(f"Failed to filter data: {str(e)}")

def create_whole_chicken_report(df: pd.DataFrame) -> pd.DataFrame:
    """Create whole chicken report with calculated metrics"""
    try:
        print("\nCreating whole chicken report...")

        # Extract relevant columns
        report_df = pd.DataFrame()
        report_df['MONTH'] = df['year_month']

        # Get whole chicken columns (convert to numeric and apply absolute value)
        report_df['TOTAL INFLOW'] = pd.to_numeric(df['total_whole_chicken_inflow_quantity'], errors='coerce').fillna(0).abs()
        report_df['INFLOW WEIGHT'] = pd.to_numeric(df['total_whole_chicken_inflow_weight'], errors='coerce').fillna(0).abs()
        report_df['TOTAL RELEASE'] = pd.to_numeric(df['total_whole_chicken_release_quantity'], errors='coerce').fillna(0).abs()
        report_df['RELEASE WEIGHT'] = pd.to_numeric(df['total_whole_chicken_release_weight'], errors='coerce').fillna(0).abs()
        report_df['BALANCE'] = pd.to_numeric(df['whole_chicken_quantity_stock_balance'], errors='coerce').fillna(0).abs()
        report_df['WEIGHT BALANCE'] = pd.to_numeric(df['whole_chicken_weight_stock_balance'], errors='coerce').fillna(0).abs()

        # Calculate BIRD STORED = current inflow + previous month balance (with absolute value)
        report_df['BIRD STORED'] = 0.0
        for i in range(len(report_df)):
            if i == 0:
                report_df.iloc[i, report_df.columns.get_loc('BIRD STORED')] = abs(report_df.iloc[i]['TOTAL INFLOW'])
            else:
                report_df.iloc[i, report_df.columns.get_loc('BIRD STORED')] = \
                    abs(report_df.iloc[i]['TOTAL INFLOW'] + report_df.iloc[i-1]['BALANCE'])

        # Calculate WEIGHT STORED = current inflow weight + previous month weight balance (with absolute value)
        report_df['WEIGHT STORED'] = 0.0
        for i in range(len(report_df)):
            if i == 0:
                report_df.iloc[i, report_df.columns.get_loc('WEIGHT STORED')] = abs(report_df.iloc[i]['INFLOW WEIGHT'])
            else:
                report_df.iloc[i, report_df.columns.get_loc('WEIGHT STORED')] = \
                    abs(report_df.iloc[i]['INFLOW WEIGHT'] + report_df.iloc[i-1]['WEIGHT BALANCE'])

        # Round numeric columns to 3 decimal places
        numeric_cols = ['TOTAL INFLOW', 'INFLOW WEIGHT', 'TOTAL RELEASE', 'RELEASE WEIGHT',
                       'BALANCE', 'WEIGHT BALANCE', 'BIRD STORED', 'WEIGHT STORED']
        for col in numeric_cols:
            report_df[col] = report_df[col].round(3)

        print(f"Whole chicken report created with {len(report_df)} rows")
        return report_df
    except Exception as e:
        raise DataProcessingError(f"Failed to create whole chicken report: {str(e)}")

def create_gizzard_report(df: pd.DataFrame) -> pd.DataFrame:
    """Create gizzard report with weight metrics"""
    try:
        print("\nCreating gizzard report...")

        # Extract relevant columns
        report_df = pd.DataFrame()
        report_df['MONTH'] = df['year_month']

        # Get gizzard columns (convert to numeric and apply absolute value)
        report_df['INFLOW WEIGHT'] = pd.to_numeric(df['total_gizzard_inflow_weight'], errors='coerce').fillna(0).abs()
        report_df['RELEASE WEIGHT'] = pd.to_numeric(df['total_gizzard_release_weight'], errors='coerce').fillna(0).abs()
        report_df['WEIGHT BALANCE'] = pd.to_numeric(df['gizzard_weight_stock_balance'], errors='coerce').fillna(0).abs()

        # Calculate WEIGHT STORED = current inflow weight + previous month weight balance (with absolute value)
        report_df['WEIGHT STORED'] = 0.0
        for i in range(len(report_df)):
            if i == 0:
                report_df.iloc[i, report_df.columns.get_loc('WEIGHT STORED')] = abs(report_df.iloc[i]['INFLOW WEIGHT'])
            else:
                report_df.iloc[i, report_df.columns.get_loc('WEIGHT STORED')] = \
                    abs(report_df.iloc[i]['INFLOW WEIGHT'] + report_df.iloc[i-1]['WEIGHT BALANCE'])

        # Round numeric columns to 3 decimal places
        numeric_cols = ['INFLOW WEIGHT', 'RELEASE WEIGHT', 'WEIGHT BALANCE', 'WEIGHT STORED']
        for col in numeric_cols:
            report_df[col] = report_df[col].round(3)

        print(f"Gizzard report created with {len(report_df)} rows")
        return report_df
    except Exception as e:
        raise DataProcessingError(f"Failed to create gizzard report: {str(e)}")

def create_combined_report(chicken_df: pd.DataFrame, gizzard_df: pd.DataFrame) -> pd.DataFrame:
    """Create combined report with summed weights (all absolute values)"""
    try:
        print("\nCreating combined report...")

        # Create combined report
        report_df = pd.DataFrame()
        report_df['MONTH'] = chicken_df['MONTH']

        # Sum chicken and gizzard weights (apply absolute value to ensure no negatives)
        report_df['TOTAL INFLOW WEIGHT'] = (chicken_df['INFLOW WEIGHT'] + gizzard_df['INFLOW WEIGHT']).abs()
        report_df['TOTAL RELEASE WEIGHT'] = (chicken_df['RELEASE WEIGHT'] + gizzard_df['RELEASE WEIGHT']).abs()
        report_df['WEIGHT BALANCE'] = (chicken_df['WEIGHT BALANCE'] + gizzard_df['WEIGHT BALANCE']).abs()
        report_df['WEIGHT STORED'] = (chicken_df['WEIGHT STORED'] + gizzard_df['WEIGHT STORED']).abs()

        # Round numeric columns to 3 decimal places
        numeric_cols = ['TOTAL INFLOW WEIGHT', 'TOTAL RELEASE WEIGHT', 'WEIGHT BALANCE', 'WEIGHT STORED']
        for col in numeric_cols:
            report_df[col] = report_df[col].round(3)

        print(f"Combined report created with {len(report_df)} rows")
        return report_df
    except Exception as e:
        raise DataProcessingError(f"Failed to create combined report: {str(e)}")

def prepare_df_for_upload(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare dataframe for upload to Google Sheets"""
    print("\nPreparing dataframe for upload...")
    df_copy = df.copy()

    # Convert all columns to string
    for col in df_copy.columns:
        df_copy[col] = df_copy[col].fillna('')
        df_copy[col] = df_copy[col].astype(str)
        df_copy[col] = df_copy[col].replace('nan', '')

    return df_copy

def get_sheet_id(service: Any, spreadsheet_id: str, sheet_name: str) -> int:
    """Get the sheet ID for a given sheet name"""
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        for sheet in spreadsheet.get('sheets', []):
            if sheet['properties']['title'] == sheet_name:
                return sheet['properties']['sheetId']
        # If sheet doesn't exist, return None
        return None
    except Exception as e:
        print(f"Error getting sheet ID: {str(e)}")
        return None

def create_sheet_if_not_exists(service: Any, spreadsheet_id: str, sheet_name: str) -> int:
    """Create sheet if it doesn't exist and return sheet ID"""
    try:
        sheet_id = get_sheet_id(service, spreadsheet_id, sheet_name)
        if sheet_id is not None:
            return sheet_id

        # Create the sheet
        def _create_sheet():
            return service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    'requests': [{
                        'addSheet': {
                            'properties': {
                                'title': sheet_name
                            }
                        }
                    }]
                }
            ).execute()

        result = robust_sheets_operation(_create_sheet)
        return result['replies'][0]['addSheet']['properties']['sheetId']
    except Exception as e:
        raise DataProcessingError(f"Failed to create sheet {sheet_name}: {str(e)}")

def add_formulas_to_sheet(service: Any, spreadsheet_id: str, sheet_name: str, report_type: str, num_rows: int):
    """Add formulas for calculated columns and average row"""
    try:
        print(f"Adding formulas to {sheet_name}...")

        formulas = []
        avg_row = num_rows + 2  # Average row comes after all data rows

        if report_type == 'whole_chicken':
            # COST/BIRD = TOTAL COST / BIRD STORED (column M = L/I)
            # COST/KG = TOTAL COST / WEIGHT STORED (column N = L/J)
            for row in range(2, num_rows + 2):  # Start from row 2 (skip header)
                formulas.append({
                    'range': f'{sheet_name}!M{row}',
                    'values': [[f'=IF(I{row}=0,"",L{row}/I{row})']]  # Avoid division by zero
                })
                formulas.append({
                    'range': f'{sheet_name}!N{row}',
                    'values': [[f'=IF(J{row}=0,"",L{row}/J{row})']]
                })

            # Add AVERAGE formulas
            formulas.append({
                'range': f'{sheet_name}!M{avg_row}',
                'values': [[f'=AVERAGE(M2:M{num_rows + 1})']]
            })
            formulas.append({
                'range': f'{sheet_name}!N{avg_row}',
                'values': [[f'=AVERAGE(N2:N{num_rows + 1})']]
            })
        else:  # gizzard or combined
            # COST/KG = TOTAL COST / WEIGHT STORED (column I = H/E)
            for row in range(2, num_rows + 2):
                formulas.append({
                    'range': f'{sheet_name}!I{row}',
                    'values': [[f'=IF(E{row}=0,"",H{row}/E{row})']]
                })

            # Add AVERAGE formula for COST/KG
            formulas.append({
                'range': f'{sheet_name}!I{avg_row}',
                'values': [[f'=AVERAGE(I2:I{num_rows + 1})']]
            })

        # Batch update formulas
        if formulas:
            def _update_formulas():
                data = [{'range': f['range'], 'values': f['values']} for f in formulas]
                return service.spreadsheets().values().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={
                        'valueInputOption': 'USER_ENTERED',  # Allows formulas
                        'data': data
                    }
                ).execute()

            robust_sheets_operation(_update_formulas)
            print(f"Added {len(formulas)} formulas")

    except Exception as e:
        print(f"Warning: Failed to add formulas: {str(e)}")

def format_sheet(service: Any, spreadsheet_id: str, sheet_name: str, report_type: str, num_rows: int):
    """Apply professional formatting to the sheet using batch operations"""
    try:
        print(f"Applying formatting to {sheet_name}...")

        sheet_id = get_sheet_id(service, spreadsheet_id, sheet_name)
        if sheet_id is None:
            print(f"Warning: Could not find sheet {sheet_name} for formatting")
            return

        requests = []

        # Define column counts based on report type
        if report_type == 'whole_chicken':
            total_cols = 14  # A-N
            our_cols = 9     # A-I
            manual_cols_start = 9  # J
            manual_cols_end = 12   # L
            calc_cols_start = 12   # M
        else:  # gizzard or combined
            total_cols = 9   # A-I
            our_cols = 5     # A-E
            manual_cols_start = 5  # F
            manual_cols_end = 8    # H
            calc_cols_start = 8    # I

        # 1. Header row formatting (row 1, all columns)
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1,
                    'startColumnIndex': 0,
                    'endColumnIndex': total_cols
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {'red': 0.29, 'green': 0.33, 'blue': 0.41},  # #4A5568
                        'textFormat': {
                            'foregroundColor': {'red': 1, 'green': 1, 'blue': 1},
                            'bold': True,
                            'fontSize': 11
                        },
                        'horizontalAlignment': 'CENTER',
                        'verticalAlignment': 'MIDDLE'
                    }
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)'
            }
        })

        # 2. Our data columns (light blue) - excluding average row
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1,
                    'endRowIndex': num_rows + 1,
                    'startColumnIndex': 0,
                    'endColumnIndex': our_cols
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {'red': 0.88, 'green': 0.95, 'blue': 0.996}  # #E0F2FE
                    }
                },
                'fields': 'userEnteredFormat.backgroundColor'
            }
        })

        # 3. Manual input columns (light yellow) - excluding average row
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1,
                    'endRowIndex': num_rows + 1,
                    'startColumnIndex': manual_cols_start,
                    'endColumnIndex': manual_cols_end
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {'red': 0.996, 'green': 0.95, 'blue': 0.78}  # #FEF3C7
                    }
                },
                'fields': 'userEnteredFormat.backgroundColor'
            }
        })

        # 4. Calculated columns (light green) - excluding average row
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1,
                    'endRowIndex': num_rows + 1,
                    'startColumnIndex': calc_cols_start,
                    'endColumnIndex': total_cols
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {'red': 0.82, 'green': 0.98, 'blue': 0.898}  # #D1FAE5
                    }
                },
                'fields': 'userEnteredFormat.backgroundColor'
            }
        })

        # 5. Format AVERAGE row distinctly (darker grey, bold)
        avg_row_index = num_rows + 1
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': avg_row_index,
                    'endRowIndex': avg_row_index + 1,
                    'startColumnIndex': 0,
                    'endColumnIndex': total_cols
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9},  # Light grey
                        'textFormat': {
                            'bold': True,
                            'fontSize': 11
                        },
                        'horizontalAlignment': 'CENTER'
                    }
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
            }
        })

        # 6. Add borders to all cells (including average row)
        requests.append({
            'updateBorders': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': num_rows + 2,  # Include average row
                    'startColumnIndex': 0,
                    'endColumnIndex': total_cols
                },
                'top': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.8, 'green': 0.8, 'blue': 0.8}},
                'bottom': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.8, 'green': 0.8, 'blue': 0.8}},
                'left': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.8, 'green': 0.8, 'blue': 0.8}},
                'right': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.8, 'green': 0.8, 'blue': 0.8}}
            }
        })

        # 7. Freeze header row
        requests.append({
            'updateSheetProperties': {
                'properties': {
                    'sheetId': sheet_id,
                    'gridProperties': {
                        'frozenRowCount': 1
                    }
                },
                'fields': 'gridProperties.frozenRowCount'
            }
        })

        # 8. Auto-resize columns
        requests.append({
            'autoResizeDimensions': {
                'dimensions': {
                    'sheetId': sheet_id,
                    'dimension': 'COLUMNS',
                    'startIndex': 0,
                    'endIndex': total_cols
                }
            }
        })

        # Execute all formatting in one batch request
        def _apply_formatting():
            return service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'requests': requests}
            ).execute()

        robust_sheets_operation(_apply_formatting)
        print(f"Formatting applied successfully to {sheet_name}")

    except Exception as e:
        print(f"Warning: Failed to apply formatting: {str(e)}")

def upload_df_to_gsheet(df: pd.DataFrame,
                       spreadsheet_id: str,
                       sheet_name: str,
                       service: Any,
                       report_type: str) -> bool:
    """Upload dataframe to Google Sheets with partial clearing and full formatting"""
    try:
        print(f"\nUploading data to sheet: {sheet_name}...")

        # Ensure sheet exists
        create_sheet_if_not_exists(service, spreadsheet_id, sheet_name)

        df_to_upload = prepare_df_for_upload(df)

        # Determine column ranges based on report type
        if report_type == 'whole_chicken':
            our_range = 'A:I'  # Our 9 columns
            all_headers = ['MONTH', 'TOTAL INFLOW', 'INFLOW WEIGHT', 'TOTAL RELEASE',
                          'RELEASE WEIGHT', 'BALANCE', 'WEIGHT BALANCE', 'BIRD STORED',
                          'WEIGHT STORED', 'UNIT USED', 'TOTAL DEPOSIT', 'TOTAL COST',
                          'COST/BIRD', 'COST/KG']
        else:  # gizzard or combined
            our_range = 'A:E'  # Our 5 columns
            all_headers = ['MONTH', 'INFLOW WEIGHT', 'RELEASE WEIGHT', 'WEIGHT BALANCE',
                          'WEIGHT STORED', 'UNIT USED', 'TOTAL DEPOSIT', 'TOTAL COST', 'COST/KG']

        # Prepare values with full headers but only our data
        values = [all_headers]  # Full header row
        values.extend([[str(cell) if cell is not None and cell == cell else ''
                       for cell in row] for row in df_to_upload.values.tolist()])

        # Clear only our columns
        def _clear_our_columns():
            return service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!{our_range}'
            ).execute()

        # Update with headers and our data
        def _update_sheet():
            return service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A1',
                valueInputOption='RAW',
                body={'values': values}
            ).execute()

        robust_sheets_operation(_clear_our_columns)
        result = robust_sheets_operation(_update_sheet)

        num_rows = len(df_to_upload)
        print(f"Updated {result.get('updatedCells')} cells in {sheet_name}")

        # Add AVERAGE label in column A of average row
        avg_row = num_rows + 2
        def _add_average_label():
            return service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A{avg_row}',
                valueInputOption='RAW',
                body={'values': [['AVERAGE']]}
            ).execute()

        robust_sheets_operation(_add_average_label)

        # Add formulas for calculated columns
        add_formulas_to_sheet(service, spreadsheet_id, sheet_name, report_type, num_rows)

        # Apply formatting
        format_sheet(service, spreadsheet_id, sheet_name, report_type, num_rows)

        return True

    except Exception as e:
        print(f"Failed to upload to {sheet_name}: {str(e)}")
        return False

def main():
    CREDENTIALS_FILE = 'service-account.json'

    try:
        print("\n" + "="*60)
        print("Starting Cold Room Cost Analysis")
        print("="*60)

        # Get spreadsheet IDs from environment
        etl_spreadsheet_id = os.getenv('INVENTORY_ETL_SPREADSHEET_ID')
        analysis_spreadsheet_id = os.getenv('COLD_ROOM_ANALYSIS_SPREADSHEET_ID')

        if not etl_spreadsheet_id:
            raise DataProcessingError("INVENTORY_ETL_SPREADSHEET_ID environment variable not set")
        if not analysis_spreadsheet_id:
            raise DataProcessingError("COLD_ROOM_ANALYSIS_SPREADSHEET_ID environment variable not set")

        print("\nConnecting to ETL and analysis spreadsheets...")
        print("Spreadsheet IDs retrieved from environment variables")

        # Create credentials and service
        credentials = get_credentials(CREDENTIALS_FILE)
        sheets_service = build('sheets', 'v4', credentials=credentials)

        # Read summary sheet
        summary_df = read_summary_sheet(sheets_service, etl_spreadsheet_id)

        # Filter for Jan 2025 onwards
        filtered_df = filter_data_from_jan_2025(summary_df)

        if filtered_df.empty:
            print("\nNo data found for Jan 2025 onwards. Exiting.")
            return

        # Create reports
        chicken_report = create_whole_chicken_report(filtered_df)
        gizzard_report = create_gizzard_report(filtered_df)
        combined_report = create_combined_report(chicken_report, gizzard_report)

        # Upload reports with their types
        upload_tasks = [
            (chicken_report, 'whole_chicken_report', 'whole_chicken'),
            (gizzard_report, 'gizzard_report', 'gizzard'),
            (combined_report, 'combined_report', 'combined')
        ]

        success = True
        for df, sheet_name, report_type in upload_tasks:
            if not upload_df_to_gsheet(df, analysis_spreadsheet_id, sheet_name, sheets_service, report_type):
                success = False
                print(f"Failed to upload {sheet_name}")

        if success:
            print("\n" + "="*60)
            print("Cold Room Cost Analysis completed successfully!")
            print("="*60)
        else:
            raise DataProcessingError("Failed to upload one or more reports")

    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
