import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import os
import time
import random
from typing import Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from datetime import datetime, timedelta, timezone

GOOGLE_SHEETS_SCOPE = ['https://www.googleapis.com/auth/spreadsheets']

class DataProcessingError(Exception):
    """Custom exception for data processing errors"""
    pass

def get_wat_timestamp() -> str:
    """Get current timestamp in WAT timezone with AM/PM format"""
    # WAT is UTC+1
    utc_now = datetime.now(timezone.utc)
    wat_now = utc_now + timedelta(hours=1)

    # Format: "January 10, 2025 at 3:45 PM WAT"
    formatted_time = wat_now.strftime("%B %d, %Y at %-I:%M %p WAT")
    return formatted_time

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

def filter_data_from_current_year(df: pd.DataFrame) -> pd.DataFrame:
    """Filter data for January of current year onwards"""
    try:
        current_year = datetime.now().year
        print(f"\nFiltering data for Jan {current_year} onwards...")

        # Convert year_month to datetime for filtering
        df['date_filter'] = pd.to_datetime(df['year_month'], format='%Y-%m')

        # Filter for Jan of current year onwards
        jan_current_year = pd.to_datetime(f'{current_year}-01', format='%Y-%m')
        filtered_df = df[df['date_filter'] >= jan_current_year].copy()

        # Sort by date in ascending order (oldest first) for calculations
        filtered_df = filtered_df.sort_values('date_filter')

        print(f"Filtered to {len(filtered_df)} rows from Jan {current_year} onwards")
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

        # Round quantity columns to 0 decimal places
        quantity_cols = ['TOTAL INFLOW', 'TOTAL RELEASE', 'BALANCE', 'BIRD STORED']
        for col in quantity_cols:
            report_df[col] = report_df[col].round(0)

        # Round weight columns to 2 decimal places
        weight_cols = ['INFLOW WEIGHT', 'RELEASE WEIGHT', 'WEIGHT BALANCE', 'WEIGHT STORED']
        for col in weight_cols:
            report_df[col] = report_df[col].round(2)

        print(f"Whole chicken report created with {len(report_df)} rows")
        return report_df
    except Exception as e:
        raise DataProcessingError(f"Failed to create whole chicken report: {str(e)}")


def create_combined_report(df: pd.DataFrame) -> pd.DataFrame:
    """Create combined report with summed weights of chicken and gizzard (all absolute values)"""
    try:
        print("\nCreating combined report...")

        # Extract relevant columns
        report_df = pd.DataFrame()
        report_df['MONTH'] = df['year_month']

        # Get chicken columns (convert to numeric and apply absolute value)
        chicken_inflow = pd.to_numeric(df['total_whole_chicken_inflow_weight'], errors='coerce').fillna(0).abs()
        chicken_release = pd.to_numeric(df['total_whole_chicken_release_weight'], errors='coerce').fillna(0).abs()
        chicken_balance = pd.to_numeric(df['whole_chicken_weight_stock_balance'], errors='coerce').fillna(0).abs()

        # Get gizzard columns (convert to numeric and apply absolute value)
        gizzard_inflow = pd.to_numeric(df['total_gizzard_inflow_weight'], errors='coerce').fillna(0).abs()
        gizzard_release = pd.to_numeric(df['total_gizzard_release_weight'], errors='coerce').fillna(0).abs()
        gizzard_balance = pd.to_numeric(df['gizzard_weight_stock_balance'], errors='coerce').fillna(0).abs()

        # Sum chicken and gizzard weights
        report_df['INFLOW WEIGHT'] = (chicken_inflow + gizzard_inflow).abs()
        report_df['RELEASE WEIGHT'] = (chicken_release + gizzard_release).abs()
        report_df['WEIGHT BALANCE'] = (chicken_balance + gizzard_balance).abs()

        # Calculate WEIGHT STORED = current inflow weight + previous month weight balance (with absolute value)
        report_df['WEIGHT STORED'] = 0.0
        for i in range(len(report_df)):
            if i == 0:
                report_df.iloc[i, report_df.columns.get_loc('WEIGHT STORED')] = abs(report_df.iloc[i]['INFLOW WEIGHT'])
            else:
                report_df.iloc[i, report_df.columns.get_loc('WEIGHT STORED')] = \
                    abs(report_df.iloc[i]['INFLOW WEIGHT'] + report_df.iloc[i-1]['WEIGHT BALANCE'])

        # Round numeric columns to 2 decimal places
        numeric_cols = ['INFLOW WEIGHT', 'RELEASE WEIGHT', 'WEIGHT BALANCE', 'WEIGHT STORED']
        for col in numeric_cols:
            report_df[col] = report_df[col].round(2)

        print(f"Combined report created with {len(report_df)} rows")
        return report_df
    except Exception as e:
        raise DataProcessingError(f"Failed to create combined report: {str(e)}")

def prepare_df_for_upload(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare dataframe for upload to Google Sheets - preserve numeric types"""
    print("\nPreparing dataframe for upload...")
    df_copy = df.copy()

    # Process each column based on its type
    for col in df_copy.columns:
        if col == 'MONTH':
            # MONTH is text - convert to string
            df_copy[col] = df_copy[col].fillna('').astype(str)
        else:
            # All other columns are numeric - keep as float for proper formatting
            # Replace NaN with 0 for numeric columns
            df_copy[col] = df_copy[col].fillna(0)
            # Ensure they're float type (not string)
            df_copy[col] = df_copy[col].astype(float)

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
        avg_row = num_rows + 5  # Average row comes after all data rows (rows 1-3: headers, row 4: column headers, rows 5+: data)

        if report_type == 'whole_chicken':
            # Column K = TOTAL COST (manual input)
            # Column L = COST/UNIT = TOTAL COST / UNIT USED (K/J)
            # Column M = COST/BIRD = TOTAL COST / BIRD STORED (K/H)
            # Column N = COST/KG = TOTAL COST / WEIGHT STORED (K/I)
            for row in range(5, num_rows + 5):  # Start from row 5 (skip timestamp, methodology, formulas, column headers)
                formulas.append({
                    'range': f'{sheet_name}!L{row}',
                    'values': [[f'=IF(J{row}=0,"",K{row}/J{row})']]  # TOTAL COST / UNIT USED
                })
                formulas.append({
                    'range': f'{sheet_name}!M{row}',
                    'values': [[f'=IF(H{row}=0,"",K{row}/H{row})']]  # TOTAL COST / BIRD STORED
                })
                formulas.append({
                    'range': f'{sheet_name}!N{row}',
                    'values': [[f'=IF(I{row}=0,"",K{row}/I{row})']]  # TOTAL COST / WEIGHT STORED
                })

            # Add AVERAGE formulas
            formulas.append({
                'range': f'{sheet_name}!L{avg_row}',
                'values': [[f'=AVERAGE(L5:L{num_rows + 4})']]
            })
            formulas.append({
                'range': f'{sheet_name}!M{avg_row}',
                'values': [[f'=AVERAGE(M5:M{num_rows + 4})']]
            })
            formulas.append({
                'range': f'{sheet_name}!N{avg_row}',
                'values': [[f'=AVERAGE(N5:N{num_rows + 4})']]
            })
        else:  # gizzard or combined
            # Column G = TOTAL COST (manual input)
            # Column H = COST/UNIT = TOTAL COST / UNIT USED (G/F)
            # Column I = COST/KG = TOTAL COST / WEIGHT STORED (G/E)
            for row in range(5, num_rows + 5):
                formulas.append({
                    'range': f'{sheet_name}!H{row}',
                    'values': [[f'=IF(F{row}=0,"",G{row}/F{row})']]  # TOTAL COST / UNIT USED
                })
                formulas.append({
                    'range': f'{sheet_name}!I{row}',
                    'values': [[f'=IF(E{row}=0,"",G{row}/E{row})']]  # TOTAL COST / WEIGHT STORED
                })

            # Add AVERAGE formulas
            formulas.append({
                'range': f'{sheet_name}!H{avg_row}',
                'values': [[f'=AVERAGE(H5:H{num_rows + 4})']]
            })
            formulas.append({
                'range': f'{sheet_name}!I{avg_row}',
                'values': [[f'=AVERAGE(I5:I{num_rows + 4})']]
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
            manual_cols_start = 9  # J (UNIT USED)
            manual_cols_end = 11   # K (TOTAL COST)
            calc_cols_start = 11   # L (COST/UNIT, COST/BIRD, COST/KG)
        else:  # gizzard or combined
            total_cols = 9   # A-I
            our_cols = 5     # A-E
            manual_cols_start = 5  # F (UNIT USED)
            manual_cols_end = 7    # G (TOTAL COST)
            calc_cols_start = 7    # H (COST/UNIT, COST/KG)

        # 1. Merge cells in timestamp row (row 1)
        requests.append({
            'mergeCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1,
                    'startColumnIndex': 0,
                    'endColumnIndex': total_cols
                },
                'mergeType': 'MERGE_ALL'
            }
        })

        # 1a. Timestamp row formatting (row 1)
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
                        'backgroundColor': {'red': 0.85, 'green': 0.92, 'blue': 0.95},  # Light blue-grey
                        'textFormat': {
                            'bold': True,
                            'fontSize': 10,
                            'foregroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2}
                        },
                        'horizontalAlignment': 'LEFT',
                        'verticalAlignment': 'MIDDLE'
                    }
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)'
            }
        })

        # 2. Merge cells in methodology row (row 2)
        requests.append({
            'mergeCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1,
                    'endRowIndex': 2,
                    'startColumnIndex': 0,
                    'endColumnIndex': total_cols
                },
                'mergeType': 'MERGE_ALL'
            }
        })

        # 2a. Methodology row formatting (row 2)
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 1,
                    'endRowIndex': 2,
                    'startColumnIndex': 0,
                    'endColumnIndex': total_cols
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {'red': 0.996, 'green': 0.98, 'blue': 0.88},  # Light info background
                        'textFormat': {
                            'italic': True,
                            'fontSize': 9,
                            'foregroundColor': {'red': 0.3, 'green': 0.3, 'blue': 0.3}
                        },
                        'horizontalAlignment': 'LEFT',
                        'verticalAlignment': 'MIDDLE',
                        'wrapStrategy': 'WRAP'
                    }
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)'
            }
        })

        # 3. Merge cells in formula description row (row 3)
        requests.append({
            'mergeCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 2,
                    'endRowIndex': 3,
                    'startColumnIndex': 0,
                    'endColumnIndex': total_cols
                },
                'mergeType': 'MERGE_ALL'
            }
        })

        # 3a. Formula description row formatting (row 3)
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 2,
                    'endRowIndex': 3,
                    'startColumnIndex': 0,
                    'endColumnIndex': total_cols
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {'red': 0.95, 'green': 0.95, 'blue': 0.97},  # Light grey-purple
                        'textFormat': {
                            'fontSize': 8,
                            'foregroundColor': {'red': 0.4, 'green': 0.4, 'blue': 0.4}
                        },
                        'horizontalAlignment': 'LEFT',
                        'verticalAlignment': 'MIDDLE',
                        'wrapStrategy': 'WRAP'
                    }
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)'
            }
        })

        # 4. Column header row formatting (row 4, all columns)
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 3,
                    'endRowIndex': 4,
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

        # 5. Our data columns (light blue) - excluding average row
        # Data rows: row 5 to row (4 + num_rows) in 1-indexed terms
        # In 0-indexed API terms: startRowIndex 4 to endRowIndex (4 + num_rows)
        # Explicitly reset text formatting to ensure no bold/center from previous runs
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 4,
                    'endRowIndex': 4 + num_rows,
                    'startColumnIndex': 0,
                    'endColumnIndex': our_cols
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {'red': 0.88, 'green': 0.95, 'blue': 0.996},  # #E0F2FE
                        'textFormat': {
                            'bold': False
                        },
                        'horizontalAlignment': 'LEFT'
                    }
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
            }
        })

        # 6. Manual input columns (light yellow) - excluding average row
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 4,
                    'endRowIndex': 4 + num_rows,
                    'startColumnIndex': manual_cols_start,
                    'endColumnIndex': manual_cols_end
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {'red': 0.996, 'green': 0.95, 'blue': 0.78},  # #FEF3C7
                        'textFormat': {
                            'bold': False
                        },
                        'horizontalAlignment': 'LEFT'
                    }
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
            }
        })

        # 7. Calculated columns (light green) - excluding average row
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 4,
                    'endRowIndex': 4 + num_rows,
                    'startColumnIndex': calc_cols_start,
                    'endColumnIndex': total_cols
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {'red': 0.82, 'green': 0.98, 'blue': 0.898},  # #D1FAE5
                        'textFormat': {
                            'bold': False
                        },
                        'horizontalAlignment': 'LEFT'
                    }
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
            }
        })

        # 8. Format AVERAGE row distinctly (darker grey, bold)
        # AVERAGE row is at row (5 + num_rows) in 1-indexed terms (after all data rows)
        # In 0-indexed API terms: startRowIndex = 4 + num_rows, endRowIndex = 4 + num_rows + 1
        avg_row_index = 4 + num_rows
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

        # 9. Add borders to all cells (including timestamp, methodology, formulas, and average row)
        requests.append({
            'updateBorders': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 4 + num_rows + 1,  # Include all rows: 4 header rows + num_rows data rows + 1 average row
                    'startColumnIndex': 0,
                    'endColumnIndex': total_cols
                },
                'top': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.8, 'green': 0.8, 'blue': 0.8}},
                'bottom': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.8, 'green': 0.8, 'blue': 0.8}},
                'left': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.8, 'green': 0.8, 'blue': 0.8}},
                'right': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.8, 'green': 0.8, 'blue': 0.8}}
            }
        })

        # 10. Freeze first 4 rows (timestamp, methodology, formulas, column headers)
        requests.append({
            'updateSheetProperties': {
                'properties': {
                    'sheetId': sheet_id,
                    'gridProperties': {
                        'frozenRowCount': 4
                    }
                },
                'fields': 'gridProperties.frozenRowCount'
            }
        })

        # 11. Auto-resize all columns to fit content properly
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

        # 12. Set minimum column widths to ensure readability
        for col_index in range(total_cols):
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': col_index,
                        'endIndex': col_index + 1
                    },
                    'properties': {
                        'pixelSize': 130  # Minimum width for proper number display
                    },
                    'fields': 'pixelSize'
                }
            })

        # Execute all visual formatting in one batch request
        def _apply_formatting():
            return service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'requests': requests}
            ).execute()

        robust_sheets_operation(_apply_formatting)
        print(f"Visual formatting applied successfully to {sheet_name}")

    except Exception as e:
        print(f"Warning: Failed to apply formatting: {str(e)}")

def apply_number_formatting(service: Any, spreadsheet_id: str, sheet_name: str, report_type: str, num_rows: int):
    """Apply number formatting with thousand separators as a separate operation"""
    try:
        print(f"Applying number formatting to {sheet_name}...")

        sheet_id = get_sheet_id(service, spreadsheet_id, sheet_name)
        if sheet_id is None:
            print(f"Warning: Could not find sheet {sheet_name} for number formatting")
            return

        requests = []

        # Apply number formatting with thousand separators
        # Quantity columns: #,##0 (0 decimal places)
        # Weight/Money columns: #,##0.00 (2 decimal places)
        if report_type == 'whole_chicken':
            # Quantity columns with 0 dp: B (TOTAL INFLOW), D (TOTAL RELEASE), F (BALANCE), H (BIRD STORED)
            for col_idx in [1, 3, 5, 7]:  # B, D, F, H
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 4,
                            'endRowIndex': 4 + num_rows,
                            'startColumnIndex': col_idx,
                            'endColumnIndex': col_idx + 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'numberFormat': {
                                    'type': 'NUMBER',
                                    'pattern': '#,##0'
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.numberFormat'
                    }
                })

            # Weight columns with 2 dp: C (INFLOW WEIGHT), E (RELEASE WEIGHT), G (WEIGHT BALANCE), I (WEIGHT STORED)
            for col_idx in [2, 4, 6, 8]:  # C, E, G, I
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 4,
                            'endRowIndex': 4 + num_rows,
                            'startColumnIndex': col_idx,
                            'endColumnIndex': col_idx + 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'numberFormat': {
                                    'type': 'NUMBER',
                                    'pattern': '#,##0.00'
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.numberFormat'
                    }
                })

            # Manual input: J (UNIT USED) - 0 dp
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 4,
                        'endRowIndex': 4 + num_rows,
                        'startColumnIndex': 9,  # Column J
                        'endColumnIndex': 10
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'NUMBER',
                                'pattern': '#,##0'
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat'
                }
            })

            # Manual input: K (TOTAL COST) - 2 dp with Naira symbol
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 4,
                        'endRowIndex': 4 + num_rows,
                        'startColumnIndex': 10,  # Column K
                        'endColumnIndex': 11     # Column K only
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'NUMBER',
                                'pattern': '₦#,##0.00'
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat'
                }
            })

            # Formula columns L-N (COST/UNIT, COST/BIRD, COST/KG) - 2 dp with Naira symbol, includes average row
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 4,
                        'endRowIndex': 4 + num_rows + 1,  # Include average row
                        'startColumnIndex': 11,  # Column L
                        'endColumnIndex': 14     # Column N
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'NUMBER',
                                'pattern': '₦#,##0.00'
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat'
                }
            })
        else:  # gizzard or combined
            # Weight columns B-E (INFLOW WEIGHT, RELEASE WEIGHT, WEIGHT BALANCE, WEIGHT STORED) - 2 dp
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 4,
                        'endRowIndex': 4 + num_rows,
                        'startColumnIndex': 1,  # Column B
                        'endColumnIndex': 5     # Up to column E
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'NUMBER',
                                'pattern': '#,##0.00'
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat'
                }
            })

            # Manual input: F (UNIT USED) - 0 dp
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 4,
                        'endRowIndex': 4 + num_rows,
                        'startColumnIndex': 5,  # Column F
                        'endColumnIndex': 6
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'NUMBER',
                                'pattern': '#,##0'
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat'
                }
            })

            # Manual input: G (TOTAL COST) - 2 dp with Naira symbol
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 4,
                        'endRowIndex': 4 + num_rows,
                        'startColumnIndex': 6,  # Column G
                        'endColumnIndex': 7     # Column G only
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'NUMBER',
                                'pattern': '₦#,##0.00'
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat'
                }
            })

            # Formula columns H-I (COST/UNIT, COST/KG) - 2 dp with Naira symbol, includes average row
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 4,
                        'endRowIndex': 4 + num_rows + 1,  # Include average row
                        'startColumnIndex': 7,  # Column H
                        'endColumnIndex': 9     # Column I
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'NUMBER',
                                'pattern': '₦#,##0.00'
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat'
                }
            })

        # Execute number formatting as a separate batch request
        if requests:
            def _apply_number_formatting():
                return service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={'requests': requests}
                ).execute()

            robust_sheets_operation(_apply_number_formatting)
            print(f"Number formatting applied successfully to {sheet_name}")

    except Exception as e:
        print(f"Warning: Failed to apply number formatting: {str(e)}")

def apply_conditional_formatting(service: Any, spreadsheet_id: str, sheet_name: str, report_type: str, num_rows: int):
    """Apply conditional formatting to highlight COST/KG > ₦250 in red"""
    try:
        print(f"Applying conditional formatting to {sheet_name}...")

        sheet_id = get_sheet_id(service, spreadsheet_id, sheet_name)
        if sheet_id is None:
            print(f"Warning: Could not find sheet {sheet_name} for conditional formatting")
            return

        requests = []

        # Determine COST/KG column based on report type
        if report_type == 'whole_chicken':
            cost_kg_col = 13  # Column N (0-indexed)
        else:  # combined
            cost_kg_col = 8   # Column I (0-indexed)

        # Add conditional formatting rule for COST/KG > 250
        requests.append({
            'addConditionalFormatRule': {
                'rule': {
                    'ranges': [{
                        'sheetId': sheet_id,
                        'startRowIndex': 4,  # Data rows start at row 5 (0-indexed: 4)
                        'endRowIndex': 4 + num_rows,  # Exclude AVERAGE row
                        'startColumnIndex': cost_kg_col,
                        'endColumnIndex': cost_kg_col + 1
                    }],
                    'booleanRule': {
                        'condition': {
                            'type': 'NUMBER_GREATER',
                            'values': [{'userEnteredValue': '250'}]
                        },
                        'format': {
                            'backgroundColor': {'red': 1.0, 'green': 0.8, 'blue': 0.8},  # Light red
                            'textFormat': {
                                'foregroundColor': {'red': 0.6, 'green': 0.0, 'blue': 0.0},  # Dark red text
                                'bold': True
                            }
                        }
                    }
                },
                'index': 0
            }
        })

        # Execute conditional formatting
        if requests:
            def _apply_conditional_formatting():
                return service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={'requests': requests}
                ).execute()

            robust_sheets_operation(_apply_conditional_formatting)
            print(f"Conditional formatting applied successfully to {sheet_name}")

    except Exception as e:
        print(f"Warning: Failed to apply conditional formatting: {str(e)}")

def add_header_rows(service: Any, spreadsheet_id: str, sheet_name: str, report_type: str):
    """Add timestamp, methodology note, and formula description rows at the top of the sheet"""
    try:
        print(f"Adding header rows to {sheet_name}...")

        # Get current timestamp
        timestamp = get_wat_timestamp()
        timestamp_text = f"Last Updated: {timestamp}"

        # Define methodology notes based on report type
        if report_type == 'whole_chicken':
            methodology_note = "COST/BIRD shows cost per bird stored. For cost per kg, refer to the Combined Report."
            formula_note = "KEY FORMULAS: BIRD STORED = Current Inflow + Previous Balance | WEIGHT STORED = Current Inflow Weight + Previous Weight Balance"
        else:  # combined
            methodology_note = "This represents the true storage cost per kg, calculated by dividing total monthly cost by combined weight of all products (chicken + gizzard)."
            formula_note = "KEY FORMULAS: WEIGHT STORED = Current Inflow Weight + Previous Weight Balance"

        # Add timestamp, methodology, and formula description rows
        def _add_headers():
            return service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A1:A3',
                valueInputOption='RAW',
                body={'values': [[timestamp_text], [methodology_note], [formula_note]]}
            ).execute()

        robust_sheets_operation(_add_headers)
        print(f"Header rows added to {sheet_name}")

    except Exception as e:
        print(f"Warning: Failed to add header rows: {str(e)}")

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
                          'WEIGHT STORED', 'UNIT USED', 'TOTAL COST',
                          'COST/UNIT', 'COST/BIRD', 'COST/KG']
        else:  # gizzard or combined
            our_range = 'A:E'  # Our 5 columns
            all_headers = ['MONTH', 'INFLOW WEIGHT', 'RELEASE WEIGHT', 'WEIGHT BALANCE',
                          'WEIGHT STORED', 'UNIT USED', 'TOTAL COST', 'COST/UNIT', 'COST/KG']

        # Prepare values with full headers but only our data
        # Keep numbers as floats so Google Sheets applies number formatting
        values = [all_headers]  # Full header row
        for row in df_to_upload.values.tolist():
            row_values = []
            for i, cell in enumerate(row):
                if i == 0:  # MONTH column - keep as string
                    row_values.append(str(cell) if cell is not None else '')
                else:  # Numeric columns - keep as float
                    row_values.append(float(cell) if cell is not None and cell == cell else 0)
            values.append(row_values)

        # Clear only our columns
        def _clear_our_columns():
            return service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!{our_range}'
            ).execute()

        # Update with headers and our data (starting from row 4, after timestamp, methodology, and formula descriptions)
        # Use USER_ENTERED so Google Sheets interprets numeric strings as numbers
        # This allows the number formatting (#,##0.000) to be applied properly
        def _update_sheet():
            return service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A4',
                valueInputOption='USER_ENTERED',
                body={'values': values}
            ).execute()

        robust_sheets_operation(_clear_our_columns)
        result = robust_sheets_operation(_update_sheet)

        num_rows = len(df_to_upload)
        print(f"Updated {result.get('updatedCells')} cells in {sheet_name}")

        # Add timestamp, methodology, and formula description rows at the top
        add_header_rows(service, spreadsheet_id, sheet_name, report_type)

        # Add AVERAGE label in column A of average row
        avg_row = num_rows + 5
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

        # Apply visual formatting (colors, borders, etc.)
        format_sheet(service, spreadsheet_id, sheet_name, report_type, num_rows)

        # Apply number formatting as a SEPARATE operation AFTER all data and visual formatting
        # This ensures Google Sheets has fully processed the numeric values
        apply_number_formatting(service, spreadsheet_id, sheet_name, report_type, num_rows)

        # Apply conditional formatting for COST/KG > ₦250
        apply_conditional_formatting(service, spreadsheet_id, sheet_name, report_type, num_rows)

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

        # Filter for current year onwards
        filtered_df = filter_data_from_current_year(summary_df)

        if filtered_df.empty:
            current_year = datetime.now().year
            print(f"\nNo data found for Jan {current_year} onwards. Exiting.")
            return

        # Create reports
        chicken_report = create_whole_chicken_report(filtered_df)
        combined_report = create_combined_report(filtered_df)

        # Upload reports with their types
        upload_tasks = [
            (chicken_report, 'whole_chicken_report_2026', 'whole_chicken'),
            (combined_report, 'combined_report_2026', 'combined')
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
