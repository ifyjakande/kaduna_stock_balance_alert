import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import gspread
import os
import time
import random
from typing import Tuple, Dict, List, Any
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

SHEET_NAMES = {
    'STOCK_INFLOW': 'stock_inflow',
    'RELEASE': 'release',
    'STOCK_INFLOW_CLEAN': 'stock_inflow_clean',
    'RELEASE_CLEAN': 'release_clean',
    'SUMMARY': 'summary'
}

DATE_FORMATS = ['%d %b %Y', '%d/%m/%y', '%d-%b-%Y']
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
            # Add jitter to prevent thundering herd
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

def connect_to_sheets(credentials: service_account.Credentials, spreadsheet_id: str) -> gspread.Spreadsheet:
    max_retries = 3

    for attempt in range(max_retries):
        try:
            # Use the newer approach to avoid deprecation warning
            gc = gspread.Client(auth=credentials)
            return gc.open_by_key(spreadsheet_id)
        except Exception as e:
            if attempt < max_retries - 1 and "500" in str(e):
                print(f"Attempt {attempt + 1} failed with 500 error, retrying in {2 ** attempt} seconds...")
                time.sleep(2 ** attempt)
                continue
            raise DataProcessingError(f"Failed to connect to Google Sheets after {attempt + 1} attempts: {str(e)}")

def read_worksheet_to_df(spreadsheet: gspread.Spreadsheet, worksheet_name: str) -> pd.DataFrame:
    try:
        def _get_worksheet_data():
            worksheet = spreadsheet.worksheet(worksheet_name)
            return worksheet.get_all_values()
        
        all_values = robust_sheets_operation(_get_worksheet_data)
        if not all_values:
            raise DataProcessingError(f"No data found in worksheet {worksheet_name}")
        
        headers = all_values[0]
        data = all_values[1:]
        df = pd.DataFrame(data, columns=headers)
        
        if 'date' in df.columns:
            print(f"\nProcessing dates in {worksheet_name}")
        
        return df
    except Exception as e:
        raise DataProcessingError(f"Failed to read worksheet {worksheet_name}: {str(e)}")

def standardize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    try:
        print("\nStandardizing dataframe...")
        
        df_clean = df.copy()
        
        # Standardize column names
        df_clean.columns = (df_clean.columns.str.lower()
                          .str.strip()
                          .str.replace(' ', '_')
                          .str.replace('-', '_'))
        
        # Handle the weight_in_kg to weight rename
        if 'weight_in_kg' in df_clean.columns:
            df_clean = df_clean.rename(columns={'weight_in_kg': 'weight'})
        
        for column in df_clean.columns:
            df_clean[column] = df_clean[column].astype(str).str.strip().str.lower()
            try:
                numeric_values = pd.to_numeric(df_clean[column].str.replace(',', ''))
                df_clean[column] = numeric_values
            except (ValueError, TypeError):
                pass
        
        return df_clean
    except Exception as e:
        raise DataProcessingError(f"Failed to standardize dataframe: {str(e)}")

def standardize_dates(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        # Add required columns even for empty dataframe to avoid KeyError downstream
        df = df.copy()
        df['month'] = pd.Series(dtype='str')
        df['year_month'] = pd.Series(dtype='str')
        print("Empty dataframe - added month and year_month columns")
        return df

    try:
        print("\nStandardizing dates...")
        df = df.copy()
        
        date_parsed = False
        for format in DATE_FORMATS:
            try:
                print(f"Trying date format: {format}")
                df['date'] = pd.to_datetime(df['date'], format=format)
                date_parsed = True
                print("Successfully parsed dates using format:", format)
                break
            except ValueError as e:
                print(f"Failed to parse with format {format}: {str(e)}")
                continue
        
        if not date_parsed:
            print("Falling back to mixed format parsing")
            df['date'] = pd.to_datetime(df['date'], format='mixed', dayfirst=True)
        
        if df['date'].isna().any():
            problematic_dates = df[df['date'].isna()]['date'].unique()
            print(f"Warning: Failed to parse {len(problematic_dates)} date entries")
            raise DataProcessingError(f"Failed to parse {len(problematic_dates)} date entries")
        
        df['month'] = df['date'].dt.strftime('%b').str.lower()
        df['year_month'] = df['date'].dt.strftime('%Y-%b')
        
        return df
    except Exception as e:
        raise DataProcessingError(f"Failed to standardize dates: {str(e)}")

def remove_opening_stock(df: pd.DataFrame, column_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    try:
        print(f"\nRemoving opening stock using column: {column_name}")
        opening_stock_mask = df[column_name].str.contains('opening stock', case=False, na=False)
        opening_stock_df = df[opening_stock_mask].copy()
        main_df = df[~opening_stock_mask].copy()
        
        print(f"Remaining entries: {len(main_df)}")
        
        return main_df, opening_stock_df
    except Exception as e:
        raise DataProcessingError(f"Failed to separate opening stock: {str(e)}")

def create_summary_df(stock_inflow_df: pd.DataFrame, release_df: pd.DataFrame) -> pd.DataFrame:
    try:
        
        all_year_months = sorted(list(set(stock_inflow_df['year_month'].unique()) | 
                                    set(release_df['year_month'].unique())))
        
        summary_df = pd.DataFrame({'year_month': all_year_months})
        summary_df['month'] = summary_df['year_month'].str.split('-').str[1].str.lower()
        summary_df = summary_df[['month', 'year_month']]
        
        # Get unique product types dynamically from the data
        
        # Create dynamic product summaries for both inflow and release
        product_summaries = {}
        
        # Get unique product types from both inflow and release data
        # Standardize casing to match inflow format (title case)
        inflow_products = stock_inflow_df['product_type'].dropna().unique()
        
        if 'product' in release_df.columns:
            release_products = release_df['product'].dropna().unique()
        else:
            release_products = []
        
        # Process inflow data for each product type
        for product_type in inflow_products:
            product_data = stock_inflow_df[stock_inflow_df['product_type'] == product_type]
            if not product_data.empty:
                agg_dict = {'weight': 'sum'}
                # Add quantity aggregation for products that have quantity data
                if 'quantity' in product_data.columns and product_data['quantity'].notna().any():
                    agg_dict['quantity'] = 'sum'
                
                product_summaries[f'{product_type}_inflow'] = product_data.groupby('year_month').agg(agg_dict)
        
        # Process release data for each product type
        for product_type in release_products:
            product_data = release_df[release_df['product'] == product_type]
            if not product_data.empty:
                agg_dict = {'weight': 'sum'}
                # Add quantity aggregation for products that have quantity data
                if 'quantity' in product_data.columns and product_data['quantity'].notna().any():
                    agg_dict['quantity'] = 'sum'
                
                product_summaries[f'{product_type}_release'] = product_data.groupby('year_month').agg(agg_dict)
        
        # Create dynamic summary columns for inflow and release
        summary_columns = {}
        
        # Add inflow columns for each product type
        for product_type in inflow_products:
            product_key = f'{product_type}_inflow'
            if product_key in product_summaries:
                summary_key = product_type.replace(' ', '_').lower()
                if 'quantity' in product_summaries[product_key].columns:
                    summary_columns[f'total_{summary_key}_inflow_quantity'] = (product_key, 'quantity')
                if 'weight' in product_summaries[product_key].columns:
                    summary_columns[f'total_{summary_key}_inflow_weight'] = (product_key, 'weight')
        
        # Add release columns for each product type
        for product_type in release_products:
            product_key = f'{product_type}_release'
            if product_key in product_summaries:
                summary_key = product_type.replace(' ', '_').lower()
                if 'quantity' in product_summaries[product_key].columns:
                    summary_columns[f'total_{summary_key}_release_quantity'] = (product_key, 'quantity')
                if 'weight' in product_summaries[product_key].columns:
                    summary_columns[f'total_{summary_key}_release_weight'] = (product_key, 'weight')
        
        for col_name, (summary_key, metric) in summary_columns.items():
            if metric in product_summaries[summary_key].columns:
                summary_df[col_name] = summary_df['year_month'].map(
                    product_summaries[summary_key][metric]).fillna(0)
            else:
                summary_df[col_name] = 0

        # Add customer type breakdown columns for specific products
        target_products = ['whole chicken', 'gizzard']
        
        if 'customer_type' in release_df.columns:
            # Get unique customer types
            customer_types = release_df['customer_type'].dropna().unique()
            
            for target_product in target_products:
                # Check if this product exists in release data
                product_exists = release_df['product'].str.contains(target_product, case=False, na=False).any()
                
                if product_exists:
                    for customer_type in customer_types:
                        # Filter data for specific product and customer type
                        filtered_data = release_df[
                            (release_df['product'].str.contains(target_product, case=False, na=False)) &
                            (release_df['customer_type'] == customer_type)
                        ]
                        
                        if not filtered_data.empty:
                            # Clean customer type name for column naming
                            clean_customer_type = customer_type.replace(' ', '_').replace('-', '_').lower()
                            clean_product = target_product.replace(' ', '_').lower()
                            
                            # Create aggregation dictionary
                            agg_dict = {}
                            if 'quantity' in filtered_data.columns and filtered_data['quantity'].notna().any():
                                agg_dict['quantity'] = 'sum'
                            if 'weight' in filtered_data.columns and filtered_data['weight'].notna().any():
                                agg_dict['weight'] = 'sum'
                            
                            if agg_dict:
                                # Aggregate by year_month
                                customer_product_summary = filtered_data.groupby('year_month').agg(agg_dict)
                                
                                # Add columns to summary_df
                                if 'quantity' in agg_dict:
                                    col_name = f'{clean_product}_release_{clean_customer_type}_quantity'
                                    summary_df[col_name] = summary_df['year_month'].map(
                                        customer_product_summary['quantity']).fillna(0)
                                
                                if 'weight' in agg_dict:
                                    col_name = f'{clean_product}_release_{clean_customer_type}_weight'
                                    summary_df[col_name] = summary_df['year_month'].map(
                                        customer_product_summary['weight']).fillna(0)

            # Validation: Ensure customer type columns sum to total columns
            for target_product in target_products:
                clean_product = target_product.replace(' ', '_').lower()
                
                # Check quantity validation
                total_qty_col = f'total_{clean_product}_release_quantity'
                if total_qty_col in summary_df.columns:
                    customer_qty_cols = [col for col in summary_df.columns 
                                       if col.startswith(f'{clean_product}_release_') 
                                       and col.endswith('_quantity')]
                    
                    if customer_qty_cols:
                        customer_sum = summary_df[customer_qty_cols].sum(axis=1)
                        total_values = summary_df[total_qty_col]
                        
                        # Allow for small floating point differences
                        tolerance = 0.001
                        discrepancies = abs(customer_sum - total_values) > tolerance
                        
                        if discrepancies.any():
                            raise DataProcessingError(f"Customer type quantity validation failed for {target_product}. Contact administrator to review data integrity.")
                
                # Check weight validation
                total_wt_col = f'total_{clean_product}_release_weight'
                if total_wt_col in summary_df.columns:
                    customer_wt_cols = [col for col in summary_df.columns 
                                      if col.startswith(f'{clean_product}_release_') 
                                      and col.endswith('_weight')]
                    
                    if customer_wt_cols:
                        customer_sum = summary_df[customer_wt_cols].sum(axis=1)
                        total_values = summary_df[total_wt_col]
                        
                        # Allow for small floating point differences
                        tolerance = 0.001
                        discrepancies = abs(customer_sum - total_values) > tolerance
                        
                        if discrepancies.any():
                            raise DataProcessingError(f"Customer type weight validation failed for {target_product}. Contact administrator to review data integrity.")

        # Sort by year_month in ascending order to process chronologically
        summary_df['sort_date'] = pd.to_datetime(summary_df['year_month'], format='%Y-%b')
        summary_df = summary_df.sort_values('sort_date')

        # Create dynamic opening stock and stock balance columns
        opening_stock_columns = []
        stock_balance_columns = []
        
        # Create opening stock and balance columns for each product type
        all_products = set(inflow_products) | set(release_products)
        for product_type in all_products:
            summary_key = product_type.replace(' ', '_').lower()
            
            # Add quantity columns if product has quantity data
            inflow_key = f'{product_type}_inflow'
            if inflow_key in product_summaries and 'quantity' in product_summaries[inflow_key].columns:
                opening_stock_columns.append(f'{summary_key}_quantity_opening_stock')
                stock_balance_columns.append(f'{summary_key}_quantity_stock_balance')
            
            # Add weight columns if product has weight data
            if (inflow_key in product_summaries and 'weight' in product_summaries[inflow_key].columns) or \
               (f'{product_type}_release' in product_summaries and 'weight' in product_summaries[f'{product_type}_release'].columns):
                opening_stock_columns.append(f'{summary_key}_weight_opening_stock')
                stock_balance_columns.append(f'{summary_key}_weight_stock_balance')
        
        # Initialize all opening stock and balance columns
        for column in opening_stock_columns + stock_balance_columns:
            summary_df[column] = 0.0

        # Calculate running balances for each month
        for i in range(len(summary_df)):
            for product_type in all_products:
                summary_key = product_type.replace(' ', '_').lower()
                
                # Handle quantity columns
                qty_opening_col = f'{summary_key}_quantity_opening_stock'
                qty_balance_col = f'{summary_key}_quantity_stock_balance'
                qty_inflow_col = f'total_{summary_key}_inflow_quantity'
                qty_release_col = f'total_{summary_key}_release_quantity'
                
                if qty_opening_col in summary_df.columns and qty_balance_col in summary_df.columns:
                    if i == 0:
                        summary_df.iloc[i, summary_df.columns.get_loc(qty_opening_col)] = 0
                    else:
                        summary_df.iloc[i, summary_df.columns.get_loc(qty_opening_col)] = \
                            summary_df.iloc[i-1, summary_df.columns.get_loc(qty_balance_col)]
                    
                    # Calculate balance
                    opening_stock = summary_df.iloc[i, summary_df.columns.get_loc(qty_opening_col)]
                    inflow = summary_df.iloc[i, summary_df.columns.get_loc(qty_inflow_col)] if qty_inflow_col in summary_df.columns else 0
                    release = summary_df.iloc[i, summary_df.columns.get_loc(qty_release_col)] if qty_release_col in summary_df.columns else 0
                    summary_df.iloc[i, summary_df.columns.get_loc(qty_balance_col)] = opening_stock + inflow - release
                
                # Handle weight columns
                wt_opening_col = f'{summary_key}_weight_opening_stock'
                wt_balance_col = f'{summary_key}_weight_stock_balance'
                wt_inflow_col = f'total_{summary_key}_inflow_weight'
                wt_release_col = f'total_{summary_key}_release_weight'
                
                if wt_opening_col in summary_df.columns and wt_balance_col in summary_df.columns:
                    if i == 0:
                        summary_df.iloc[i, summary_df.columns.get_loc(wt_opening_col)] = 0
                    else:
                        summary_df.iloc[i, summary_df.columns.get_loc(wt_opening_col)] = \
                            summary_df.iloc[i-1, summary_df.columns.get_loc(wt_balance_col)]
                    
                    # Calculate balance
                    opening_stock = summary_df.iloc[i, summary_df.columns.get_loc(wt_opening_col)]
                    inflow = summary_df.iloc[i, summary_df.columns.get_loc(wt_inflow_col)] if wt_inflow_col in summary_df.columns else 0
                    release = summary_df.iloc[i, summary_df.columns.get_loc(wt_release_col)] if wt_release_col in summary_df.columns else 0
                    summary_df.iloc[i, summary_df.columns.get_loc(wt_balance_col)] = opening_stock + inflow - release

        # Sort in descending order (newest first) and clean up
        summary_df = summary_df.sort_values('sort_date', ascending=False)
        summary_df['year_month'] = summary_df['sort_date'].dt.strftime('%Y-%m')
        summary_df = summary_df.drop('sort_date', axis=1)
        
        # Format all numeric columns to 3 decimal places
        numeric_columns = summary_df.select_dtypes(include=['float64', 'int64']).columns
        for col in numeric_columns:
            summary_df[col] = summary_df[col].astype(float).round(3)
        
        return summary_df
    except Exception as e:
        raise DataProcessingError(f"Failed to create summary: {str(e)}")

def prepare_df_for_upload(df: pd.DataFrame) -> pd.DataFrame:
    print("\nPreparing dataframe for upload...")
    df_copy = df.copy()
    
    date_columns = df_copy.select_dtypes(include=['datetime64']).columns
    for col in date_columns:
        df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d')
    
    for col in df_copy.columns:
        df_copy[col] = df_copy[col].fillna('')
        df_copy[col] = df_copy[col].astype(str)
        df_copy[col] = df_copy[col].replace('nan', '')
    
    return df_copy

def upload_df_to_gsheet(df: pd.DataFrame, 
                       spreadsheet_id: str, 
                       sheet_name: str, 
                       service: Any) -> bool:
    try:
        print(f"\nUploading data to sheet: {sheet_name}")
        df_to_upload = prepare_df_for_upload(df)
        
        values = [df_to_upload.columns.tolist()]
        values.extend([[str(cell) if cell is not None and cell == cell else '' 
                       for cell in row] for row in df_to_upload.values.tolist()])
        
        def _clear_sheet():
            return service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A1:ZZ'
            ).execute()
        
        def _update_sheet():
            return service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A1',
                valueInputOption='RAW',
                body={'values': values}
            ).execute()
        
        robust_sheets_operation(_clear_sheet)
        result = robust_sheets_operation(_update_sheet)
        
        print(f"Updated {result.get('updatedCells')} cells in {sheet_name}")
        return True
        
    except Exception as e:
        print(f"Failed to upload to {sheet_name}: {str(e)}")
        return False

def process_sheets_data(stock_inflow_df: pd.DataFrame, 
                       release_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    try:
        print("\nProcessing sheets data...")
        
        stock_inflow_df = standardize_dataframe(stock_inflow_df)
        release_df = standardize_dataframe(release_df)
        
        # Check for missing dates and throw error if found
        missing_dates_inflow = stock_inflow_df['date'].isna() | (stock_inflow_df['date'] == '')
        missing_dates_release = release_df['date'].isna() | (release_df['date'] == '')
        
        if missing_dates_inflow.any():
            missing_count = missing_dates_inflow.sum()
            raise DataProcessingError(f"Found {missing_count} records with missing dates in stock_inflow sheet. All records must have valid dates.")
        
        if missing_dates_release.any():
            missing_count = missing_dates_release.sum()
            raise DataProcessingError(f"Found {missing_count} records with missing dates in release sheet. All records must have valid dates.")
        
        stock_inflow_df = standardize_dates(stock_inflow_df)
        release_df = standardize_dates(release_df)

        # Skip validation and processing if dataframes are empty
        if release_df.empty:
            print("Release dataframe is empty - skipping validation")
        else:
            # Validate customer_type column exists and has no missing values
            if 'customer_type' not in release_df.columns:
                raise DataProcessingError("customer_type column is missing from release sheet. All records must have customer type values.")

            # Check for various forms of missing customer type values
            missing_customer_types = (
                release_df['customer_type'].isna() |
                (release_df['customer_type'] == '') |
                (release_df['customer_type'].str.strip() == '') |
                (release_df['customer_type'].str.lower() == 'nan') |
                (release_df['customer_type'].str.lower() == 'none')
            )

            if missing_customer_types.any():
                missing_count = missing_customer_types.sum()
                # Get row indices for better error reporting (adding 2 to account for header and 0-indexing)
                missing_rows = release_df[missing_customer_types].index + 2
                row_list = ', '.join(map(str, missing_rows.tolist()[:10]))  # Show first 10 rows
                row_suffix = f" (showing first 10)" if len(missing_rows) > 10 else ""

                raise DataProcessingError(
                    f"Found {missing_count} records with missing customer_type values in release sheet. "
                    f"All records must have valid customer type values. "
                    f"Check spreadsheet rows: {row_list}{row_suffix}"
                )

            # Standardize product names to match between inflow and release
            if 'product' in release_df.columns:
                # Convert release product names to match inflow format (lowercase)
                release_df['product'] = release_df['product'].str.lower()

            release_df.loc[
                release_df['product'].str.contains('gizzard',
                                                 case=False, na=False),
                'quantity'
            ] = 0

        stock_inflow_main_df = stock_inflow_df
        
        summary_df = create_summary_df(stock_inflow_main_df, release_df)
        
        return stock_inflow_main_df, release_df, summary_df
    
    except Exception as e:
        raise DataProcessingError(f"Failed to process sheets data: {str(e)}")

def main():
    CREDENTIALS_FILE = 'service-account.json'
    
    try:
        print("\nStarting data processing...")
        
        source_spreadsheet_id = os.getenv('INVENTORY_SHEET_ID')
        output_spreadsheet_id = os.getenv('INVENTORY_ETL_SPREADSHEET_ID')
        
        if not source_spreadsheet_id:
            raise DataProcessingError("INVENTORY_SHEET_ID environment variable not set")
        if not output_spreadsheet_id:
            raise DataProcessingError("INVENTORY_ETL_SPREADSHEET_ID environment variable not set")
            
        # Create credentials and services once
        credentials = get_credentials(CREDENTIALS_FILE)
        source_spreadsheet = connect_to_sheets(credentials, source_spreadsheet_id)
        sheets_service = build('sheets', 'v4', credentials=credentials)
        
        # Read the worksheets from source
        stock_inflow_df = read_worksheet_to_df(source_spreadsheet, SHEET_NAMES['STOCK_INFLOW'])
        release_df = read_worksheet_to_df(source_spreadsheet, SHEET_NAMES['RELEASE'])
        
        # Process the data
        stock_inflow_main_df, release_df, summary_df = process_sheets_data(
            stock_inflow_df, release_df)
        
        # Define upload tasks
        upload_tasks = [
            (stock_inflow_main_df, SHEET_NAMES['STOCK_INFLOW_CLEAN']),
            (release_df, SHEET_NAMES['RELEASE_CLEAN']),
            (summary_df, SHEET_NAMES['SUMMARY'])
        ]
        
        # Upload all datasets
        success = True
        for df, sheet_name in upload_tasks:
            if not upload_df_to_gsheet(df, output_spreadsheet_id, sheet_name, sheets_service):
                success = False
                print(f"Failed to upload {sheet_name}")
        
        if success:
            print("\nData processing and upload completed successfully!")
        else:
            raise DataProcessingError("Failed to upload one or more datasets")
            
    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()