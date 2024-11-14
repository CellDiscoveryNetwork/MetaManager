import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import googleapiclient.discovery
from googleapiclient.discovery import build
import traceback
from .gdrive_config import GOOGLE_API_CONFIG, authenticate_with_google
from .config import authenticate_with_google
import pandas as pd
import pkg_resources
import time
import random

def backoff(attempt, max_delay=60):
    """ Calculate sleep time in seconds for exponential backoff. """
    delay = min(max_delay, (2 ** attempt) + (random.randint(0, 1000) / 1000))
    time.sleep(delay)
    return delay

def convert_numeric_to_string(data):
    if isinstance(data, dict):
        return {k: convert_numeric_to_string(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_numeric_to_string(item) for item in data]
    elif isinstance(data, (int, float)):  # Targeting only numeric types
        return str(data)
    else:
        return data

def initialize_google_sheets():
    """
    Initializes and returns a Google Sheets client authorized with OAuth2 credentials.

    This function authenticates using specified scopes and credentials, allowing further operations with Google Sheets.

    Returns:
        gspread.client.Client: An authorized Google Sheets client instance.

    Example:
        >>> gc = initialize_google_sheets()
    """
    creds = authenticate_with_google(GOOGLE_API_CONFIG['scopes'], GOOGLE_API_CONFIG['credentials_file'])
    gc = gspread.authorize(creds)
    return gc

def upload_to_sheet(df, gc, spreadsheet_id, title):
    """
    Uploads data from a DataFrame to a specific Google Sheet, cleaning the data beforehand.

    Replaces NaN, inf, and -inf values in the DataFrame with None and updates or creates a worksheet
    with the specified title in the given spreadsheet.

    Args:
        df (pandas.DataFrame): The DataFrame to upload.
        gc (gspread.client.Client): An authorized Google Sheets client instance.
        spreadsheet_id (str): The ID of the Google Spreadsheet to update.
        title (str): The title of the worksheet to update or create.

    Returns:
        gspread.models.Spreadsheet: The updated Google Spreadsheet object.

    Example:
        >>> gc = initialize_google_sheets()
        >>> df = pandas.DataFrame({'A': [1, 2], 'B': [3, 4]})
        >>> spreadsheet = upload_to_sheet(df, gc, 'your_spreadsheet_id_here', 'Sheet1')
    """
    df_cleaned = df.replace([np.nan, np.inf, -np.inf], None)  # Clean the DataFrame
    spreadsheet = gc.open_by_key(spreadsheet_id)  # Open the spreadsheet

    try:
        worksheet = spreadsheet.worksheet(title)
        worksheet.clear()  # Clear existing content before update
        worksheet.resize(rows=len(df_cleaned) + 1, cols=len(df_cleaned.columns))  # Resize if needed
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=title, rows=1000, cols=str(len(df_cleaned.columns)))

    values_to_upload = [df_cleaned.columns.tolist()] + df_cleaned.values.tolist()
    worksheet.update(values_to_upload)  # Update worksheet with new values

    return spreadsheet

def add_empty_rows(spreadsheet_id, gc, num_rows):
    """
    Add rows to the Google Sheet specified by the spreadsheet_id.

    Args:
        spreadsheet_id (str): The ID of the Google Sheet.
        gc (gspread.client.Client): An authorized Google Sheets client instance.
        num_rows (int): Number of rows to add.
    """
    spreadsheet = gc.open_by_key(spreadsheet_id)
    requests = [{
        "appendCells": {
            "sheetId": 0,  # Assuming you're working with the first sheet
            "rows": [{"values": [{} for _ in range(num_rows)]}],
            "fields": "userEnteredValue"
        }
    }]
    
    spreadsheet.batch_update({'requests': requests})

def delete_sheet(spreadsheet_id, sheet_title, gc):
    """
    Deletes a specific worksheet from a Google Spreadsheet based on the title.

    Args:
        spreadsheet_id (str): The ID of the Google Spreadsheet from which to delete the worksheet.
        sheet_title (str): The title of the worksheet to delete.
        gc (gspread.client.Client): An authorized Google Sheets client instance.

    Returns:
        dict: A response from the Google Sheets API after deleting the worksheet.

    Example:
        >>> gc = initialize_google_sheets()
        >>> response = delete_sheet('your_spreadsheet_id_here', 'Sheet1', gc)
    """
    spreadsheet = gc.open_by_key(spreadsheet_id)
    sheet = spreadsheet.worksheet(sheet_title)
    sheet_id = sheet._properties['sheetId']
    
    requests = [{
        "deleteSheet": {
            "sheetId": sheet_id
        }
    }]
    
    body = {"requests": requests}
    response = spreadsheet.batch_update(body)
    return response

def list_google_sheets(creds, folder_id):
    """
    Lists all Google Sheets within a specified Google Drive folder.

    This function retrieves the names and IDs of all spreadsheets in a given folder, printing them and returning a list.

    Args:
        creds (google.auth.credentials.Credentials): The OAuth2 credentials for accessing Google Drive.
        folder_id (str): The ID of the folder in Google Drive.

    Returns:
        list: A list of dictionaries where each dictionary contains 'id' and 'name' of a spreadsheet.

    Example:
        >>> creds = authenticate_with_google(scopes)
        >>> sheets = list_google_sheets(creds, 'your_folder_id_here')
        >>> for sheet in sheets:
        ...     print(sheet['name'], sheet['id'])
    """
    service = build('drive', 'v3', credentials=creds)
    query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet'"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])
    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            print(f"{item['name']} ({item['id']})")
    return items

def concatenate_worksheets(client, sheet_id, worksheet_name, df):
    """
    Concatenates data from a specific Google Sheet worksheet into an existing pandas DataFrame.

    This function retrieves all values from the specified worksheet, appends them to the provided DataFrame,
    and returns the concatenated DataFrame. Errors during processing are caught and printed.

    Args:
        client (gspread.client.Client): An authorized Google Sheets client instance.
        sheet_id (str): The ID of the Google Spreadsheet.
        worksheet_name (str): The name of the worksheet to fetch data from.
        df (pandas.DataFrame): The existing DataFrame to which data will be appended.

    Returns:
        pandas.DataFrame: The DataFrame containing the original and appended data.

    Raises:
        Exception: Outputs an error message and the traceback if an operation fails.

    Example:
        >>> gc = initialize_google_sheets()
        >>> existing_df = pandas.DataFrame({'A': [1, 2], 'B': [3, 4]})
        >>> updated_df = concatenate_worksheets(gc, 'your_sheet_id_here', 'Sheet1', existing_df)
    """
    try:
        sheet = client.open_by_key(sheet_id)
        worksheet = sheet.worksheet(worksheet_name)
        data = worksheet.get_all_values()
        headers = data.pop(0)
        df_temp = pd.DataFrame(data, columns=headers)
        return pd.concat([df, df_temp], ignore_index=True)
    except Exception as e:
        print(f"Error processing worksheet '{worksheet_name}' in sheet {sheet_id}: {str(e)}")
        traceback.print_exc()
        return df

def format_all_sheets(spreadsheet_id, credentials):
    service = build('sheets', 'v4', credentials=credentials)
    
    # First, retrieve all sheets in the spreadsheet
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = spreadsheet.get('sheets', [])
    
    requests = []
    
    for sheet in sheets:
        sheet_id = sheet['properties']['sheetId']  # Access the sheetId from the sheet properties

        # Set column widths and row heights
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": 52
                },
                "properties": {"pixelSize": 120},
                "fields": "pixelSize"
            }
        })

        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 0,
                    "endIndex": 1  # Adjust for the first two rows
                },
                "properties": {"pixelSize": 40},
                "fields": "pixelSize"
            }
        })

        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 1,
                    "endIndex": 2  # Adjust for the first two rows
                },
                "properties": {"pixelSize": 80},
                "fields": "pixelSize"
            }
        })

        # Format text in the first row and column, and second row
        requests.extend([
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": 52
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "CENTER",
                            "textFormat": {
                                "fontSize": 12,
                                "bold": True
                            },
                            "backgroundColor": {
                                "red": 0.8, "green": 0.8, "blue": 0.8  # Light gray
                            }
                        }
                    },
                    "fields": "userEnteredFormat(textFormat,backgroundColor,horizontalAlignment)"
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": 3,
                        "startColumnIndex": 0,
                        "endColumnIndex": 52
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "CENTER",
                            "textFormat": {
                                "fontSize": 10,
                                "italic": True
                            },
                            "backgroundColor": {
                                "red": 0.9, "green": 0.9, "blue": 0.9  # Very light gray
                            }
                        }
                    },
                    "fields": "userEnteredFormat(textFormat,backgroundColor,horizontalAlignment)"
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 4,
                        "startColumnIndex": 0,
                        "endColumnIndex": 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "CENTER",
                            "textFormat": {
                                "fontSize": 10,
                                "italic": True,
                                "bold": True,
                            },
                            "backgroundColor": {
                                "red": 0.9, "green": 0.9, "blue": 0.9  # Very light gray
                            },                           
                        }
                    },
                    "fields": "userEnteredFormat(textFormat,backgroundColor,horizontalAlignment)"
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": 4,
                        "startColumnIndex": 0,
                        "endColumnIndex": 52
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "LEFT",
                            "textFormat": {
                                "fontSize": 10,
                                "italic": True,
                            }
                        }
                    },
                    "fields": "userEnteredFormat(textFormat,backgroundColor,horizontalAlignment)"
                }
            },
        ])
        # Insert a new row at position 5
        requests.append({
            "insertDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 4,  # Rows are zero-indexed; 4 corresponds to the 5th row
                    "endIndex": 5
                },
                "inheritFromBefore": False
            }
        })
        
        # Merge cells in the new row
        requests.append({
            "mergeCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 4,
                    "endRowIndex": 5,
                    "startColumnIndex": 0,
                    "endColumnIndex": 52  # Adjust based on the number of columns you want to span
                },
                "mergeType": "MERGE_ALL"
            }
        })
        
        # Here we add a big merged cell in row 5 to show where people should fill out
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 4,
                    "endRowIndex": 5,
                    "startColumnIndex": 0,
                    "endColumnIndex": 52
                },
               "cell": {
                    "userEnteredValue": {
                        "stringValue": "FILL OUT INFORMATION BELOW THIS ROW"
                    },
                    "userEnteredFormat": {
                        "backgroundColor": {
                            "red": 0.8, "green": 0.8, "blue": 0.8
                        },
                        "horizontalAlignment": "CENTER",
                        "textFormat": {
                            "fontSize": 20,
                            "bold": True,
                            "italic": True
                        }
                    }
                },
                "fields": "userEnteredValue,userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"

            }
        })

    # Execute the batch update
    body = {"requests": requests}
    response = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    print(f"Formatting {len(sheets)} sheets")

def get_sheet_title_from_id(spreadsheet_id, sheet_id, credentials):
    """
    Retrieve the title of a sheet given its ID in a specific spreadsheet.

    Args:
    sheet_service: Authenticated googleapiclient.discovery service object.
    spreadsheet_id (str): The ID of the spreadsheet.
    sheet_id (int): The ID of the sheet.

    Returns:
    str: The title of the sheet.
    """
    service = build('sheets', 'v4', credentials=credentials)
    spreadsheet_info = service.spreadsheets().get(spreadsheetId=spreadsheet_id, fields='sheets(properties)').execute()
    for sheet in spreadsheet_info['sheets']:
        if sheet['properties']['sheetId'] == sheet_id:
            return sheet['properties']['title']
    raise ValueError("Sheet ID not found in the spreadsheet.")

def fetch_sheets_with_indices(spreadsheet_id, credentials):
    """
    Fetches the indices and titles of all sheets in a specified Google Spreadsheet.

    This function interacts with the Google Sheets API to retrieve metadata about each sheet within a spreadsheet,
    specifically extracting their indices and titles. The result is a dictionary mapping sheet indices to their titles.

    Args:
        spreadsheet_id (str): The ID of the Google Spreadsheet.
        credentials: Google API credentials used for accessing the sheet.

    Returns:
        dict: A dictionary where the keys are sheet indices (int) and the values are sheet titles (str).

    Raises:
        googleapiclient.errors.HttpError: If the Google Sheets API request fails.

    Example:
        >>> credentials = authenticate_with_google(scopes)
        >>> sheets_info = fetch_sheets_with_indices('your_spreadsheet_id', credentials)
        >>> print(sheets_info)
        {1: 'dataset metadata', 2: 'donor metadata', 3: 'sample metadata', 4: 'celltype metadata'}
    """
    service = build('sheets', 'v4', credentials=credentials)
    response = service.spreadsheets().get(spreadsheetId=spreadsheet_id, fields='sheets(properties)').execute()
    sheets = response.get('sheets', [])
    return {sheet['properties']['index']: sheet['properties']['title'] for sheet in sheets}

def cache_column_indices(spreadsheet_id, credentials):
    service = build('sheets', 'v4', credentials=credentials)
    # Fetch sheet titles and ids
    sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id, fields='sheets(properties)').execute()
    sheets = sheet_metadata.get('sheets', [])
    
    column_cache = {}
    for sheet in sheets:
        title = sheet['properties']['title']
        index = sheet['properties']['index']
        range_name = f"'{title}'!1:1"
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        headers = result.get('values', [[]])[0]
        
        # Map each column name to its index in this sheet
        column_cache[index] = {header.lower(): idx for idx, header in enumerate(headers)}
    
    return column_cache

def get_column_index(sheet_index, column_name, column_cache):
    """
    Retrieve the column index using the pre-fetched cache.
    """
    column_name = column_name.lower()
    if sheet_index in column_cache and column_name in column_cache[sheet_index]:
        return column_cache[sheet_index][column_name]
    else:
        return -1  # Column name not found


def cache_sheet_columns(spreadsheet_id, credentials):
    """
    Cache column headers for each sheet to minimize API calls.
    Returns a dictionary with sheet titles as keys and another dictionary as values, mapping column names to indices.
    """
    service = build('sheets', 'v4', credentials=credentials)
    sheets_headers = {}
    try:
        # Fetch metadata for all sheets to get their titles
        response = service.spreadsheets().get(spreadsheetId=spreadsheet_id, fields='sheets(properties)').execute()
        sheets = response.get('sheets', [])
        for sheet in sheets:
            sheet_title = sheet['properties']['title']
            range_name = f"'{sheet_title}'!1:1"  # Adjust if headers are not in the first row
            # Fetch headers for each sheet
            result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
            headers = result.get('values', [[]])[0]
            # Map column names to their indices
            sheets_headers[sheet_title] = {header: idx for idx, header in enumerate(headers)}
    except Exception as e:
        print(f"Failed to cache headers: {str(e)}")
    return sheets_headers


# def fetch_sheets_list(spreadsheet_id, credentials):
#     service = build('sheets', 'v4', credentials=credentials)
#     sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
#     sheets = sheet_metadata.get('sheets', '')
#     return {sheet['properties']['title']: sheet['properties']['sheetId'] for sheet in sheets}

def set_dropdown_list(spreadsheet_id, sheet_title, column_index, values, gc, credentials):
    """
    Set a dropdown list for a specified column in a Google Sheet using gspread.
    
    Args:
    spreadsheet_id (str): The ID of the Google Spreadsheet.
    sheet_title (str): The title of the sheet within the spreadsheet.
    column_index (int): The column index where the dropdown will be applied.
    values (list): List of allowed values for the dropdown.
    gc: Authenticated gspread client.
    
    Returns:
    response: API response from the batch_update method.
    """
    service = build('sheets', 'v4', credentials=credentials)
    spreadsheet = gc.open_by_key(spreadsheet_id)
    sheet = spreadsheet.worksheet(sheet_title)
    sheet_id = sheet._properties['sheetId']
    
    # Prepare the setDataValidation request
    requests = [{
        "setDataValidation": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 5,  # Assuming the first row is headers
                "endRowIndex": 1000,  # Adjust this as needed to match your data size
                "startColumnIndex": column_index,
                "endColumnIndex": column_index + 1
            },
            "rule": {
                "condition": {
                    "type": "ONE_OF_LIST",
                    "values": [{"userEnteredValue": str(val)} for val in values]
                },
                "inputMessage": "Select from the list",
                "strict": True,
                "showCustomUi": True
            }
        }
    }]
    
    body = {"requests": requests}
    response = spreadsheet.batch_update(body)
    return response

def set_dropdown_list_by_id_old(spreadsheet_id, sheet_index, column_index, num_header_rows, values, credentials):
    """
    Apply data validation dropdown list to a specified column in a Google Sheet.

    Args:
    spreadsheet_id (str): The ID of the Google Spreadsheet.
    sheet_index (int): The index of the sheet within the spreadsheet.
    column_index (int): The column index where the dropdown should be applied (0-based index).
    values (list): A list of values that will appear in the dropdown.
    credentials: Google API credentials.

    Returns:
    response from the API call
    """
    service = build('sheets', 'v4', credentials=credentials)
    
    # Fetch the actual sheet ID from the index
    sheet_id = fetch_sheet_id_from_index(spreadsheet_id, sheet_index, credentials)
    if not sheet_id:
        raise ValueError("Failed to find a sheet at the given index.")

    sheet_properties = service.spreadsheets().get(spreadsheetId=spreadsheet_id, fields='sheets(properties)').execute()
    for sheet in sheet_properties['sheets']:
        if sheet['properties']['sheetId'] == sheet_id:
            max_rows = sheet['properties'].get('gridProperties', {}).get('rowCount', 1000)

    # Prepare the request body for setting data validation
    requests = [{
        "setDataValidation": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": num_header_rows-1,  # Starting from row 6 in the sheet
                "endRowIndex": max_rows, 
                "startColumnIndex": column_index,
                "endColumnIndex": column_index + 1
            },
            "rule": {
                "condition": {
                    "type": "ONE_OF_LIST",
                    "values": [{"userEnteredValue": val} for val in values]
                },
                "inputMessage": "Select from the list",
                "strict": True,
                "showCustomUi": True
            }
        }
    }]

    # Send the batch update request to the Sheets API
    body = {"requests": requests}
    response = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    return response

def set_dropdown_list_by_id(spreadsheet_id, sheet_index, column_index, num_header_rows, values, credentials, sheet_properties_cache):
    """
    Apply data validation dropdown list to a specified column in a Google Sheet using cached properties.
    """
    if sheet_index not in sheet_properties_cache:
        raise ValueError(f"Sheet ID not found for index {sheet_index}")
    sheet_id, max_rows = sheet_properties_cache[sheet_index]
    # Initialize Google Sheets API service
    service = build('sheets', 'v4', credentials=credentials)
    # Check if there are meaningful values to add in the dropdown
    if not values or (len(values) == 1 and values[0] == ""):
        # print(f"No valid dropdown values to set for column index {column_index} in sheet index {sheet_index}.")
        return None  # Exit if no valid values
    # Prepare the request body for setting data validation
    requests = [{
        "setDataValidation": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": num_header_rows + 1,  # Adjust for header name rows and "fill out below" banner
                "endRowIndex": max_rows, 
                "startColumnIndex": column_index,
                "endColumnIndex": column_index + 1
            },
            "rule": {
                "condition": {
                    "type": "ONE_OF_LIST",
                    "values": [{"userEnteredValue": val} for val in values]
                },
                "inputMessage": "Select from the list",
                "strict": True,
                "showCustomUi": True
            }
        }
    }]
    # Send the batch update request to the Sheets API
    body = {"requests": requests}
    try:
        response = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
        # print(f"Dropdowns set successfully for sheet ID {sheet_id} at column index {column_index}")
    except Exception as e:
        print(f"Failed to set dropdowns: {str(e)}")
    return response


def fetch_sheet_id_from_index(spreadsheet_id, sheet_index, credentials):
    """
    Fetch the sheet ID using the sheet index.

    Args:
    spreadsheet_id (str): The ID of the Google Spreadsheet.
    sheet_index (int): The index of the sheet.
    credentials: Google API credentials.

    Returns:
    The actual sheet ID corresponding to the index or None if not found.
    """
    service = build('sheets', 'v4', credentials=credentials)
    response = service.spreadsheets().get(spreadsheetId=spreadsheet_id, fields='sheets(properties)').execute()
    sheets = response.get('sheets', [])
    
    for sheet in sheets:
        if sheet['properties']['index'] == sheet_index:
            return sheet['properties']['sheetId']
    
    return None

def cache_sheet_properties(spreadsheet_id, credentials):
    """
    Cache sheet properties including sheet IDs, their indexes, and row counts to minimize API calls.
    Returns a dictionary with sheet indexes as keys and a tuple of (sheet ID, row count) as values.
    """
    service = build('sheets', 'v4', credentials=credentials)
    sheet_properties_cache = {}
    try:
        # Fetch metadata for all sheets to get their indexes and IDs
        response = service.spreadsheets().get(spreadsheetId=spreadsheet_id, fields='sheets(properties)').execute()
        sheets = response.get('sheets', [])
        for sheet in sheets:
            sheet_index = sheet['properties']['index']
            sheet_id = sheet['properties']['sheetId']
            row_count = sheet['properties'].get('gridProperties', {}).get('rowCount', 1000)
            sheet_properties_cache[sheet_index] = (sheet_id, row_count)
    except Exception as e:
        print(f"Failed to cache sheet properties: {str(e)}")
    return sheet_properties_cache


def concatenate_metadata(descriptions, metadata_tb):
    shared_cols = descriptions.columns.intersection(metadata_tb.columns)
    descriptions_tb = pd.concat([descriptions[shared_cols], metadata_tb])
    descriptions_tb.reset_index(inplace=True, drop=True)
    return descriptions_tb

def move_sheet_in_drive(file_id, folder_id, credentials):
    """
    Move a Google Sheet to a specified folder in Google Drive.

    Args:
        file_id (str): The ID of the file to move.
        folder_id (str): The ID of the destination folder.
        credentials: Google API credentials.
    """
    drive_service = googleapiclient.discovery.build('drive', 'v3', credentials=credentials)
    file = drive_service.files().get(fileId=file_id, fields='parents').execute()
    previous_parents = ",".join(file.get('parents'))
    drive_service.files().update(fileId=file_id,
                                 addParents=folder_id,
                                 removeParents=previous_parents,
                                 fields='id, parents').execute()

def load_descriptions(csv_path=None):
    """
    Load descriptions from a CSV file. If no path is provided, load from a default location.
    Args:
        csv_path (str, optional): Path to the CSV file containing descriptions.
    Returns:
        DataFrame: Loaded descriptions from the CSV file.
    """
    if csv_path is None:
        # The resource_filename function takes the package name and relative path to the file
        csv_path = pkg_resources.resource_filename('hca_metadata_manager', 'data/metadata_descriptions.csv')
        print("Loading descriptions from:", csv_path)
    # Load the descriptions CSV
    descriptions = pd.read_csv(csv_path, index_col=0)
    return descriptions

def add_metadata_descriptions(metadata_dfs, descriptions_csv=None):
    """
    Combine metadata definitions with descriptions to prepare for upload.

    Args:
        metadata_dfs: Dictionary containing metadata DataFrames for each tab.
    
    Returns:
        A dictionary with the combined data ready for upload.
    """
    # Load descriptions
    descriptions = load_descriptions(csv_path=descriptions_csv)
    updated_dfs = {}  # Dictionary to store updated DataFrames
    
    # Process each metadata DataFrame
    for tab_name, metadata_df in metadata_dfs.items():
        # Find shared columns between descriptions and metadata DataFrame
        shared_cols = descriptions.columns.intersection(metadata_df.columns)
        
        # Combine descriptions with metadata DataFrame
        combined_df = pd.concat([
            descriptions[shared_cols],  # Description rows
            metadata_df                 # Original metadata DataFrame
        ])
        
        # Reorder columns to match the original metadata DataFrame
        combined_df = combined_df[metadata_df.columns]
        
        # Reset index to clean up the DataFrame
        combined_df.reset_index(drop=True, inplace=True)
        
        # Store the updated DataFrame in the dictionary
        updated_dfs[tab_name] = combined_df
    
    return updated_dfs


# def load_sheets_metadata(credentials, googlesheets):
#     spreadsheet_ids = []
#     for goo in googlesheets:
#         spreadsheet_ids.append(goo['id'])

#     service = build('sheets', 'v4', credentials=credentials)
#     all_dfs = {}  # Dictionary to store dataframes for each metadata type

#     for spreadsheet_id in spreadsheet_ids:
#         print(f'Loading data from Spreadsheet ID: {spreadsheet_id}')
#         spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
#         sheets = spreadsheet.get('sheets', [])

#         for sheet in sheets:
#             title = sheet['properties']['title']
#             if 'metadata' in title.lower():
#                 # First retrieve only the first row to count non-empty columns
#                 first_row_range = f'{title}!1:1'
#                 first_row_result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=first_row_range).execute()
#                 first_row_values = first_row_result.get('values', [])
                
#                 if first_row_values:
#                     # Count non-empty columns in the first row
#                     non_empty_columns = len(first_row_values[0])
#                     last_column_letter = column_to_gsheet_letter(non_empty_columns)
#                     range_name = f'{title}!A:{last_column_letter}'
                    
#                     # Retrieve the full range with the correct number of columns
#                     result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
#                     rows = result.get('values', [])
#                     if rows:
#                         headers = rows.pop(0)
#                         adjusted_rows = [row + [None] * (len(headers) - len(row)) for row in rows[4:]]
#                         df_temp = pd.DataFrame(adjusted_rows, columns=headers)
#                         df_temp['worksheet'] = spreadsheet['properties']['title'].split(" ")[0]
                        
#                         metadata_type = title.lower().split('metadata')[0].strip()
#                         if metadata_type in all_dfs:
#                             all_dfs[metadata_type] = pd.concat([all_dfs[metadata_type], df_temp], ignore_index=True)
#                         else:
#                             all_dfs[metadata_type] = df_temp

#         # Throttle the speed of API calls to avoid exceeding limit
#         time.sleep(15)

#     return all_dfs

def load_sheets_metadata(credentials, googlesheets):
    service = build('sheets', 'v4', credentials=credentials)
    all_dfs = {}  # Dictionary to store dataframes for each metadata type
    for sheet_info in googlesheets:
        spreadsheet_id = sheet_info['id']
        attempt = 0  # Initialize attempt counter
        while attempt < 8:  # Allow up to 5 attempts
            try:
                print(f'Loading data from Spreadsheet ID: {spreadsheet_id}')
                spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
                sheets = spreadsheet.get('sheets', [])
                for sheet in sheets:
                    title = sheet['properties']['title']
                    if 'metadata' in title.lower():
                        # First retrieve only the first row to count non-empty columns
                        first_row_range = f'{title}!1:1'
                        first_row_result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=first_row_range).execute()
                        first_row_values = first_row_result.get('values', [])
                        if first_row_values:
                            non_empty_columns = len(first_row_values[0])
                            last_column_letter = column_to_gsheet_letter(non_empty_columns)
                            range_name = f'{title}!A:{last_column_letter}'     
                            result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
                            rows = result.get('values', [])
                            if rows:
                                headers = rows.pop(0)
                                adjusted_rows = [row + [None] * (len(headers) - len(row)) for row in rows[4:]]
                                df_temp = pd.DataFrame(adjusted_rows, columns=headers)
                                df_temp['worksheet'] = spreadsheet['properties']['title'].split(" ")[0]
                                metadata_type = title.lower().split('metadata')[0].strip()
                                if metadata_type in all_dfs:
                                    all_dfs[metadata_type] = pd.concat([all_dfs[metadata_type], df_temp], ignore_index=True)
                                else:
                                    all_dfs[metadata_type] = df_temp
                break  # Exit loop after successful load
            except Exception as e:
                if 'Quota exceeded' in str(e) and attempt < 7:  # Check if the error is due to quota exceeded
                    sleep_time = backoff(attempt)
                    print(f"Quota exceeded, retrying after {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                    attempt += 1
                else:
                    print(f"Failed to load sheet {spreadsheet_id} on attempt {attempt + 1}: {str(e)}")
                    break  # Break after max attempts or other types of errors
    return all_dfs

def column_to_gsheet_letter(column_number):
    """
    Convert a column number (1-indexed) to a column letter, as used in Excel or Google Sheets.
    """
    letter = ''
    while column_number > 0:
        column_number, remainder = divmod(column_number - 1, 26)
        letter = chr(65 + remainder) + letter
    return letter

def create_set_dropdown_request(sheet_id, column_index, values, num_header_rows, max_rows):
    """Generate a request to set dropdowns for a specific column."""
    # Check if there are meaningful values to add in the dropdown
    if not values or (len(values) == 1 and values[0] == ""):
        # print(f"No valid dropdown values to set for column index {column_index} in sheet index {sheet_index}.")
        return None  # Exit if no valid values
    return {
        "setDataValidation": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": num_header_rows,  # Skip header rows
                "endRowIndex": max_rows, 
                "startColumnIndex": column_index,
                "endColumnIndex": column_index + 1
            },
            "rule": {
                "condition": {
                    "type": "ONE_OF_LIST",
                    "values": [{"userEnteredValue": str(val)} for val in values]
                },
                "inputMessage": "Select from the list",
                "strict": True,
                "showCustomUi": True
            }
        }
    }
