import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import googleapiclient.discovery
from googleapiclient.discovery import build
import os
import traceback
from .gdrive_config import GOOGLE_API_CONFIG, authenticate_with_google
from .config import authenticate_with_google
import pandas as pd
import pkg_resources

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
        worksheet = spreadsheet.add_worksheet(title=title, rows=str(len(df_cleaned) + 1), cols=str(len(df_cleaned.columns)))

    values_to_upload = [df_cleaned.columns.tolist()] + df_cleaned.values.tolist()
    worksheet.update(values_to_upload)  # Update worksheet with new values

    return spreadsheet

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
    print(f"Formatted {len(sheets)} sheets with response: {response}")

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


def get_column_index(spreadsheet_id, sheet_index, column_name, credentials):
    """
    Retrieve the column index for a given column name in a specific sheet.

    Args:
    spreadsheet_id (str): The ID of the spreadsheet.
    sheet_index (int): The index of the sheet within the spreadsheet.
    column_name (str): The name of the column to find.
    credentials: Google API credentials.

    Returns:
    int: The 0-based index of the column, or an error if not found.
    """
    service = build('sheets', 'v4', credentials=credentials)
    
    # Fetch all sheets to get the title for the specified index
    try:
        response = service.spreadsheets().get(spreadsheetId=spreadsheet_id, fields='sheets(properties)').execute()
        sheets = response.get('sheets', [])
        sheet_title = None
        for sheet in sheets:
            if sheet['properties']['index'] == sheet_index:
                sheet_title = sheet['properties']['title']
                break
        
        if not sheet_title:
            return ValueError(f"No sheet found at index {sheet_index}")


        # Use the sheet title to specify the range in A1 notation
        range_name = f"'{sheet_title}'!1:1"
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=range_name).execute()
        headers = result.get('values', [[]])[0]
        
        if column_name in headers:
            return headers.index(column_name)
        else:
            return ValueError(f"Column name '{column_name}' not found in the sheet.")
    except Exception as e:
        print(f"Failed to retrieve or parse headers: {str(e)}")
        return None

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

def set_dropdown_list_by_id(spreadsheet_id, sheet_index, column_index, values, credentials):
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
                "startRowIndex": 4,  # Starting from row 6 in the sheet
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
    
def load_descriptions():
    """
    Load descriptions from a CSV file located in the 'data/' directory of the repository.
    """
    # The resource_filename function takes the package name and relative path to the file
    resource_path = pkg_resources.resource_filename('your_package_name', 'data/metadata_descriptions.csv')
    
    # Load the descriptions CSV
    descriptions = pd.read_csv(resource_path, index_col=0)
    return descriptions

def add_metadata_descriptions(metadata_dfs):
    """
    Combine metadata definitions with descriptions to prepare for upload.

    Args:
        metadata_dfs: Dictionary containing metadata DataFrames for each tab.
    
    Returns:
        A dictionary with the combined data ready for upload.
    """
    # Load descriptions
    descriptions = pd.read_csv('data/metadata_descriptions.csv', index_col=0)
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
        # Reset index to clean up the DataFrame
        combined_df.reset_index(drop=True, inplace=True)
        # Store the updated DataFrame in the dictionary
        updated_dfs[tab_name] = combined_df
    return updated_dfs