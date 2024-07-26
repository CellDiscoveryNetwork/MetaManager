import pytest
import unittest
import pandas as pd
from unittest.mock import patch, Mock, MagicMock
from hca_patient_meta.utils import (
    initialize_google_sheets, upload_to_sheet, delete_sheet,
    fetch_sheets_with_indices
)

# # Mock spreadsheet and worksheet objects
# mock_spreadsheet = Mock()
# mock_worksheet = Mock()

# # Sample data for testing upload_to_sheet
# sample_data = {
#     "sample_id": ["id1", "id2", "id3"],
#     "library_id": ["lib1", "lib2", "lib3"],
#     "disease": ["disease1", "disease2", "disease3"]
# }
# df = pd.DataFrame(sample_data)

# # Test for initialize_google_sheets
# @patch('hca_patient_meta.utils.authenticate_with_google')
# @patch('gspread.authorize')
# def test_initialize_google_sheets():
#     with patch('utils.authenticate_with_google') as mock_auth:
#         with patch('gspread.authorize') as mock_authorize:
#             mock_auth.return_value = 'mock_credentials'
#             mock_authorize.return_value = 'mock_client'
#             result = initialize_google_sheets()
#             assert result == 'mock_client'

# # Test for upload_to_sheet
# @patch('gspread.client.Client.open_by_key')
# @patch('gspread.Spreadsheet.worksheet')
# def test_upload_to_sheet():
#     with patch('gspread.client.Client.open_by_key', return_value=mock_spreadsheet) as mock_open:
#         with patch('gspread.models.Spreadsheet.worksheet', return_value=mock_worksheet) as mock_worksheet_method:
#             with patch('gspread.models.Worksheet.update') as mock_update:
#                 with patch('utils.authenticate_with_google') as mock_auth:
#                     with patch('gspread.authorize') as mock_authorize:
#                         # Setup the authentication and spreadsheet mocks
#                         mock_auth.return_value = 'mock_credentials'
#                         mock_authorize.return_value = Mock()
                        
#                         # Assume the worksheet exists and no exception is thrown
#                         gc = mock_authorize.return_value
#                         utils.upload_to_sheet(df, gc, 'fake_spreadsheet_id', 'sample metadata')
                        
#                         # Assertions to check if sheet is accessed and data is uploaded correctly
#                         mock_open.assert_called_once_with('fake_spreadsheet_id')
#                         mock_worksheet_method.assert_called_once_with('sample metadata')
#                         mock_update.assert_called()  # Check update was called, can be more specific

# @patch('gspread.client.Client.open_by_key')
# @patch('gspread.Spreadsheet.worksheet')
# @patch('gspread.Spreadsheet.batch_update')
# def test_delete_sheet():
#     with patch('gspread.client.Client.open_by_key', return_value=mock_spreadsheet) as mock_open:
#         with patch('gspread.models.Spreadsheet.worksheet', return_value=mock_worksheet) as mock_worksheet_method:
#             with patch('gspread.models.Worksheet.batch_update') as mock_batch_update:
#                 with patch('utils.authenticate_with_google') as mock_auth:
#                     with patch('gspread.authorize') as mock_authorize:
#                         # Setup the authentication and spreadsheet mocks
#                         mock_auth.return_value = 'mock_credentials'
#                         mock_authorize.return_value = Mock()

#                         # Setting up properties for the worksheet to be deleted
#                         mock_worksheet._properties = {'sheetId': 12345}

#                         # Call the function to test
#                         gc = mock_authorize.return_value
#                         utils.delete_sheet('fake_spreadsheet_id', 'sheet1', gc)

#                         # Assertions to check if correct requests are made
#                         mock_open.assert_called_once_with('fake_spreadsheet_id')
#                         mock_worksheet_method.assert_called_once_with('sheet1')
#                         mock_batch_update.assert_called_once_with({
#                             "requests": [{"deleteSheet": {"sheetId": 12345}}]
#                         })

# class TestListGoogleSheets(unittest.TestCase):
#     @patch('utils.build')
#     def test_list_google_sheets(self, mock_build):
#         # Setup the mock responses
#         mock_service = MagicMock()
#         mock_files = MagicMock()
#         mock_list = MagicMock(return_value=mock_files)
#         mock_execute = MagicMock(return_value={'files': [{'id': '123', 'name': 'Test Sheet'}]})

#         mock_build.return_value = mock_service
#         mock_service.files.return_value = mock_files
#         mock_files.list = mock_list
#         mock_list.return_value.execute = mock_execute

#         # Call the function
#         creds = MagicMock()
#         folder_id = 'test_folder_id'
#         result = list_google_sheets(creds, folder_id)

#         # Assertions to ensure correct call and response handling
#         mock_build.assert_called_once_with('drive', 'v3', credentials=creds)
#         mock_list.assert_called_once_with(q="'test_folder_id' in parents and mimeType='application/vnd.google-apps.spreadsheet'", fields="files(id, name)")
#         mock_execute.assert_called_once()
#         self.assertEqual(result, [{'id': '123', 'name': 'Test Sheet'}])

# class TestConcatenateWorksheets(unittest.TestCase):
#     @patch('utils.pd.concat')
#     @patch('gspread.client.Client')
#     def test_concatenate_worksheets(self, mock_client, mock_concat):
#         # Set up the mocks
#         mock_sheet = Mock()
#         mock_worksheet = Mock()
#         mock_get_all_values = Mock(return_value=[['sample_id', 'library_id', 'disease'], ['1', '2', '3']])
        
#         mock_client.open_by_key.return_value = mock_sheet
#         mock_sheet.worksheet.return_value = mock_worksheet
#         mock_worksheet.get_all_values = mock_get_all_values
#         mock_concat.return_value = pd.DataFrame({'sample_id': [1], 'library_id': [2], 'disease': [3]})
        
#         client = mock_client()
#         sheet_id = 'sheet_id'
#         worksheet_name = 'worksheet_name'
#         df = pd.DataFrame({'sample_id': [], 'library_id': [], 'disease': []})

#         # Execute the function
#         result = concatenate_worksheets(client, sheet_id, worksheet_name, df)

#         # Assertions
#         mock_client.open_by_key.assert_called_with(sheet_id)
#         mock_sheet.worksheet.assert_called_with(worksheet_name)
#         self.assertTrue(isinstance(result, pd.DataFrame))

# @patch('gspread.Spreadsheet.batch_update')
# class TestFormatSheet(unittest.TestCase):
#     @patch('gspread.client.Client.batch_update')
#     def test_format_sheet(self, mock_batch_update):
#         # Set up mock
#         gc = Mock()
#         spreadsheet_id = 'spreadsheet_id'
#         sheet_id = 'sheet_id'
#         requests = [{'updateCells': {'range': 'A1:Z100', 'fields': 'userEnteredFormat'}}]  # Sample request
#         response = {'replies': [{'updateCells': 'done'}], 'spreadsheetId': spreadsheet_id}
#         mock_batch_update.return_value = response

#         # Execute
#         result = format_sheet(spreadsheet_id, sheet_id, gc)

#         # Assertions
#         gc.batch_update.assert_called_once_with(spreadsheet_id, {'requests': []})
#         self.assertEqual(result, response)
import unittest
from unittest.mock import patch, Mock
import pandas as pd
from hca_patient_meta.utils import (
    initialize_google_sheets, upload_to_sheet, delete_sheet,
    fetch_sheets_with_indices
)

class TestUtils(unittest.TestCase):

    @patch('hca_patient_meta.utils.authenticate_with_google')
    @patch('gspread.authorize')
    def test_initialize_google_sheets(self, mock_authorize, mock_auth):
        mock_auth.return_value = 'mock_credentials'
        mock_authorize.return_value = 'mock_client'
        result = initialize_google_sheets()
        assert result == 'mock_client'

    @patch('gspread.client.Client.open_by_key')
    @patch('gspread.models.Spreadsheet.worksheet')
    def test_upload_to_sheet(self, mock_open, mock_worksheet):
        mock_spreadsheet = Mock()
        mock_open.return_value = mock_spreadsheet
        mock_spreadsheet.worksheet.return_value = mock_worksheet

        df = pd.DataFrame({'sample_id': [1, 2, 3], 'library_id': [4, 5, 6], 'disease': ['A', 'B', 'C']})
        gc = Mock()
        spreadsheet_id = 'fake_spreadsheet_id'
        title = 'sample metadata'

        result = upload_to_sheet(df, gc, spreadsheet_id, title)

        mock_open.assert_called_once_with(spreadsheet_id)
        mock_spreadsheet.worksheet.assert_called_once_with(title)
        mock_worksheet.clear.assert_called_once()

    @patch('gspread.client.Client.open_by_key')
    @patch('gspread.models.Spreadsheet.worksheet')
    @patch('gspread.models.Spreadsheet.batch_update')
    def test_delete_sheet(self, mock_batch_update, mock_worksheet, mock_open):
        mock_spreadsheet = Mock()
        mock_open.return_value = mock_spreadsheet
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        mock_worksheet._properties = {'sheetId': 12345}

        gc = Mock()
        spreadsheet_id = 'fake_spreadsheet_id'
        sheet_title = 'Sheet1'

        result = delete_sheet(spreadsheet_id, sheet_title, gc)

        mock_open.assert_called_once_with(spreadsheet_id)
        mock_spreadsheet.worksheet.assert_called_once_with(sheet_title)
        mock_batch_update.assert_called_once()

class TestFetchSheetsWithIndices(unittest.TestCase):
    @patch('hca_patient_meta.utils.build')
    def test_fetch_sheets_with_indices(self, mock_build):
        # Setup the mock service and response
        mock_service = Mock()
        mock_sheets = Mock()
        mock_execute = Mock(return_value={
            'sheets': [
                {'properties': {'index': 0, 'title': 'dataset metadata'}},
                {'properties': {'index': 1, 'title': 'donor metadata'}},
                {'properties': {'index': 2, 'title': 'sample metadata'}},
                {'properties': {'index': 3, 'title': 'celltype metadata'}}
            ]
        })
        
        mock_build.return_value = mock_service
        mock_service.spreadsheets.return_value = mock_sheets
        mock_sheets.get.return_value.execute = mock_execute
        
        # Call the function with mocks
        credentials = Mock()
        spreadsheet_id = 'fake_spreadsheet_id'
        result = fetch_sheets_with_indices(spreadsheet_id, credentials)
        
        # Assertions
        mock_build.assert_called_once_with('sheets', 'v4', credentials=credentials)
        mock_service.spreadsheets.assert_called_once()
        mock_sheets.get.assert_called_once_with(spreadsheetId=spreadsheet_id, fields='sheets(properties)')
        mock_execute.assert_called_once()
        
        expected_result = {
            0: 'dataset metadata',
            1: 'donor metadata',
            2: 'sample metadata',
            3: 'celltype metadata'
        }
        self.assertEqual(result, expected_result)

if __name__ == '__main__':
    unittest.main()