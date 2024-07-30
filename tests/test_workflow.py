import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from hca_metadata_manager.workflow import generate_empty_metadata_entry_sheets  # Adjust the import based on your actual module structure


class TestGenerateEmptyMetadataSheets(unittest.TestCase):
    def setUp(self):
        # Setup mock for Google Sheets client and credentials
        self.gc = MagicMock()
        self.credentials = MagicMock()
        self.folder_id = "mock_folder_id"
        self.dataset_id = "Kimler2025"

        # Mock metadata definitions as if they were loaded from Google Sheets
        self.metadata_dfs = {
            'Tier 1 Dataset Metadata': pd.DataFrame({'column': ['value1', 'value2']}),
            'Tier 1 Donor Metadata': pd.DataFrame({'column': ['value3', 'value4']}),
            # Add more mock metadata definitions as necessary
        }

    def test_empty_sheet_generation(self):
        # This tests that the function can be called without errors
        try:
            generate_empty_metadata_entry_sheets(
                metadata_dfs=self.metadata_dfs,
                gc=self.gc,
                credentials=self.credentials,
                folder_id=self.folder_id,
                dataset_id=self.dataset_id
            )
            execution_passed = True
        except Exception as e:
            execution_passed = False
        
        self.assertTrue(execution_passed, "The function should execute without errors.")

    def test_correct_calls_made(self):
        # This tests that the correct Google Sheets functions are called
        generate_empty_metadata_entry_sheets(
            metadata_dfs=self.metadata_dfs,
            gc=self.gc,
            credentials=self.credentials,
            folder_id=self.folder_id,
            dataset_id=self.dataset_id
        )
        
        # Assert that a new spreadsheet is created for each tier
        self.assertEqual(self.gc.create.call_count, 2, "Two sheets should be created, one for each tier.")
        # More assertions can be added to check other behaviors

if __name__ == '__main__':
    unittest.main()
