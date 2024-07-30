from hca_metadata_manager.utils import * 
import os
from google_auth_oauthlib.flow import InstalledAppFlow
from time import sleep
import pandas as pd

def apply_dropdowns(spreadsheet_id, credentials, gc, metadata_dfs=None, num_header_rows=1, manual_config_mode=False):
    """
    Apply dropdown configurations to specified Google Sheet based on predefined settings,
    considering additional header rows.

    Args:
        spreadsheet_id (str): The ID of the Google Spreadsheet.
        credentials: Google API credentials used for accessing the sheet.
        gc: Google Sheets client authorized with the gspread library.
        metadata_dfs: Dictionary of DataFrames loaded from a predefined metadata definitions sheet.
        num_header_rows (int): Number of header rows in each sheet which should not have dropdowns.
        manual_config_mode (bool): Whether to use a manual configuration for dropdowns.
    """
    try:
        delete_sheet(spreadsheet_id, "Sheet1", gc)
    except gspread.exceptions.WorksheetNotFound:
        print("Sheet1 does not exist or was already deleted.")

    # Fetch sheet index to title mappings
    sheets_info = fetch_sheets_with_indices(spreadsheet_id, credentials)
    print("Sheets info:", sheets_info)

    # Define or fetch dropdown configuration
    dropdowns_config = {}
    if manual_config_mode:
        print('Using manual dropdown config mode.')
        # example for manual mode
        # dropdowns_config = {
        #     'dataset metadata': {
        #         'reference_genome': ['GRCh38', 'GRCh37', 'GRCm39', 'GRCm38', 'GRCm37', 'not_applicable'],
        #         'alignment_software': ['cellranger_3.0', 'starsolo', 'kallisto_bustools_0.1'],
        #         'intron_inclusion': ['yes', 'no']
        #     },
        #     'donor metadata': {
        #         'organism_ontology_term_id': ['NCBITaxon:9606'],  # Human
        #         'manner_of_death': ['not_applicable', 'unknown', '0', '1', '2', '3', '4'],
        #         'sex_ontology_term_id': ['PATO:0000383', 'PATO:0000384']  # Female, Male
        #     },
        #     'sample metadata': {
        #         'tissue_ontology_term': ['duodenum', 'jejunum', 'ileum', 'ascending_colon', 'transverse_colon', 'descending_colon',
        #                                  'sigmoid_colon', 'rectum', 'anal_canal', 'small_intestine', 'colon', 'caecum',
        #                                  'gastrointestinal_system_mesentery', 'vermiform_appendix'],
        #         'sample_source': ["surgical_donor", "postmortem_donor", "organ_donor"],
        #         'sample_collection_method': ['biopsy', 'surgical_resection', 'brush'],
        #         'tissue_type': ["tissue", "organoid", "cell_culture"],
        #         'sampled_site_condition': ["healthy", "diseased", "adjacent"],
        #         'sample_preservation_method': ['fresh', 'frozen'],
        #         'suspension_type': ['cell', 'nucleus', 'na'],
        #         'is_primary_data': ['FALSE', 'TRUE']
        #     },
        #     'celltype': {
        #         ''
        #     }
        # }
    else:
        for sheet_title, df in metadata_dfs.items():
            dropdowns_config[sheet_title.lower()] = {col: df[col].dropna().unique().tolist() for col in df.columns if not df[col].isnull().all()}

    # Apply dropdowns using the fetched sheet information
    for sheet_index, sheet_title in sheets_info.items():
        tab_config = dropdowns_config.get(sheet_title, {})
        for column_name, values in tab_config.items():
            print('Adding dropdowns to ' + column_name)
            try:
                col_index = get_column_index(spreadsheet_id, sheet_index, column_name, credentials)
                if col_index >= 0:
                    # Calculate the range to apply dropdowns, adjusting for header rows
                    dropdown_range = f"{col_index}{num_header_rows+1}:{col_index}"
                    set_dropdown_list_by_id(spreadsheet_id, sheet_index, dropdown_range, values, credentials)
                else:
                    print(f"Column {column_name} not found in {sheet_title}.")
            except ValueError as e:
                print(e)


# def upload_metadata_to_drive(adata, descriptions, donor_metadata_config, sample_metadata_config,
#                            dataset_metadata_config, celltype_metadata_config, gc, credentials, folder_id):
#     """
#     Process and upload metadata for each study in the AnnData object.

#     Args:
#         adata: AnnData object containing study data.
#         descriptions: DataFrame containing descriptions for metadata fields.
#         donor_metadata_config: Configuration for donor metadata.
#         sample_metadata_config: Configuration for sample metadata.
#         dataset_metadata_config: Configuration for dataset metadata.
#         celltype_metadata_config: Configuration for celltype metadata.
#         gc: Google Sheets client.
#         credentials: Google API credentials.
#         folder_id: ID of the Google Drive folder to move the sheets into.
#     """
#     for study in adata.obs.study.unique():
#         subadata = adata[adata.obs.study == study]
#         study_dir = f'src/harmonized_metadata/{study}'
#         if not os.path.exists(study_dir):
#             os.makedirs(study_dir)

#         # Save metadata to CSV
#         save_metadata_to_csv(subadata, donor_metadata_config, f'{study_dir}/{study}_prefilled_donor_metadata.csv')
#         save_metadata_to_csv(subadata, sample_metadata_config, f'{study_dir}/{study}_prefilled_sample_metadata.csv')
#         save_metadata_to_csv(subadata, dataset_metadata_config, f'{study_dir}/{study}_prefilled_dataset_metadata.csv')
#         save_metadata_to_csv(subadata, celltype_metadata_config, f'{study_dir}/{study}_prefilled_celltype_metadata.csv')

#         # Read saved metadata
#         donorMetaTb = pd.read_csv(f'{study_dir}/{study}_prefilled_donor_metadata.csv')
#         sampleMetaTb = pd.read_csv(f'{study_dir}/{study}_prefilled_sample_metadata.csv')
#         datasetMetaTb = pd.read_csv(f'{study_dir}/{study}_prefilled_dataset_metadata.csv')
#         celltypeMetaTb = pd.read_csv(f'{study_dir}/{study}_prefilled_celltype_metadata.csv')

#         # Concatenate descriptions with metadata
#         donor_descriptions_tb = concatenate_metadata(descriptions, donorMetaTb)
#         sample_descriptions_tb = concatenate_metadata(descriptions, sampleMetaTb)
#         dataset_descriptions_tb = concatenate_metadata(descriptions, datasetMetaTb)
#         celltype_descriptions_tb = concatenate_metadata(descriptions, celltypeMetaTb)

#         # Upload to Google Sheets
#         SHEET_NAME = f"{study} prefilled HCA metadata"
#         spreadsheet = gc.create(SHEET_NAME)
#         file_id = spreadsheet.id

#         spreadsheet = upload_to_sheet(dataset_descriptions_tb, gc, file_id, "dataset metadata")
#         spreadsheet = upload_to_sheet(donor_descriptions_tb, gc, file_id, "donor metadata")
#         spreadsheet = upload_to_sheet(sample_descriptions_tb, gc, file_id, "sample metadata")
#         spreadsheet = upload_to_sheet(celltype_descriptions_tb, gc, file_id, "celltype metadata")

#         sleep(10)
#         spreadsheet = gc.open_by_key(file_id)

#         # Delete "Sheet1", if it exists
#         try:
#             delete_sheet(file_id, "Sheet1", gc)
#         except gspread.exceptions.WorksheetNotFound:
#             print("Sheet1 does not exist or was already deleted.")

#         # Apply dropdowns and formatting
#         apply_dropdowns(file_id, credentials)
#         format_all_sheets(file_id, credentials)

#         # Move the sheet to the HCA gut shared drive
#         move_sheet_in_drive(file_id, folder_id, credentials)
        
#         # Sleep between datasets to avoid API rate limits
#         sleep(15)

def upload_metadata_to_drive(adata, metadata_config, gc, credentials, folder_id):
    """
    Process and upload metadata for each study in the AnnData object, creating separate Google Sheets for Tier 1 and Tier 2 metadata.

    Args:
        adata: AnnData object containing study data.
        metadata_config: Dict with configurations for donor, sample, dataset, and celltype metadata.
        gc: Google Sheets client authorized with the gspread library.
        credentials: Google API credentials.
        folder_id: Google Drive folder ID where the sheets should be moved.
    """
    for study in adata.obs.study.unique():
        subadata = adata[adata.obs.study == study]
        dataset_id = subadata.obs['dataset_id'].iloc[0]  # Assuming 'dataset_id' is consistent within a study

        # Define tiers and associated metadata types
        tiers = {
            'Tier 1': ['Dataset', 'Donor', 'Sample', 'CellType'],
            'Tier 2': ['Donor', 'Sample']
        }
        # Process each tier
        for tier, meta_types in tiers.items():
            SHEET_NAME = f"{dataset_id}_HCA_{tier.lower()}_metadata"
            spreadsheet = gc.create(SHEET_NAME)
            file_id = spreadsheet.id
            # Process each metadata type for the current tier
            for meta_type in meta_types:
                tab_name = f"{meta_type.lower()} metadata"
                # Assuming metadata is stored directly in adata.obs
                metadata_tb = subadata.obs[[col for col in subadata.obs.columns if col.startswith(meta_type.lower())]]
                upload_to_sheet(metadata_tb, gc, file_id, tab_name)
            # Delete the default "Sheet1", if it exists
            try:
                delete_sheet(file_id, "Sheet1", gc)
            except gspread.exceptions.WorksheetNotFound:
                print("Sheet1 does not exist or was already deleted.")
            # Apply dropdowns
            apply_dropdowns(file_id, credentials, gc, metadata_dfs=metadata_dfs, num_header_rows=5)
            # Move the sheet to the designated Google Drive folder
            move_sheet_in_drive(file_id, folder_id, credentials)
            # Sleep to avoid hitting API rate limits
            sleep(15)

def generate_empty_metadata_entry_sheets(metadata_dfs, gc, credentials, folder_id, dataset_id, num_header_rows=5):
    """
    Create Google Sheets for Tier 1 and Tier 2 metadata without input from an AnnData object.

    Args:
        metadata_dfs: Dictionary of DataFrames loaded from a predefined metadata definitions sheet.
        gc: Google Sheets client authorized with the gspread library.
        credentials: Google API credentials.
        folder_id: Google Drive folder ID where the sheets should be moved.
    """
    dataset_id = dataset_id  # Static dataset ID as requested

    # Define the tiers and associated tabs
    tiers = {
        'Tier 1': ['Tier 1 Dataset Metadata', 'Tier 1 Donor Metadata', 'Tier 1 Sample Metadata'],
        'Tier 2': ['Tier 2 Donor Metadata', 'Tier 2 Sample Metadata']
    }

    for tier, tabs in tiers.items():
        SHEET_NAME = f"{dataset_id}_HCA_{tier.lower()}_metadata"
        spreadsheet = gc.create(SHEET_NAME)
        file_id = spreadsheet.id

        for tab in tabs:
            tab_name = tab.replace('Tier 1 ', '').replace('Tier 2 ', '')
            if tab in metadata_dfs:
                metadata_tb = metadata_dfs[tab]
                # Ensure the DataFrame has at least `num_header_rows` rows for dropdown application
                while len(metadata_tb) < num_header_rows:
                    metadata_tb = pd.concat([metadata_tb, pd.DataFrame([{}])], ignore_index=True)
                upload_to_sheet(metadata_tb, gc, file_id, tab_name)
            else:
                print(f"Missing metadata for {tab}")

        try:
            delete_sheet(file_id, "Sheet1", gc)
        except gspread.exceptions.WorksheetNotFound:
            print("Sheet1 does not exist or was already deleted.")

        apply_dropdowns(file_id, credentials, gc, metadata_dfs=metadata_dfs, num_header_rows=5)
        move_sheet_in_drive(file_id, folder_id, credentials)
        sleep(15)