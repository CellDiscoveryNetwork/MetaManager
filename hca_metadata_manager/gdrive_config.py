import os
from google.oauth2.credentials import Credentials

# Google Sheets config
# For this metadata manager, we need google sheets and google drive
GOOGLE_API_CONFIG = {
    'api_service_name': 'sheets',
    'api_version': 'v4',
    'credentials_file': '/Users/kylekimler/OAuth_kk_metadata_uploader2.json',
    'scopes': ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
}

def authenticate_with_google(scopes, CREDENTIALS_FILE):
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', scopes)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, scopes)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds
