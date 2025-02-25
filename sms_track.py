import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

class GoogleSheetManager:
    def __init__(self, credentials_path):
        self.credentials_path = credentials_path
        self.scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        self._initialize_clients()
        
    def _initialize_clients(self):
        """Initialize Google API clients with credentials"""
        creds = Credentials.from_service_account_file(
            self.credentials_path, 
            scopes=self.scopes
        )
        self.gspread_client = gspread.authorize(creds)
        self.drive_service = build('drive', 'v3', credentials=creds)

    def search_files(self, folder_id, name_pattern):
        """
        Search for files in a Google Drive folder
        :param folder_id: ID of the Drive folder to search
        :param name_pattern: Pattern to match in filenames
        :return: List of matching file dictionaries (id, name)
        """
        query =(
            f"'{folder_id}' in parents "
            f"and mimeType='application/vnd.google-apps.spreadsheet' "
            f"and name contains '{name_pattern}'"
        )
        results = self.drive_service.files().list(
            q=query,
            fields="files(id, name)"
        ).execute()
        return results.get('files', [])

    def last_months_smscount(self, sheet_id, column=2):
        """
        Extract the last value from a specified column
        :param sheet_id: Google Sheet ID
        :param column: Column number to read (default: 2)
        :return: Value from last cell or None
        """
        try:
            sheet = self.gspread_client.open_by_key(sheet_id).sheet1
            col_values = sheet.col_values(column)
            return col_values[-1] if col_values else None
        except Exception as e:
            print(f"Error reading sheet {sheet_id}: {str(e)}")
            return None

    def update_target_sheet(self, target_sheet_id, phone_number, value):
        """
        Update target sheet with extracted value
        :param target_sheet_id: ID of the target Google Sheet
        :param phone_number: Phone number to search in column 5
        :param value: Value to write to column 6
        """
        try:
            sheet = self.gspread_client.open_by_key(target_sheet_id).sheet1
            col_values = sheet.col_values(5)
            
            if phone_number in col_values:
                row_index = col_values.index(phone_number) + 1
                sheet.update_cell(row_index, 6, value)
                print(f"Updated {phone_number} with value: {value}")
            else:
                print(f"Phone number {phone_number} not found in target sheet")
                
        except Exception as e:
            print(f"Error updating target sheet: {str(e)}")

    def process_files(self, folder_id, target_sheet_id, name_pattern="DiD3-"):
        """
        Main processing method to handle all files
        :param folder_id: Drive folder ID to process
        :param target_sheet_id: Target sheet ID for updates
        :param name_pattern: Filename pattern to match
        """
        files = self.search_files(folder_id, name_pattern)
        
        for file in files:
            file_name = file['name']
            file_id = file['id']
            
            # Extract phone number from filename
            try:
                phone_number = file_name.split('-')[1].split('.')[0]
            except IndexError:
                print(f"Skipping invalid filename format: {file_name}")
                continue
            
            # Extract value from source sheet
            last_value = self.extract_last_cell_value(file_id)
            if not last_value:
                continue
                
            # Update target sheet
            self.update_target_sheet(target_sheet_id, phone_number, last_value)

# Example usage
if __name__ == '__main__':
    # Configuration
    CREDENTIALS_PATH = 'credentials.json'
    SOURCE_FOLDER_ID = 'your_source_folder_id'
    TARGET_SHEET_ID = 'your_target_sheet_id'

    # Create manager instance
    sheet_manager = GoogleSheetManager(CREDENTIALS_PATH)
    
    # Process files
    sheet_manager.process_files(
        folder_id=SOURCE_FOLDER_ID,
        target_sheet_id=TARGET_SHEET_ID
    )