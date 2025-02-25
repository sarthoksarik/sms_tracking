import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime
import re
import logging


# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Change to logging.DEBUG for more detailed output during development
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



class SMSTRACK:
    def __init__(self, credentials_path):
        self.credentials_path = credentials_path
        self.scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        self._initialize_clients()
        self.month_col = 1
        self.did_col = 5
        self.update_col = 73
        
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
        Search for files in a Google Drive folder.
        :param folder_id: ID of the Drive folder to search.
        :param name_pattern: Pattern to match in filenames.
        :return: List of matching file dictionaries (id, name).
        """
        query = (
            f"'{folder_id}' in parents "
            f"and mimeType='application/vnd.google-apps.spreadsheet' "
            f"and name contains '{name_pattern}'"
        )
        results = self.drive_service.files().list(
            q=query,
            fields="files(id, name)"
        ).execute()
        return results.get('files', [])

    def get_last_month_smscount(self, sheet_id):
        """
        Retrieves the SMS count from column 3 for the previous month
        (formatted as 'February 2025') found in column 2 of the sheet.
        
        :param sheet_id: Google Sheet ID to look into.
        :return: The SMS count value as an integer (or None if not found).
        """
        
        try:
            # Open the sheet and access the "SMS Logs" worksheet
            sheet = self.gspread_client.open_by_key(sheet_id).worksheet("SMS Logs")
            
            # Get all values from column 3 (which should contain month-year strings)
            month_values = sheet.col_values(self.month_col)
            
            # Look for the target month string in the column
            if self.target_date_str in month_values:
                # gspread uses 1-indexing for rows.
                row_index = month_values.index(self.target_date_str) + 1
                # Get the corresponding SMS count  for that row
                sms_value = sheet.cell(row_index, self.month_col + 1).value
                return int(sms_value or 0)
            else:
                logger.warning(f"Month '{self.target_date_str}' not found in column {self.month_col} of sheet {sheet_id}.")
                return None
        except Exception as e:
            logger.error(f"Error reading sheet {sheet_id}: {e}")
            return None

    def update_target_sheet(self, target_sheet_id, phone_number, value):
        """
        Update target sheet with extracted value.
        :param target_sheet_id: ID of the target Google Sheet.
        :param phone_number: Phone number to search in column 5.
        :param value: Value to write to column 6.
        """
        try:
            sheet = self.gspread_client.open_by_key(target_sheet_id).worksheet("Customers")
            col_values = sheet.col_values(self.did_col)
            
            if phone_number in col_values:
                row_index = col_values.index(phone_number) + 1
                sheet.update_cell(row_index, self.update_col, value)
                logger.info(f"Updated {phone_number} with value: {value}")
            else:
                logger.warning(f"Phone number {phone_number} not found in target sheet.")
        except Exception as e:
            logger.error(f"Error updating target sheet: {e}")

    def process_files(self, folder_id, target_sheet_id, name_pattern="DID3-"):
        """
        Main processing method to handle all files.
        :param folder_id: Drive folder ID to process.
        :param target_sheet_id: Target sheet ID for updates.
        :param name_pattern: Filename pattern to match.
        """
        files = self.search_files(folder_id, name_pattern)
        today = datetime.today()
        year = today.year
        month = today.month - 1
        if month == 0:  # Handle the January edge case
            month = 12
            year -= 1
        self.target_date_str = datetime(year, month, 1).strftime("%B %Y")
        for file in files:
            file_name = file['name']
            file_id = file['id']
            
            # Extract phone number from filename using regex
            match = re.search(r'DID3(?:-[^-]*)*-(\d{9,})\b', file_name)
            if not match:
                logger.warning(f"Skipping invalid filename format: {file_name}")
                continue
            
            phone_number = match.group(1)
            
            # Extract value from source sheet
            last_value = self.get_last_month_smscount(file_id)
            if last_value is None:
                continue
                
            # Update target sheet
            self.update_target_sheet(target_sheet_id, phone_number, last_value)


# run
# if __name__ == '__main__':
#     # Configuration
#     CREDENTIALS_PATH = 'credentials.json'
#     SOURCE_FOLDER_ID = 'your_source_folder_id'
#     TARGET_SHEET_ID = 'your_target_sheet_id'

#     # Create manager instance
#     sheet_manager = SMSTRACK(CREDENTIALS_PATH)
    
#     # Process files
#     sheet_manager.process_files(
#         folder_id=SOURCE_FOLDER_ID,
#         target_sheet_id=TARGET_SHEET_ID
#     )
