from sms_track import SMSTRACK
if __name__ == '__main__':
    # Configuration
    CREDENTIALS_PATH = 'credentials.json'
    SOURCE_FOLDER_ID = 'your_source_folder_id'
    TARGET_SHEET_ID = 'your_target_sheet_id'

    # Create manager instance
    sheet_manager = SMSTRACK(CREDENTIALS_PATH)
    
    # Process files
    sheet_manager.process_files(
        folder_id=SOURCE_FOLDER_ID,
        target_sheet_id=TARGET_SHEET_ID
    )
