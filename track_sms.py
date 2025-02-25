from sms_track import SMSTRACK
if __name__ == '__main__':
    # Configuration
    CREDENTIALS_PATH = 'creds.json'
    SOURCE_FOLDER_ID = '12P0yrMcumb-2-8t0FNxlGXzUomoya8re'
    TARGET_SHEET_ID = '1ATCR6d6OfAqPGdozoyA_iLOybRkj7h2-VL7do5ZVuOM'

    # Create manager instance
    sheet_manager = SMSTRACK(CREDENTIALS_PATH)
    
    # Process files
    sheet_manager.process_files(
        folder_id=SOURCE_FOLDER_ID,
        target_sheet_id=TARGET_SHEET_ID
    )
