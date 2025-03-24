import gspread
import re
from datetime import datetime
import logging
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)


class CALLLOGTRACK:
    def __init__(self, credentials_path):
        self.credentials_path = credentials_path
        self.scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        self._initialize_clients()

        # Configuration
        self.tab_prefix = "Appels-"
        self.identifier_regex = re.compile(r"Appels-(\d{9})")
        self.customer_lookup_col = 5  # Column E for DID numbers
        self.data_columns = [68, 69, 70, 71, 72, 73]  # Columns to update
        self.date_update_col = 2  # Column B for dates

    def _initialize_clients(self):
        """Initialize Google API clients"""
        creds = Credentials.from_service_account_file(
            self.credentials_path, scopes=self.scopes
        )
        self.gspread_client = gspread.authorize(creds)

    def process_call_logs(self, master_sheet_id, customer_sheet_id):
        """
        Main processing method
        :param master_sheet_id: ID of the master call log spreadsheet
        :param customer_sheet_id: ID of the customer info spreadsheet
        """
        try:
            master_sheet = self.gspread_client.open_by_key(master_sheet_id)
            customer_sheet = self.gspread_client.open_by_key(customer_sheet_id)

            call_logs = self._get_call_log_tabs(master_sheet)
            data_map = self._collect_call_data(call_logs)

            if data_map:
                self._update_customer_sheet(customer_sheet, data_map)

        except Exception as e:
            logger.error(f"Processing failed: {e}")

    def _get_call_log_tabs(self, master_sheet):
        """Retrieve all worksheet tabs starting with Appels-"""
        return [
            ws
            for ws in master_sheet.worksheets()
            if ws.title.startswith(self.tab_prefix)
        ]

    def _collect_call_data(self, worksheets):
        """Collect data from 3rd/4th rows (columns B-D)"""
        data_map = {}
        for ws in worksheets:
            try:
                if match := self.identifier_regex.match(ws.title):
                    did = match.group(1)
                    # Get number of rows in the tab
                    num_rows = ws.row_count

                    # Select 4th and 5th rows from the last
                    start_row = max(1, num_rows - 4)
                    end_row = num_rows - 3
                    range_data = ws.get(f"B{start_row}:D{end_row}")
                    # Flatten to single list [B3,C3,D3,B4,C4,D4]
                    flattened = [cell for row in range_data for cell in row]

                    data_map[did] = {}
            except Exception as e:
                logger.error(f"Error processing {ws.title}: {e}")
        return data_map

    def _update_customer_sheet(self, customer_sheet, data_map):
        """Update customer sheet with collected data"""
        try:
            worksheet = customer_sheet.worksheet("Customers")
            dids = worksheet.col_values(self.customer_lookup_col)

            # Prepare date string
            today = datetime.today()
            prev_month = today.month - 1 or 12
            prev_year = today.year if today.month > 1 else today.year - 1
            date_str = datetime(prev_year, prev_month, 1).strftime("%B %Y")

            # Prepare batch update
            batch_data = []
            for row_idx, did in enumerate(dids, start=1):
                if did in data_map:
                    # Add date update
                    batch_data.append({"range": f"B{row_idx}", "values": [[date_str]]})
                    # Add data updates
                    for i, col in enumerate(self.data_columns):
                        batch_data.append(
                            {
                                "range": f"{gspread.utils.rowcol_to_A1(row_idx, col)}",
                                "values": [[data_map[did][i]]],
                            }
                        )

            # Execute batch update in chunks
            chunk_size = 50  # Stay under API limits
            for i in range(0, len(batch_data), chunk_size):
                worksheet.batch_update(batch_data[i : i + chunk_size])

            logger.info(f"Updated {len(data_map)} customer records")

        except Exception as e:
            logger.error(f"Customer sheet update failed: {e}")


# Usage example
if __name__ == "__main__":
    tracker = CALLLOGTRACK("credentials.json")
    tracker.process_call_logs(
        master_sheet_id="your_master_sheet_id",
        customer_sheet_id="your_customer_sheet_id",
    )
