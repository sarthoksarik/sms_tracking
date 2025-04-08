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
        self.data_columns = [27, 29, 31, 33]  # Columns to update AA, AC, AE, AG
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
        """Collect data from the 1st and 2nd row of the last 5 rows (columns B-C),
        skipping sheets with fewer than 5 rows.
        """
        data_map = {}
        for ws in worksheets:
            try:
                num_rows = ws.row_count
                if num_rows < 5:
                    logger.warning(f"Skipping {ws.title} as it has fewer than 5 rows.")
                    continue  # Skip to the next worksheet

                if match := self.identifier_regex.match(ws.title):
                    did = match.group(1)

                    # Calculate the starting row for the last 5 rows
                    start_of_last_5 = max(1, num_rows - 4)

                    # Target rows are the first two within the last 5
                    target_row_1 = start_of_last_5
                    target_row_2 = start_of_last_5 + 1

                    # Ensure that target rows are valid (though now redundant with the initial check)
                    if target_row_2 <= num_rows:
                        range_data = ws.get(f"B{target_row_1}:C{target_row_2}")
                        if (
                            len(range_data) == 2
                            and len(range_data[0]) == 2
                            and len(range_data[1]) == 2
                        ):
                            total_recus = range_data[0][0]
                            total_emis = range_data[0][1]
                            total_recus_min_str = range_data[1][0]
                            total_emis_min_str = range_data[1][1]

                            total_recus_min = (
                                total_recus_min_str.split("min")[0].strip()
                                if "min" in total_recus_min_str
                                else "0"
                            )
                            total_emis_min = (
                                total_emis_min_str.split("min")[0].strip()
                                if "min" in total_emis_min_str
                                else "0"
                            )

                            data_map[did] = {
                                "total_recus": total_recus,
                                "total_emis": total_emis,
                                "total_recus_min": total_recus_min,
                                "total_emis_min": total_emis_min,
                            }
                        else:
                            logger.warning(
                                f"Unexpected data format in {ws.title} for call details."
                            )
                    else:
                        logger.warning(
                            f"Insufficient rows in {ws.title} to find the last 5 rows (should not occur)."
                        )

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
                    customer_data = data_map[did]
                    # Add date update
                    batch_data.append({"range": f"B{row_idx}", "values": [[date_str]]})
                    # Add data updates using the correct keys
                    updates = [
                        customer_data["total_recus"],
                        customer_data["total_emis"],
                        customer_data["total_recus_min"],
                        customer_data["total_emis_min"],
                    ]
                    for i, col in enumerate(self.data_columns):
                        batch_data.append(
                            {
                                "range": f"{gspread.utils.rowcol_to_A1(row_idx, col)}",
                                "values": [[updates[i]]],
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
