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
        self.identifier_regex = re.compile(r".*?Appels-(\d{9}).*")
        self.customer_lookup_col = 5  # Column E for DID numbers
        self.data_columns = [27, 29, 31, 33, 37]  # Columns to update AA, AC, AE, AG, AK
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

    def _get_last_row_column_a(self, worksheet):
        """Finds the last row with data in column A of a worksheet."""
        col_a_values = worksheet.col_values(1)  # Get all values from column A
        for i in reversed(range(len(col_a_values))):
            if col_a_values[i] and str(col_a_values[i]).strip():
                return i + 1  # Row index is 1-based
        return 0  # Return 0 if no data is found in column A

    def _collect_call_data(self, worksheets):
        """Collect data from the 1st and 2nd row of the last 5 rows (columns B-C),
        skipping sheets with fewer than 5 data-containing rows.
        """
        data_map = {}
        for ws in worksheets:
            try:
                num_rows_with_data = self._get_last_row_column_a(ws)
                if num_rows_with_data < 5:
                    logger.warning(
                        f"Skipping {ws.title} as it has fewer than 5 rows with data."
                    )
                    continue

                if match := self.identifier_regex.match(ws.title):
                    did = match.group(1)

                    # Calculate the starting row for the last 5 data-containing rows
                    start_of_last_5 = max(1, num_rows_with_data - 4)

                    # Target rows are the first two within the last 5 data-containing rows
                    target_row_1 = start_of_last_5
                    target_row_2 = start_of_last_5 + 1

                    # Ensure that target rows are valid
                    if target_row_2 <= num_rows_with_data:
                        range_data = ws.get(f"B{target_row_1}:D{target_row_2}")
                        if (
                            len(range_data) == 2
                            and len(range_data[0]) == 3
                            and len(range_data[1]) == 3
                        ):
                            total_recus = range_data[0][0]
                            total_emis = range_data[0][1]
                            total_recus_min_str = range_data[1][0]
                            total_emis_min_str = range_data[1][1]
                            total_duree = range_data[0][2]

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
                            total_duree_min = (
                                int(total_duree.split("s")[0].strip()) // 60
                            )

                            data_map[did] = {
                                "total_recus": total_recus,
                                "total_emis": total_emis,
                                "total_recus_min": total_recus_min,
                                "total_emis_min": total_emis_min,
                                "total_duree_min": total_duree_min,
                            }
                        else:
                            logger.warning(
                                f"Unexpected data format in {ws.title} for call details."
                            )
                    else:
                        logger.warning(
                            f"Insufficient data-containing rows in {ws.title} to find the last 5."
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
                        customer_data["total_recus_min"],
                        customer_data["total_emis"],
                        customer_data["total_emis_min"],
                        customer_data["total_duree_min"],
                    ]
                    for i, col in enumerate(self.data_columns):
                        batch_data.append(
                            {
                                "range": f"{gspread.utils.rowcol_to_a1(row_idx, col)}",
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
    status = "DEPLO"  # or 'DEPLOYED'
    if status == "DEBUG":
        customer_sheet_id = "1WwcwvTpYAdjhGfH60aqPqfk72pZfF5fS4vzEUEH--_w"
    else:
        customer_sheet_id = "1ATCR6d6OfAqPGdozoyA_iLOybRkj7h2-VL7do5ZVuOM"
    tracker = CALLLOGTRACK("creds.json")
    tracker.process_call_logs(
        master_sheet_id="1y6gbUeBBuZvC6gra4N69kWBdqSIork501TqFexaii2w",
        customer_sheet_id=customer_sheet_id,
    )
