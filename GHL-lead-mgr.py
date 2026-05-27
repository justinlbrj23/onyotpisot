import pandas as pd
import csv
import re
import gspread
from google.oauth2.service_account import Credentials

# ===================================================
# CONFIG
# ===================================================

SPREADSHEET_ID = "1n1daep0zpdeC4ITPoRTYeW7-ayx_rcEh2nGYAeavCL0"
SHEET_NAME = "For REI Upload"

SERVICE_ACCOUNT_FILE = "service-account.json"

OUTPUT_FILE = "processed_output.csv"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly"
]

# ===================================================
# GOOGLE SHEETS AUTH
# ===================================================

credentials = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=SCOPES
)

client = gspread.authorize(credentials)

# ===================================================
# HELPERS
# ===================================================

def clean_email(email):
    """
    Remove HTML tags from an email string.
    """
    clean = re.compile(r'<.*?>')
    return re.sub(clean, '', str(email)).strip()


# ===================================================
# MAIN PROCESS
# ===================================================

def process_data():

    print("[INFO] Connecting to Google Sheets...")

    # Open spreadsheet
    spreadsheet = client.open_by_key(SPREADSHEET_ID)

    # Open worksheet
    worksheet = spreadsheet.worksheet(SHEET_NAME)

    print("[INFO] Fetching sheet records...")

    # Fetch all rows
    input_data = worksheet.get_all_records()

    print(f"[INFO] Retrieved {len(input_data)} rows")

    # ===================================================
    # OUTPUT STORAGE
    # ===================================================

    output_data = []

    # ===================================================
    # PROCESS EACH ROW
    # ===================================================

    for row in input_data:

        address = row.get('Address', '')
        first_name = row.get('First Name', '')
        last_name = row.get('Last Name', '')

        # ==========================================
        # PHONE NUMBERS
        # ==========================================

        phone_numbers = [
            str(row.get(f'Phone {i}', '')).strip()
            for i in range(1, 6)
            if str(row.get(f'Phone {i}', '')).strip()
        ]

        # ==========================================
        # PHONE TYPES
        # ==========================================

        phone_types = [
            str(row.get(f'Phone Type {i}', '')).strip()
            for i in range(1, 6)
            if str(row.get(f'Phone Type {i}', '')).strip()
        ]

        # ==========================================
        # EMAILS
        # ==========================================

        emails = [
            clean_email(row.get(f'Email {i}', ''))
            for i in range(1, 4)
            if str(row.get(f'Email {i}', '')).strip()
        ]

        # ==========================================
        # DETERMINE MAX ROWS
        # ==========================================

        max_rows = max(
            len(phone_numbers),
            len(phone_types),
            len(emails),
            1
        )

        # ==========================================
        # BUILD OUTPUT ROWS
        # ==========================================

        for i in range(max_rows):

            output_data.append({
                'Address': address,
                'First Name': first_name,
                'Last Name': last_name,
                'Phone Number': phone_numbers[i] if i < len(phone_numbers) else '',
                'Phone Type': phone_types[i] if i < len(phone_types) else '',
                'Email': emails[i] if i < len(emails) else ''
            })

    # ===================================================
    # EXPORT CSV
    # ===================================================

    print("[INFO] Creating CSV file...")

    output_df = pd.DataFrame(output_data)

    output_df.to_csv(
        OUTPUT_FILE,
        index=False,
        encoding='utf-8-sig'
    )

    print(f"[SUCCESS] CSV exported: {OUTPUT_FILE}")


# ===================================================
# EXECUTION
# ===================================================

if __name__ == "__main__":

    try:
        process_data()

    except Exception as e:
        print(f"[ERROR] {str(e)}")
        raise
