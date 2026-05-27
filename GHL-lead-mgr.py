import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import re
import io

# ===================================================
# CONFIG
# ===================================================

SPREADSHEET_ID = "1n1daep0zpdeC4ITPoRTYeW7-ayx_rcEh2nGYAeavCL0"
SHEET_NAME = "For REI Upload"

OUTPUT_FILE = "processed_output.csv"

# ===================================================
# GOOGLE SHEETS AUTH
# ===================================================
# REQUIREMENTS:
# pip install pandas gspread google-auth

# Replace with your Google Service Account JSON file path
SERVICE_ACCOUNT_FILE = "service_account.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly"
]

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

def process_google_sheet():

    print("[INFO] Connecting to Google Sheets...")

    # Open spreadsheet
    spreadsheet = client.open_by_key(SPREADSHEET_ID)

    # Open worksheet
    worksheet = spreadsheet.worksheet(SHEET_NAME)

    print("[INFO] Fetching data from sheet...")

    # Get all records
    input_data = worksheet.get_all_records()

    output_data = []

    print(f"[INFO] Processing {len(input_data)} rows...")

    # Process each row
    for row in input_data:

        address = row.get('Address', '')
        first_name = row.get('First Name', '')
        last_name = row.get('Last Name', '')

        # ==========================
        # PHONE NUMBERS
        # ==========================
        phone_numbers = [
            row.get(f'Phone {i}', '')
            for i in range(1, 6)
            if str(row.get(f'Phone {i}', '')).strip()
        ]

        # ==========================
        # PHONE TYPES
        # ==========================
        phone_types = [
            row.get(f'Phone Type {i}', '')
            for i in range(1, 6)
            if str(row.get(f'Phone Type {i}', '')).strip()
        ]

        # ==========================
        # EMAILS
        # ==========================
        emails = [
            clean_email(row.get(f'Email {i}', ''))
            for i in range(1, 4)
            if str(row.get(f'Email {i}', '')).strip()
        ]

        # ==========================
        # MAX ROWS
        # ==========================
        max_rows = max(
            len(phone_numbers),
            len(phone_types),
            len(emails),
            1
        )

        # ==========================
        # BUILD OUTPUT
        # ==========================
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

    print("[INFO] Generating CSV...")

    output_df = pd.DataFrame(output_data)

    output_df.to_csv(
        OUTPUT_FILE,
        index=False,
        encoding='utf-8-sig'
    )

    print(f"[SUCCESS] CSV saved as: {OUTPUT_FILE}")


# ===================================================
# EXECUTION
# ===================================================

if __name__ == "__main__":
    process_google_sheet()
