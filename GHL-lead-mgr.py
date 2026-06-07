import pandas as pd
import re
import gspread
from google.oauth2.service_account import Credentials

# ===================================================
# CONFIG
# ===================================================

SPREADSHEET_ID = "1LTaEIiJW1gUwaNBED5PF_pH8xqOGxhqMqTTO-HFu6sY"
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
    clean = re.compile(r"<.*?>")
    return re.sub(clean, "", str(email)).strip()


# ===================================================
# MAIN PROCESS
# ===================================================

def process_data():

    print("[INFO] Connecting to Google Sheets...")

    # ==========================================
    # OPEN SHEET
    # ==========================================

    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    worksheet = spreadsheet.worksheet(SHEET_NAME)

    print("[INFO] Fetching raw sheet data...")

    # ==========================================
    # FETCH RAW DATA
    # ==========================================

    rows = worksheet.get_all_values()

    if not rows:
        raise Exception("Sheet is empty.")

    headers = rows[0]
    data_rows = rows[1:]

    print(f"[INFO] Retrieved {len(data_rows)} rows")

    # ==========================================
    # FIND COLUMN INDEXES
    # ==========================================

    # Core fields
    site_idx = headers.index("Site")
    first_name_idx = headers.index("First Name")
    last_name_idx = headers.index("Last Name")

    # Duplicate phone columns
    phone_indexes = [
        i for i, h in enumerate(headers)
        if h.strip() == "Phone Number"
    ]

    # Duplicate phone type columns
    phone_type_indexes = [
        i for i, h in enumerate(headers)
        if h.strip() == "Phone Type"
    ]

    # Email columns
    email_indexes = [
        i for i, h in enumerate(headers)
        if h.strip().startswith("Email")
    ]

    print(f"[INFO] Phone columns found: {len(phone_indexes)}")
    print(f"[INFO] Phone Type columns found: {len(phone_type_indexes)}")
    print(f"[INFO] Email columns found: {len(email_indexes)}")

    # ==========================================
    # OUTPUT STORAGE
    # ==========================================

    output_data = []

    # ==========================================
    # PROCESS EACH ROW
    # ==========================================

    for row in data_rows:

        # Prevent short rows from crashing
        while len(row) < len(headers):
            row.append("")

        # ======================================
        # BASIC FIELDS
        # ======================================

        address = row[site_idx].strip()
        first_name = row[first_name_idx].strip()
        last_name = row[last_name_idx].strip()

        # ======================================
        # PHONE NUMBERS
        # ======================================

        phone_numbers = []

        for idx in phone_indexes:

            value = row[idx].strip()

            if value:
                phone_numbers.append(value)

        # ======================================
        # PHONE TYPES
        # ======================================

        phone_types = []

        for idx in phone_type_indexes:

            value = row[idx].strip()

            if value:
                phone_types.append(value)

        # ======================================
        # EMAILS
        # ======================================

        emails = []

        for idx in email_indexes:

            value = clean_email(row[idx])

            if value:
                emails.append(value)

        # ======================================
        # DETERMINE MAX ROWS
        # ======================================

        max_rows = max(
            len(phone_numbers),
            len(phone_types),
            len(emails),
            1
        )

        # ======================================
        # BUILD OUTPUT ROWS
        # ======================================

        for i in range(max_rows):

            output_data.append({
                "Address": address,
                "First Name": first_name,
                "Last Name": last_name,
                "Phone Number": (
                    phone_numbers[i]
                    if i < len(phone_numbers)
                    else ""
                ),
                "Phone Type": (
                    phone_types[i]
                    if i < len(phone_types)
                    else ""
                ),
                "Email": (
                    emails[i]
                    if i < len(emails)
                    else ""
                )
            })

    # ==========================================
    # EXPORT CSV
    # ==========================================

    print("[INFO] Creating CSV file...")

    output_df = pd.DataFrame(output_data)

    output_df.to_csv(
        OUTPUT_FILE,
        index=False,
        encoding="utf-8-sig"
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
