import pandas as pd
import re
import gspread
from google.oauth2.service_account import Credentials

# ===================================================
# CONFIG
# ===================================================

SPREADSHEET_ID = "1C-85RMXealULter_fPjpP1aWaGVrgf8b2duWkowlgNM"
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


def normalize_header(header):
    """
    Normalize a header for reliable matching.

    This removes extra spaces and makes header matching
    case-insensitive.
    """
    return re.sub(r"\s+", " ", str(header)).strip().casefold()


def find_column_index(headers, column_name):
    """
    Return the index of a required column.

    Header matching is case-insensitive and ignores
    leading, trailing, or repeated spaces.
    """
    normalized_target = normalize_header(column_name)

    for index, header in enumerate(headers):
        if normalize_header(header) == normalized_target:
            return index

    raise ValueError(
        f'Required column "{column_name}" was not found in the sheet.'
    )


def find_column_indexes(headers, column_name):
    """
    Return all indexes matching a duplicated column name.
    """
    normalized_target = normalize_header(column_name)

    return [
        index
        for index, header in enumerate(headers)
        if normalize_header(header) == normalized_target
    ]


def find_email_indexes(headers):
    """
    Return all columns whose header starts with 'Email'.

    Examples:
        Email
        Email 2
        Email Address
        Email Address 2
    """
    return [
        index
        for index, header in enumerate(headers)
        if normalize_header(header).startswith("email")
    ]


def get_cell_value(row, column_index):
    """
    Safely retrieve and clean a row value.
    """
    if column_index >= len(row):
        return ""

    return str(row[column_index]).strip()


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

    print(f"[INFO] Retrieved {len(data_rows)} data rows")

    # ==========================================
    # FIND CORE COLUMN INDEXES
    # ==========================================

    site_idx = find_column_index(headers, "Site")
    first_name_idx = find_column_index(headers, "First Name")
    last_name_idx = find_column_index(headers, "Last Name")

    # ==========================================
    # FIND ADDITIONAL COLUMN INDEXES
    # ==========================================

    city_idx = find_column_index(headers, "City")
    state_idx = find_column_index(headers, "State")
    apn_idx = find_column_index(headers, "APN")
    crexi_link_idx = find_column_index(headers, "CREXI Link")
    property_type_idx = find_column_index(headers, "Property Type")
    dor_name_idx = find_column_index(headers, "DOR Name")
    lender_name_idx = find_column_index(headers, "Lender Name")
    loan_amount_idx = find_column_index(headers, "Loan Amount")
    loan_type_idx = find_column_index(headers, "Loan Type")
    interest_rate_idx = find_column_index(headers, "Interest Rate")
    maturity_date_idx = find_column_index(headers, "Maturity Date")

    # ==========================================
    # FIND REPEATED CONTACT COLUMN INDEXES
    # ==========================================

    phone_indexes = find_column_indexes(
        headers,
        "Phone Number"
    )

    phone_type_indexes = find_column_indexes(
        headers,
        "Phone Type"
    )

    email_indexes = find_email_indexes(headers)

    print(f"[INFO] Phone columns found: {len(phone_indexes)}")
    print(f"[INFO] Phone Type columns found: {len(phone_type_indexes)}")
    print(f"[INFO] Email columns found: {len(email_indexes)}")

    if not phone_indexes:
        print("[WARNING] No Phone Number columns were found.")

    if not phone_type_indexes:
        print("[WARNING] No Phone Type columns were found.")

    if not email_indexes:
        print("[WARNING] No Email columns were found.")

    if len(phone_indexes) != len(phone_type_indexes):
        print(
            "[WARNING] The number of Phone Number columns does not "
            "match the number of Phone Type columns."
        )

    # ==========================================
    # OUTPUT STORAGE
    # ==========================================

    output_data = []

    # ==========================================
    # PROCESS EACH ORIGINAL SHEET ROW
    # ==========================================

    for sheet_row_number, original_row in enumerate(
        data_rows,
        start=2
    ):
        # Create a copy so the original result is not modified
        row = list(original_row)

        # Prevent short rows from crashing
        while len(row) < len(headers):
            row.append("")

        # ======================================
        # BASIC FIELDS
        # ======================================

        address = get_cell_value(row, site_idx)
        first_name = get_cell_value(row, first_name_idx)
        last_name = get_cell_value(row, last_name_idx)

        # ======================================
        # ADDITIONAL PROPERTY AND LOAN FIELDS
        # ======================================

        city = get_cell_value(row, city_idx)
        state = get_cell_value(row, state_idx)
        apn = get_cell_value(row, apn_idx)
        crexi_link = get_cell_value(row, crexi_link_idx)
        property_type = get_cell_value(row, property_type_idx)
        dor_name = get_cell_value(row, dor_name_idx)
        lender_name = get_cell_value(row, lender_name_idx)
        loan_amount = get_cell_value(row, loan_amount_idx)
        loan_type = get_cell_value(row, loan_type_idx)
        interest_rate = get_cell_value(row, interest_rate_idx)
        maturity_date = get_cell_value(row, maturity_date_idx)

        # ======================================
        # DETERMINE CONTACT SLOT COUNT
        # ======================================

        max_contact_slots = max(
            len(phone_indexes),
            len(phone_type_indexes),
            len(email_indexes),
            1
        )

        # ======================================
        # BUILD EXPANDED OUTPUT ROWS
        # ======================================

        contact_rows_added = 0

        for i in range(max_contact_slots):

            phone_number = (
                get_cell_value(row, phone_indexes[i])
                if i < len(phone_indexes)
                else ""
            )

            phone_type = (
                get_cell_value(row, phone_type_indexes[i])
                if i < len(phone_type_indexes)
                else ""
            )

            email = (
                clean_email(get_cell_value(row, email_indexes[i]))
                if i < len(email_indexes)
                else ""
            )

            # Skip completely empty secondary contact slots.
            # Always retain at least one output row per sheet row.
            if (
                i > 0
                and not phone_number
                and not phone_type
                and not email
            ):
                continue

            output_data.append({
                "Address": address,
                "City": city,
                "State": state,
                "APN": apn,
                "CREXI Link": crexi_link,
                "Property Type": property_type,
                "DOR Name": dor_name,
                "Lender Name": lender_name,
                "Loan Amount": loan_amount,
                "Loan Type": loan_type,
                "Interest Rate": interest_rate,
                "Maturity Date": maturity_date,
                "First Name": first_name,
                "Last Name": last_name,
                "Phone Number": phone_number,
                "Phone Type": phone_type,
                "Email": email
            })

            contact_rows_added += 1

        print(
            f"[INFO] Sheet row {sheet_row_number}: "
            f"{contact_rows_added} output row(s) created"
        )

    # ==========================================
    # EXPORT CSV
    # ==========================================

    print("[INFO] Creating CSV file...")

    output_columns = [
        "Address",
        "City",
        "State",
        "APN",
        "CREXI Link",
        "Property Type",
        "DOR Name",
        "Lender Name",
        "Loan Amount",
        "Loan Type",
        "Interest Rate",
        "Maturity Date",
        "First Name",
        "Last Name",
        "Phone Number",
        "Phone Type",
        "Email"
    ]

    output_df = pd.DataFrame(
        output_data,
        columns=output_columns
    )

    output_df.to_csv(
        OUTPUT_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    print(f"[SUCCESS] CSV exported: {OUTPUT_FILE}")
    print(f"[SUCCESS] Total output rows: {len(output_df)}")


# ===================================================
# EXECUTION
# ===================================================

if __name__ == "__main__":

    try:
        process_data()

    except Exception as e:
        print(f"[ERROR] {str(e)}")
        raise
