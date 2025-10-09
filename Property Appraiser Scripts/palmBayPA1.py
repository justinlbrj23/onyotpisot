import os
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from selenium.common.exceptions import NoSuchElementException, TimeoutException

# Google Sheets setup
SHEET_ID = '1VUB2NdGSY0l3tuQAfkz8QV2XZpOj2khCB69r5zU1E5A'
SHEET_NAME = 'Palm Bay - ArcGIS RAW'

# Define file paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_PATH = os.path.join(BASE_DIR, "credentials.json")
TOKEN_PATH = os.path.join(BASE_DIR, "token.json")
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Authenticate with Google Sheets API
def authenticate_google_sheets():
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, "w") as token:
            token.write(creds.to_json())
    return build("sheets", "v4", credentials=creds)

# Function to process a single row
def process_row(driver, sheet_service, site, i):
    try:
        # Navigate to the site
        driver.get('https://www.bcpao.us/propertysearch/#/nav/Search')

        # Input the Site ID and search
        site_input = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, '#txtPropertySearch_Pid'))
        )
        site_input.send_keys(site, Keys.RETURN)

        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="cssDetails_Top_Outer"]/div[2]/div/div[1]/div[2]/div[1]'))
        )
        print("✅ Result loaded")

        # Extract Data
        ownership_text = driver.find_element(By.XPATH, '//*[@id="cssDetails_Top_Outer"]/div[2]/div/div[1]/div[2]/div[1]').text
        additional_text = driver.find_element(By.XPATH, '//*[@id="cssDetails_Top_Outer"]/div[2]/div/div[2]/div[2]/div').text
        property_value = WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="tSalesTransfers"]/tbody/tr[1]/td[2]'))
        ).text
        building_info = driver.find_element(By.XPATH, '//*[@id="cssDetails_Top_Outer"]/div[2]/div/div[7]/div[2]').text

        # Update Google Sheet
        updates = [
            (f"{SHEET_NAME}!C{i}", [[ownership_text]]),
            (f"{SHEET_NAME}!D{i}", [[additional_text]]),
            (f"{SHEET_NAME}!E{i}", [[property_value]]),
            (f"{SHEET_NAME}!F{i}", [[building_info]])
        ]

        for rng, val in updates:
            sheet_service.values().update(
                spreadsheetId=SHEET_ID,
                range=rng,
                valueInputOption="RAW",
                body={"values": val}
            ).execute()

        print(f"✅ Row {i} processed.")

    except Exception as e:
        print(f"❌ Error processing row {i}: {e}")

# Main function
def fetch_data_and_update_sheet():
    # Authenticate once
    sheets_service = authenticate_google_sheets()
    sheet = sheets_service.spreadsheets()

    # Fetch site data and check column G (skip rows where G is filled)
    range_sites = f"{SHEET_NAME}!A2:A"
    range_checks = f"{SHEET_NAME}!G2:G"
    site_data = sheet.values().get(spreadsheetId=SHEET_ID, range=range_sites).execute().get("values", [])
    check_data = sheet.values().get(spreadsheetId=SHEET_ID, range=range_checks).execute().get("values", [])

    # Pad check_data to match site_data
    while len(check_data) < len(site_data):
        check_data.append([])

    # Start the browser only once
    options = webdriver.FirefoxOptions()
    options.add_argument("--headless")
    driver = webdriver.Firefox(service=Service(), options=options)

    try:
        for i, (site_row, check_row) in enumerate(zip(site_data, check_data), start=2):
            site = site_row[0].strip() if site_row else None
            check_value = check_row[0].strip() if check_row else ""

            print(f"🔎 Row {i}: Site = {site}, G = '{check_value}'")

            if not site:
                print(f"⚠️ Skipping row {i} (empty site).")
                continue

            if check_value:
                print(f"⏭️ Skipping row {i} (G not empty).")
                continue

            process_row(driver, sheets_service, site, i)

    finally:
        driver.quit()
        print("🚪 Browser closed.")

    print("🚀 All applicable rows have been processed.")

if __name__ == '__main__':
    fetch_data_and_update_sheet()
