import os
import json
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
SHEET_NAME = 'PalmBay_ArcGIS_LandONLY'

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

# Function to safely extract text
def extract_text(driver, xpath, default_value="Not Found"):
    try:
        element = WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        return element.text.strip()
    except (NoSuchElementException, TimeoutException):
        return default_value

# Correcting the function
def fetch_data_and_update_sheet():
    try:
        # Authenticate with Google Sheets API
        sheets_service = authenticate_google_sheets()  # Changed 'service' to 'sheets_service'
        sheet = sheets_service.spreadsheets()  # This is the correct object to interact with Sheets API

        # Define the range for the data
        range_ = f"{SHEET_NAME}!A2739:A5474"
        result = sheet.values().get(spreadsheetId=SHEET_ID, range=range_).execute()
        sheet_data = result.get("values", [])
        print(f"Fetched data: {sheet_data}")  # Debug print to check the data
    except Exception as e:
        print(f"Error fetching data from Google Sheets: {e}")
        return  # Exit if there's an issue fetching the sheet data

def process_row(site, i, sheet):
    driver = None  # Initialize the driver variable to None
    try:
        # Create a new WebDriver instance
        options = webdriver.FirefoxOptions()
        options.add_argument("--headless")
        service = Service()
        driver = webdriver.Firefox(service=service, options=options)

        # Navigate to the site
        driver.get('https://www.bcpao.us/propertysearch/#/nav/Search')

        # Input the Site and Search
        site_input = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, '#txtPropertySearch_Pid'))
        )
        site_input.send_keys(site, Keys.RETURN)

        ownership_text = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="cssDetails_Top_Outer"]/div[2]/div/div[1]/div[2]/div[1]'))
        )
        print("result loaded")
    except Exception as e:
        print(f"Error fetching data from Google Sheets: {e}")
    
        

    try:
        # Authenticate with Google Sheets API
        sheets_service = authenticate_google_sheets()  # Changed 'service' to 'sheets_service'
        sheet = sheets_service.spreadsheets()  # This is the correct object to interact with Sheets API

        # Define the range for the data
        range_ = f"{SHEET_NAME}!A2739:A5474"
        result = sheet.values().get(spreadsheetId=SHEET_ID, range=range_).execute()
        sheet_data = result.get("values", [])
        print(f"Fetched data: {sheet_data}")  # Debug print to check the data
    except Exception as e:
        print(f"Error fetching data from Google Sheets: {e}")
        return  # Exit if there's an issue fetching the sheet data

    try:
        # Extract Data
        ownership_text = driver.find_element(By.XPATH, '//*[@id="cssDetails_Top_Outer"]/div[2]/div/div[1]/div[2]/div[1]').text
        sheets_service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!C{i}",
                valueInputOption="RAW",
                body={"values": [[ownership_text]]}
            ).execute()

        additional_text = driver.find_element(By.XPATH, '//*[@id="cssDetails_Top_Outer"]/div[2]/div/div[2]/div[2]/div').text
        sheets_service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!D{i}",
                valueInputOption="RAW",
                body={"values": [[additional_text]]}
            ).execute()

            # Click Value tab and extract property value
        property_value = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="tSalesTransfers"]/tbody/tr[1]/td[2]'))
            ).text
        sheets_service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!E{i}",
                valueInputOption="RAW",
                body={"values": [[property_value]]}
            ).execute()

        building_info = driver.find_element(By.XPATH, '//*[@id="cssDetails_Top_Outer"]/div[2]/div/div[7]/div[2]').text
        sheets_service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!F{i}",
                valueInputOption="RAW",
                body={"values": [[building_info]]}
            ).execute()

        print(f"✅ Row {i} completed.")

    except Exception as e:
        print(f"❌ Error processing row {i}: {e}")

    finally:
        # Close the browser if it was initialized
        if driver:
            driver.quit()
        print(f"🚪 Closed browser instance for Row {i}\n")


# Main data fetching and updating
def fetch_data_and_update_sheet():
    sheets_service = authenticate_google_sheets()
    sheet = sheets_service.spreadsheets()

    # ✅ Fetch data from Google Sheet
    range_ = f"{SHEET_NAME}!A2739:A5474"
    result = sheet.values().get(spreadsheetId=SHEET_ID, range=range_).execute()
    sheet_data = result.get("values", [])

    # ✅ Process each row with a new browser instance
    for i, row in enumerate(sheet_data, start=2739):
        site = row[0].strip() if row else None
        print(f"Processing Name: {site}")

        if not site:
            print(f"Skipping empty row {i}")
            continue

        # ✅ Process the row with a new browser instance
        process_row(site, i, sheet)

    print("🚀 All rows have been processed.")

if __name__ == '__main__':
    fetch_data_and_update_sheet()
