import os
import time
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
import urllib3
from urllib3.exceptions import ProtocolError
import ssl
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials  # Correct import for OAuth2 credentials

# Request with retries
def make_request_with_retries(url, retries=3, backoff_factor=1):
    http = urllib3.PoolManager()
    attempt = 0
    while attempt < retries:
        try:
            response = http.request('GET', url)
            return response
        except ProtocolError as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            attempt += 1
            sleep_time = backoff_factor * (2 ** attempt)  # Exponential backoff
            print(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
    raise Exception(f"Failed to fetch {url} after {retries} attempts.")

# Example usage:
url = 'https://www.leepa.org/Search/PropertySearch.aspx'
response = make_request_with_retries(url)
print(response.data)

# Disable SSL verification temporarily (use only for testing)
os.environ['NO_PROXY'] = 'localhost,127.0.0.1'
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
context = ssl.create_default_context()
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE

# Google Sheets setup
SHEET_ID = '1VUB2NdGSY0l3tuQAfkz8QV2XZpOj2khCB69r5zU1E5A'
SHEET_NAME = 'Raw Cape Coral - ArcGIS (lands)'

# Define file paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_PATH = os.path.join(BASE_DIR, "credentials.json")
TOKEN_PATH = os.path.join(BASE_DIR, "token.json")
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Authenticate with Google Sheets API
def authenticate_google_sheets():
    """Authenticate with Google Sheets API."""
    creds = None
    # Check if the token file exists
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    # If no valid credentials, allow the user to login via OAuth
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())  # Refresh token if expired
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)

    # Save the credentials for the next run
    with open(TOKEN_PATH, "w") as token:
        token.write(creds.to_json())

    return build("sheets", "v4", credentials=creds)

# Correcting the function
def fetch_data_and_update_sheet():
    try:
        # Authenticate with Google Sheets API
        sheets_service = authenticate_google_sheets()  # Changed 'service' to 'sheets_service'
        sheet = sheets_service.spreadsheets()  # This is the correct object to interact with Sheets API

        # Define the range for the data
        range_ = f"{SHEET_NAME}!A5001:A8000"
        result = sheet.values().get(spreadsheetId=SHEET_ID, range=range_).execute()
        sheet_data = result.get("values", [])
        print(f"Fetched data: {sheet_data}")  # Debug print to check the data
    except Exception as e:
        print(f"Error fetching data from Google Sheets: {e}")
        return  # Exit if there's an issue fetching the sheet data

    # Web scraping and updating data in Google Sheets
    url = 'https://www.leepa.org/Search/PropertySearch.aspx'

    for i, row in enumerate(sheet_data, start=5001):
        owner = row[0] if row else None
        if not owner or owner.strip() == '':
            print(f"Skipping empty or blank cell at row {i}")
            continue

        print(f"Processing Name: {owner}")

        # Setup Firefox driver with headless option
        options = webdriver.FirefoxOptions()
        options.add_argument("--headless")
        service = Service()  # Selenium WebDriver service (for Firefox)
        driver = webdriver.Firefox(service=service, options=options)

        try:
            driver.get(url)

            # Enter owner name and submit
            strap_input = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.ID, "ctl00_BodyContentPlaceHolder_WebTab1_tmpl0_STRAPTextBox"))
            )
            strap_input.send_keys(owner, Keys.RETURN)

            try:
                # Handle warning pop-up
                WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.ID, "ctl00_BodyContentPlaceHolder_pnlIssues"))
                )
                warning_button = driver.find_element(By.ID, "ctl00_BodyContentPlaceHolder_btnWarning")
                warning_button.click()
            except:
                print("No pop-up found, continuing to next step.")

            time.sleep(1)

            # Navigate to property details
            href = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="ctl00_BodyContentPlaceHolder_WebTab1"]/div/div[1]/div[1]/table/tbody/tr/td[4]/div/div[1]/a'))
            ).get_attribute('href')
            driver.get(href)

            time.sleep(1)

            # Click image to reveal ownership details
            img_element = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="divDisplayParcelOwner"]/div[1]/div/div[1]/a[2]/img'))
            )
            img_element.click()

            ownership_text = driver.find_element(By.XPATH, '//*[@id="ownershipDiv"]/div/ul').text
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!C{i}",
                valueInputOption="RAW",
                body={"values": [[ownership_text]]}
            ).execute()

            additional_text = driver.find_element(By.XPATH, '//*[@id="divDisplayParcelOwner"]/div[1]/div/div[2]/div').text
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!D{i}",
                valueInputOption="RAW",
                body={"values": [[additional_text]]}
            ).execute()

            # Click Value tab and extract property value
            value_tab = driver.find_element(By.ID, "ValuesHyperLink").click()
            property_value = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="valueGrid"]/tbody/tr[2]/td[4]'))
            ).text
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!E{i}",
                valueInputOption="RAW",
                body={"values": [[property_value]]}
            ).execute()

            building_info = driver.find_element(By.XPATH, '//*[@id="divDisplayParcelOwner"]/div[3]/table[1]/tbody/tr[3]/td').text
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!F{i}",
                valueInputOption="RAW",
                body={"values": [[building_info]]}
            ).execute()

            full_site = driver.find_element(By.XPATH, '//*[@id="divDisplayParcelOwner"]/div[2]/div[3]').text
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!S{i}",
                valueInputOption="RAW",
                body={"values": [[full_site]]}
            ).execute()

        except Exception as e:
            print(f"Error processing row {i}: {e}")

        finally:
            driver.quit()

if __name__ == "__main__":
    fetch_data_and_update_sheet()
