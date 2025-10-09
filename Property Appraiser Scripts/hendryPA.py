import os
import time
from undetected_chromedriver import Chrome, ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# Constants
USER_DATA_DIR = "C:\\Users\\DELL\\AppData\\Local\\Google\\Chrome\\User Data"
PROFILE_DIRECTORY = "Profile 1"
SHEET_ID = "1VUB2NdGSY0l3tuQAfkz8QV2XZpOj2khCB69r5zU1E5A"
SHEET_NAME = "Labelle FL - Vacant Lands"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CREDENTIALS_PATH = os.path.join(os.getcwd(), "credentials.json")
TOKEN_PATH = os.path.join(os.getcwd(), "token.json")
URL = "https://beacon.schneidercorp.com/Application.aspx?AppID=1105&LayerID=27399&PageTypeID=2&PageID=11144"

# Set up undetected Chrome driver
def setup_undetected_chrome_driver():
    """Set up undetected Chrome driver with user profile and debugging options."""
    options = ChromeOptions()
    options.add_argument(f"--user-data-dir={USER_DATA_DIR}")
    options.add_argument(f"--profile-directory={PROFILE_DIRECTORY}")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    return Chrome(options=options, use_subprocess=True)

# Authenticate with Google Sheets
def authenticate_google_sheets():
    """Authenticate and return a Google Sheets API service."""
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    else:
        flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
        creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, "w") as token_file:
            token_file.write(creds.to_json())
    return build("sheets", "v4", credentials=creds)

# Perform human-like mouse movement
def human_like_mouse_movement(driver, element):
    """Simulate human-like mouse movement to an element."""
    actions = ActionChains(driver)
    actions.move_to_element(element).perform()

# Fetch data and update Google Sheet
def fetch_data_and_update_sheet():
    sheets = authenticate_google_sheets()
    sheet = sheets.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f'{SHEET_NAME}!G2:G'
    ).execute()
    sheet_data = sheet.get('values', [])

    # Initialize the WebDriver once
    driver = setup_undetected_chrome_driver()  # Replace with undetected Chrome driver setup if needed
    driver.get(URL)

    for i, row in enumerate(sheet_data):
        site = row[0] if row else None
        if not site or not site.strip():
            print(f"Skipping empty or blank cell at row {i + 2}")
            continue

        try:
            # Navigate back to the initial URL for the next sequence
            driver.get(URL)
            print(f"Processing row {i + 2} with site: {site}")

            # Dismiss warning if present
            try:
                warning_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, '//*[@id="appBody"]/div[4]/div/div/div[2]/div[2]/a[1]'))
                )
                human_like_mouse_movement(driver, warning_button)
                warning_button.click()
                time.sleep(3)
                print("Warning dismissed successfully.")
            except Exception as e:
                print("Warning button not found or clickable, continuing...")

            try:
                site_input = WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="ctlBodyPane_ctl02_ctl01_txtParcelID"]'))
                )
                site_input.send_keys(site)
                site_input.send_keys(Keys.RETURN)
            except Exception as e:
                print(f"Error processing row {i + 2}: {e}")
                continue  # Skip to the next iteration

            # Extract and update ownership text 1
            ownership_text1 = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 
                    '#ctlBodyPane_ctl02_ctl01_rptOwner_ctl00_sprOwnerName1_lnkUpmSearchLinkSuppressed_lnkSearch'
                ))
            ).text
            sheets.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!Z{i + 2}",
                valueInputOption="RAW",
                body={"values": [[ownership_text1]]}
            ).execute()
            
            # Extract and update ownership text 1
            ownership_text2 = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 
                    '#ctlBodyPane_ctl02_ctl01_rptOwner_ctl00_sprOwnerName2_lnkUpmSearchLinkSuppressed_lnkSearch'
                ))
            ).text
            sheets.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!AA{i + 2}",
                valueInputOption="RAW",
                body={"values": [[ownership_text2]]}
            ).execute()

            # Additional data extraction
            additional_text = driver.find_element(By.XPATH,
                '//*[@id="ctlBodyPane_ctl02_ctl01_rptOwner_ctl00_lblOwnerAddress"]'
            ).text
            sheets.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f'{SHEET_NAME}!AB{i + 2}',
                valueInputOption='RAW',
                body={'values': [[additional_text]]}
            ).execute()

            # Property value extraction
            property_value = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH,
                    '//*[@id="ctlBodyPane_ctl11_ctl01_grdSales"]/tbody/tr[1]/td[1]'
                ))
            ).text
            sheets.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f'{SHEET_NAME}!AC{i + 2}',
                valueInputOption='RAW',
                body={'values': [[property_value]]}
            ).execute()

            # Building information extraction
            bldg_info = driver.find_element(By.XPATH,
                '//*[@id="ctlBodyPane_ctl03_ctl01_grdValuation"]/tbody/tr[5]/td[1]'
            ).text
            sheets.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f'{SHEET_NAME}!AD{i + 2}',
                valueInputOption='RAW',
                body={'values': [[bldg_info]]}
            ).execute()
    

        except Exception as e:
            print(f"Error processing row {i + 2}: {e}")
        finally:
            # Clear any entered data or reset state if needed
            pass

    # Quit the driver after all sequences
    driver.quit()

# Main execution
if __name__ == "__main__":
    fetch_data_and_update_sheet()

