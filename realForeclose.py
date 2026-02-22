import os
import re
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle

# Google Sheets configuration
SHEET_ID = '1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA'
SHEET_NAME = 'Palm Beach - Taxdeed'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Google Sheets authentication
def authenticate_google_sheets():
    creds = None
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)

        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)

    service = build("sheets", "v4", credentials=creds)
    return service.spreadsheets()

# Scraping function
def scrape_data(url):
    options = Options()
    driver = webdriver.Edge(options=options)
    extracted_data = []
    try:
        driver.get(url)
        time.sleep(10)

        current_page = 1
        total_pages = 1

        while current_page <= total_pages:
            try:
                pagination_element = driver.find_element(By.CSS_SELECTOR, "#BID_WINDOW_CONTAINER > div.Head_W > div:nth-child(3) > span.PageText")
                pagination_text = pagination_element.text
                match = re.search(r"page of (\d+)", pagination_text, re.IGNORECASE)
                total_pages = int(match.group(1)) if match else 1
            except Exception as e:
                print(f"Error parsing pagination: {e}")
                break

            batches = driver.find_elements(By.CSS_SELECTOR, ".PREVIEW")
            for batch in batches:
                try:
                    auction_date_time = batch.find_element(By.CSS_SELECTOR, "div.AUCTION_STATS > div.ASTAT_MSGB.Astat_DATA").text
                    if auction_date_time.lower() == "redeemed":
                        continue

                    case_number = batch.find_element(By.CSS_SELECTOR, "div.AUCTION_DETAILS > table > tbody > tr:nth-child(2) > td").text
                    fj_amount = batch.find_element(By.CSS_SELECTOR, "div.AUCTION_DETAILS > table > tbody > tr:nth-child(4) > td").text
                    parcel = batch.find_element(By.CSS_SELECTOR, "div.AUCTION_DETAILS > table > tbody > tr:nth-child(5) > td > a").text
                    parcel_href = batch.find_element(By.CSS_SELECTOR, "div.AUCTION_DETAILS > table > tbody > tr:nth-child(5) > td > a").get_attribute("href")
                    property_address = batch.find_element(By.CSS_SELECTOR, "div.AUCTION_DETAILS > table > tbody > tr:nth-child(6) > td").text
                    city_zip = batch.find_element(By.CSS_SELECTOR, "div.AUCTION_DETAILS > table > tbody > tr:nth-child(7) > td").text

                    extracted_data.append([
                        auction_date_time.strip(),
                        case_number.strip(),
                        fj_amount.strip(),
                        parcel.strip(),
                        property_address.strip(),
                        city_zip.strip(),
                        parcel_href.strip()
                    ])
                except Exception as e:
                    print(f"Error extracting batch data: {e}")

            try:
                if current_page < total_pages:
                    next_button = driver.find_element(By.CSS_SELECTOR, "#BID_WINDOW_CONTAINER > div.Head_W > div:nth-child(3) > span.PageRight")
                    next_button.click()
                    time.sleep(5)
                    current_page += 1
                else:
                    break
            except TimeoutException:
                break

        return extracted_data

    except Exception as e:
        print(f"Scraping error: {e}")
    finally:
        driver.quit()

# Log data to Google Sheets
def log_data_to_google_sheets(data):
    try:
        sheets = authenticate_google_sheets()
        sheet_data = sheets.values().get(spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!A1:H1").execute()

        headers = ['Auction Time', 'CaseNumber', 'Final Judgement', 'ParcelID', 'Site Address', 'City & Zipcode', 'Parcel URL']
        if not sheet_data.get('values'):
            sheets.values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!A1:H1",
                valueInputOption="RAW",
                body={"values": [headers]}
            ).execute()

        all_data = sheets.values().get(spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!A:A").execute()
        last_row = len(all_data.get("values", [])) + 1

        sheets.values().update(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A{last_row}:H{last_row + len(data) - 1}",
            valueInputOption="RAW",
            body={"values": data}
        ).execute()
        print("Data successfully logged to Google Sheets.")
    except Exception as e:
        print(f"Error logging data to Google Sheets: {e}")

# Main execution
if __name__ == "__main__":
    urls = [
        "https://dallas.texas.sheriffsaleauctions.com/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE=02/03/2026"
    ]
    for url in urls:
        print(f"Processing URL: {url}")
        data = scrape_data(url)
        if data:
            log_data_to_google_sheets(data)
        else:
            print("No data extracted.")
