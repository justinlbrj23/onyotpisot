import asyncio
import os
import json
import sys
import string
import shutil
import tempfile
import time
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from pyppeteer import launch, errors
import nest_asyncio

os.environ["PYPPETEER_SKIP_CHROMIUM_DOWNLOAD"] = "true"

# Ensure UTF-8 encoding for output
sys.stdout.reconfigure(encoding='utf-8')

# Apply nest_asyncio to avoid event loop issues in certain environments
nest_asyncio.apply()

# Define file paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_PATH = os.path.join(BASE_DIR, "credentials.json")
TOKEN_PATH = os.path.join(BASE_DIR, "token.json")
SHEET_ID = "1VUB2NdGSY0l3tuQAfkz8QV2XZpOj2khCB69r5zU1E5A"

# Authenticate Google Sheets API
def authenticate_google_sheets():
    """Authenticate with Google Sheets API."""
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(
            TOKEN_PATH, ["https://www.googleapis.com/auth/spreadsheets"]
        )
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_PATH, ["https://www.googleapis.com/auth/spreadsheets"]
            )
            creds = flow.run_local_server(port=53221)
            with open(TOKEN_PATH, "w") as token:
                token.write(creds.to_json())
    return build("sheets", "v4", credentials=creds)

def get_sheet_data(sheet_id, range_name):
    """Fetch data from a Google Sheets range."""
    try:
        service = authenticate_google_sheets()
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=sheet_id, range=range_name).execute()
        
        # Debugging: Print raw API response
        print(f"Raw API Response: {json.dumps(result, indent=2)}")

        values = result.get("values", [])
        if not values:
            print(f"No data found in the range: {range_name}")
            return []

        return [row[0] for row in values if row]  # Flattening list
    except Exception as e:
        print(f"Error fetching data from Google Sheets: {e}")
        return []

def get_column_letter(index):
    """Convert column index (starting at 0) to an Excel column (AA, AB, etc.)."""
    letters = string.ascii_uppercase
    if index < 26:
        return letters[index]  # A-Z
    else:
        return letters[(index // 26) - 1] + letters[index % 26]  # AA, AB, AC...

def update_sheet_data(sheet_id, row_index, data):
    """Update the Google Sheet with extracted data starting from column AD."""
    try:
        service = authenticate_google_sheets()
        sheet = service.spreadsheets()

        for i, item in enumerate(data):
            href_column_letter = get_column_letter(26 + 2 * i)  # Start at AA (index 26), with two columns per entry
            text_column_letter = get_column_letter(26 + 2 * i + 1)  # Next column

            href_update_range = f"'Raw Cape Coral - ArcGIS (lands)'!{href_column_letter}{row_index}"  # Fixed range format
            text_update_range = f"'Raw Cape Coral - ArcGIS (lands)'!{text_column_letter}{row_index}"  # Fixed range format

            href_body = {"values": [[item['href']]]}  # Ensure values are properly structured
            text_body = {"values": [[item['text'].strip()]]}  # Ensure values are properly structured

            print(f"Updating range: {href_update_range} with data: {href_body}")  # Debugging print
            print(f"Updating range: {text_update_range} with data: {text_body}")  # Debugging print

            sheet.values().update(
                spreadsheetId=sheet_id,
                range=href_update_range,
                valueInputOption="RAW",
                body=href_body
            ).execute()
            sheet.values().update(
                spreadsheetId=sheet_id,
                range=text_update_range,
                valueInputOption="RAW",
                body=text_body
            ).execute()

            time.sleep(1)  # Add a 1-second delay between each update to prevent hitting the API limit

        print(f"Successfully updated sheet at row {row_index}")
    except Exception as e:
        print(f"Error updating Google Sheet: {e}")

async def fetch_page_html(url, retry_count=3):
    """Launch a headless browser and fetch the page HTML."""
    for attempt in range(retry_count):
        temp_dir = tempfile.mkdtemp()
        try:
            print(f"Attempt {attempt + 1}: Launching browser...")
            browser = await launch(headless=True, userDataDir=temp_dir)
            page = await browser.newPage()
            await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36")
            await page.goto(url, {'waitUntil': 'networkidle2', 'timeout': 60000})
            print(f"Successfully fetched page: {url}")
            return page, browser
        except Exception as e:
            print(f"Error fetching {url} on attempt {attempt + 1}: {e}")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
    return None, None

async def extract_hrefs_and_span_h4_within_class(page, class_name):
    """Extract hrefs and text within elements of a specific class."""
    try:
        elements = await page.xpath(f'//div[contains(@class, "{class_name}")]')
        extracted_data = []
        for element in elements:
            href_elements = await element.xpath('.//a[@href]')
            hrefs = [await page.evaluate('(element) => element.href', el) for el in href_elements]
            span_h4_elements = await element.xpath('.//span[contains(@class, "h4")]')
            span_h4_texts = [await page.evaluate('(element) => element.textContent', el) for el in span_h4_elements]
            for href, text in zip(hrefs, span_h4_texts):
                extracted_data.append({'href': href, 'text': text.strip()})
        return extracted_data
    except Exception as e:
        print(f"Error extracting content: {e}")
        return []

async def main():
    """Main function to fetch URLs, scrape content, and log results sequentially."""
    urls = get_sheet_data(SHEET_ID, "Raw Cape Coral - ArcGIS (lands)!Y2:Y")
    if not urls:
        print("No URLs found in the spreadsheet.")
        return
    for idx, url in enumerate(urls, start=2):
        print(f"\nFetching: {url}")
        page, browser = await fetch_page_html(url)
        if page:
            class_name = 'card card-body shadow-form pt-3'
            extracted_data = await extract_hrefs_and_span_h4_within_class(page, class_name)
            if extracted_data:
                print(f"Extracted data: {extracted_data}")
                update_sheet_data(SHEET_ID, idx, extracted_data)
            else:
                print(f"No data extracted from the page at {url}.")
            await page.close()
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
