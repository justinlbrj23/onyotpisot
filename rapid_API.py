import asyncio
import os
import json
import string
import shutil
import tempfile
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from pyppeteer import launch, errors

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

# Fetch data from Google Sheets
def get_sheet_data(sheet_id, range_name):
    try:
        service = authenticate_google_sheets()
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=sheet_id, range=range_name).execute()
        values = result.get("values", [])
        return [row[0] for row in values if row]
    except Exception as e:
        print(f"Error fetching data from Google Sheets: {e}")
        return []

# Convert column index to Excel column letter
def get_column_letter(index):
    letters = string.ascii_uppercase
    if index < 26:
        return letters[index]
    else:
        return letters[(index // 26) - 1] + letters[index % 26]

# Update Google Sheets
def update_sheet_data(sheet_id, row_index, data):
    try:
        service = authenticate_google_sheets()
        sheet = service.spreadsheets()
        batch_update_body = {"valueInputOption": "RAW", "data": []}

        for i, item in enumerate(data):
            href_col = get_column_letter(26 + 2 * i)
            text_col = get_column_letter(26 + 2 * i + 1)
            batch_update_body["data"].extend([
                {"range": f"'Raw Cape Coral - ArcGIS (lands)'!{href_col}{row_index}", "values": [[item["href"]]]},
                {"range": f"'Raw Cape Coral - ArcGIS (lands)'!{text_col}{row_index}", "values": [[item["text"].strip()]]},
            ])

        sheet.values().batchUpdate(spreadsheetId=sheet_id, body=batch_update_body).execute()
        print(f"Successfully updated sheet at row {row_index}")
    except Exception as e:
        print(f"Error updating Google Sheet: {e}")

# Launch browser and fetch page
async def fetch_page_html(url, retry_count=3):
    """Launch browser and fetch page HTML."""
    for attempt in range(retry_count):
        temp_dir = tempfile.mkdtemp()
        browser = None
        try:
            print(f"Attempt {attempt + 1}: Launching browser...")
            browser = await launch(
                headless=True,
                userDataDir=temp_dir,
                executablePath=r'C:\Program Files\Google\Chrome\Application\chrome.exe'
            )
            page = await browser.newPage()
            await page.setUserAgent(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
            )
            await page.goto(url, {'waitUntil': 'networkidle2', 'timeout': 30000})
            print(f"Successfully fetched page: {url}")
            return page, browser
        except Exception as e:
            print(f"Error fetching {url}: {e}")
        finally:
            try:
                if browser:
                    await browser.close()
            except Exception as cleanup_error:
                print(f"Error closing browser: {cleanup_error}")
            try:
                shutil.rmtree(temp_dir)
            except PermissionError as perm_error:
                print(f"Temporary directory in use: {perm_error}")
    return None, None

# Extract data using XPath
async def extract_hrefs_and_span_h4_within_class(page, class_name):
    try:
        elements = await page.xpath(f'//div[contains(@class, "{class_name}")]')
        extracted_data = []
        for element in elements:
            href_elements = await element.xpath('.//a[@href]')
            hrefs = [await page.evaluate('(el) => el.href', el) for el in href_elements]
            span_h4_elements = await element.xpath('.//span[contains(@class, "h4")]')
            texts = [await page.evaluate('(el) => el.textContent', el) for el in span_h4_elements]
            extracted_data.extend({'href': h, 'text': t} for h, t in zip(hrefs, texts))
        return extracted_data
    except Exception as e:
        print(f"Error extracting content: {e}")
        return []

# Main function
async def main():
    urls = get_sheet_data(SHEET_ID, "Raw Cape Coral - ArcGIS (lands)!Y2:Y")
    if not urls:
        print("No URLs found in the spreadsheet.")
        return

    for idx, url in enumerate(urls, start=2):
        page, browser = await fetch_page_html(url)
        if page:
            try:
                class_name = 'card card-body shadow-form pt-3'
                extracted_data = await extract_hrefs_and_span_h4_within_class(page, class_name)
                if extracted_data:
                    update_sheet_data(SHEET_ID, idx, extracted_data)
                else:
                    print("No data extracted from the page.")
            finally:
                await page.close()
                await browser.close()

# Entry point
if __name__ == "__main__":
    asyncio.run(main())
