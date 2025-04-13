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

# Prevent Pyppeteer from downloading Chromium
os.environ["PYPPETEER_SKIP_CHROMIUM_DOWNLOAD"] = "true"

# Ensure UTF-8 encoding for output
sys.stdout.reconfigure(encoding='utf-8')

# Apply patch for async loop reuse
nest_asyncio.apply()

# File paths and constants
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_PATH = os.path.join(BASE_DIR, "credentials.json")
TOKEN_PATH = os.path.join(BASE_DIR, "token.json")
SHEET_ID = "1VUB2NdGSY0l3tuQAfkz8QV2XZpOj2khCB69r5zU1E5A"

# Google Sheets Authentication
def authenticate_google_sheets():
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
    try:
        service = authenticate_google_sheets()
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id, range=range_name
        ).execute()
        print(f"Raw API Response: {json.dumps(result, indent=2)}")
        values = result.get("values", [])
        return [row[0] for row in values if row]
    except Exception as e:
        print(f"Error fetching data from Google Sheets: {e}")
        return []

def get_column_letter(index):
    letters = string.ascii_uppercase
    return letters[index] if index < 26 else letters[(index // 26) - 1] + letters[index % 26]

def update_sheet_data(sheet_id, row_index, data):
    try:
        service = authenticate_google_sheets()
        sheet = service.spreadsheets()
        for i, item in enumerate(data):
            href_col = get_column_letter(26 + 2 * i)
            text_col = get_column_letter(26 + 2 * i + 1)

            href_range = f"'Raw Cape Coral - ArcGIS (lands)'!{href_col}{row_index}"
            text_range = f"'Raw Cape Coral - ArcGIS (lands)'!{text_col}{row_index}"

            href_body = {"values": [[item['href']]]}
            text_body = {"values": [[item['text'].strip()]]}

            print(f"Updating range: {href_range} with {href_body}")
            print(f"Updating range: {text_range} with {text_body}")

            sheet.values().update(
                spreadsheetId=sheet_id,
                range=href_range,
                valueInputOption="RAW",
                body=href_body
            ).execute()

            sheet.values().update(
                spreadsheetId=sheet_id,
                range=text_range,
                valueInputOption="RAW",
                body=text_body
            ).execute()

            time.sleep(1)  # Prevent hitting API rate limits
        print(f"✅ Sheet updated for row {row_index}")
    except Exception as e:
        print(f"Error updating Google Sheet: {e}")

async def fetch_page_html(url, retry_count=3):
    for attempt in range(retry_count):
        temp_dir = tempfile.mkdtemp()
        try:
            print(f"Attempt {attempt + 1}: Launching browser...")

            chrome_path = os.environ.get("CHROME_PATH", "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe")

            browser = await launch(
                headless=True,
                userDataDir=temp_dir,
                executablePath=chrome_path,
                args=["--no-sandbox", "--disable-setuid-sandbox"]
            )
            page = await browser.newPage()
            await page.setUserAgent(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
            )
            await page.goto(url, {'waitUntil': 'networkidle2', 'timeout': 60000})
            print(f"✅ Successfully fetched page: {url}")
            return page, browser
        except Exception as e:
            print(f"❌ Error fetching {url} on attempt {attempt + 1}: {e}")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
    return None, None

async def extract_hrefs_and_span_h4_within_class(page, class_name):
    try:
        elements = await page.xpath(f'//div[contains(@class, "{class_name}")]')
        extracted_data = []
        for element in elements:
            hrefs = [await page.evaluate('(el) => el.href', el)
                     for el in await element.xpath('.//a[@href]')]
            texts = [await page.evaluate('(el) => el.textContent', el)
                     for el in await element.xpath('.//span[contains(@class, "h4")]')]
            for href, text in zip(hrefs, texts):
                extracted_data.append({'href': href, 'text': text.strip()})
        return extracted_data
    except Exception as e:
        print(f"❌ Error extracting content: {e}")
        return []

async def main():
    urls = get_sheet_data(SHEET_ID, "Raw Cape Coral - ArcGIS (lands)!Y2:Y")
    if not urls:
        print("⚠️ No URLs found in the spreadsheet.")
        return
    for idx, url in enumerate(urls, start=2):
        if not url.strip():
            print(f"⏭️ Skipping empty URL at row {idx}")
            continue
        print(f"\n🌐 Fetching: {url}")
        page, browser = await fetch_page_html(url)
        if page:
            class_name = 'card card-body shadow-form pt-3'
            extracted_data = await extract_hrefs_and_span_h4_within_class(page, class_name)
            if extracted_data:
                print(f"📦 Extracted data: {extracted_data}")
                update_sheet_data(SHEET_ID, idx, extracted_data)
            else:
                print(f"❌ No data extracted from: {url}")
            await page.close()
            await browser.close()
    print("\n✅ All URLs processed. Workflow complete.")

if __name__ == "__main__":
    asyncio.run(main())
