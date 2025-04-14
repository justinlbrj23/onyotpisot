import asyncio
import os
import json
import string
import shutil
import tempfile
import time
import logging
import psutil
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from pyppeteer import launch, errors

# Suppress Pyppeteer logging
logging.getLogger("pyppeteer").setLevel(logging.CRITICAL)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_PATH = os.path.join(BASE_DIR, "credentials.json")
TOKEN_PATH = os.path.join(BASE_DIR, "token.json")
SHEET_ID = "1VUB2NdGSY0l3tuQAfkz8QV2XZpOj2khCB69r5zU1E5A"

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
        result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range_name).execute()
        return [row[0] for row in result.get("values", []) if row]
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

def safe_remove_temp_dir(temp_dir):
    for _ in range(5):
        try:
            shutil.rmtree(temp_dir)
            break
        except PermissionError as e:
            print(f"Temporary directory in use: {e}")
            time.sleep(1)

def terminate_chrome_processes():
    for proc in psutil.process_iter(['name']):
        if proc.info['name'] == 'chrome.exe':
            try:
                proc.kill()
            except psutil.NoSuchProcess:
                pass

async def fetch_page_html(url, retry_count=3):
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
            return page, browser, temp_dir
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            terminate_chrome_processes()
            safe_remove_temp_dir(temp_dir)
        finally:
            if browser:
                try:
                    await browser.close()
                except Exception as browser_error:
                    print(f"Error closing browser: {browser_error}")
    return None, None, None

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

async def main():
    urls = get_sheet_data(SHEET_ID, "Raw Cape Coral - ArcGIS (lands)!Y2:Y")
    if not urls:
        print("No URLs found in the spreadsheet.")
        return

    for idx, url in enumerate(urls, start=2):
        page, browser, temp_dir = await fetch_page_html(url)
        if page:
            try:
                extracted_data = await extract_hrefs_and_span_h4_within_class(page, 'card card-body shadow-form pt-3')
                if extracted_data:
                    update_sheet_data(SHEET_ID, idx, extracted_data)
                else:
                    print("No data extracted from the page.")
            except Exception as e:
                print(f"Error during page processing: {e}")
            finally:
                try:
                    await page.close()
                except Exception as page_error:
                    print(f"Error closing page: {page_error}")
                try:
                    await browser.close()
                except Exception as browser_error:
                    print(f"Error closing browser: {browser_error}")
                terminate_chrome_processes()
                if temp_dir:
                    safe_remove_temp_dir(temp_dir)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as e:
        if "event loop is closed" in str(e).lower():
            print("Recreating event loop...")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(main())
