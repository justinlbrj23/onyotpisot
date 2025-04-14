import asyncio
import os
import json
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from pyppeteer import launch, errors
import string
import shutil
import tempfile

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
        
        print(f" Raw API Response: {json.dumps(result, indent=2)}")  # Debugging

        values = result.get("values", [])
        if not values:
            print(f" No data found in the range: {range_name}")
            return []

        return [row[0] for row in values if row]  # Flattening list
    except Exception as e:
        print(f" Error fetching data from Google Sheets: {e}")
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

            print(f" Updating range: {href_update_range} with data: {href_body}")  # Debugging print
            print(f" Updating range: {text_update_range} with data: {text_body}")  # Debugging print

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

        print(f" Successfully updated sheet at row {row_index}")
    except Exception as e:
        print(f" Error updating Google Sheet: {e}")
        
# Clean up temporary user data directory
def cleanup_tmp_user_data_dir(browser):
    """Clean up the temporary user data directory used by Pyppeteer."""
    try:
        tmp_user_data_dir = browser._launcher._tmp_user_data_dir
        if os.path.exists(tmp_user_data_dir):
            shutil.rmtree(tmp_user_data_dir)
            print(f" Successfully removed temporary user data directory: {tmp_user_data_dir}")
    except Exception as e:
        print(f" Error cleaning up temporary user data directory: {e}")

async def fetch_page_html(url, retry_count=3):
    """Launch a headless browser and fetch the page HTML safely."""
    for attempt in range(retry_count):
        temp_dir = tempfile.mkdtemp()
        browser = None
        page = None

        try:
            print(f" Attempt {attempt + 1}: Launching browser...")

            browser = await launch(
                headless=True,
                userDataDir=temp_dir,
                executablePath=r'C:\Program Files\Google\Chrome\Application\chrome.exe',  # Adjust if needed
                args=['--no-sandbox', '--disable-setuid-sandbox']  # More stable across systems
            )
            page = await browser.newPage()

            await page.setUserAgent(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
            )

            await page.goto(url, {
                'waitUntil': 'domcontentloaded',  # More stable than 'networkidle2'
                'timeout': 30000
            })
            print(f" Successfully fetched page: {url}")
            return page, browser

        except (asyncio.TimeoutError, errors.NetworkError) as e:
            print(f" Network error fetching {url}: {e}")
        except Exception as e:
            print(f" Unexpected error during browser launch or navigation: {e}")

        finally:
            # If we fail and will retry, close resources and clean up
            if browser:
                try:
                    if page:
                        await page.close()
                    await browser.close()
                    await asyncio.sleep(1)  # Ensure Chrome fully shuts down
                except Exception as cleanup_error:
                    print(f" Error during cleanup: {cleanup_error}")
            if os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                    print(f" Cleaned up temp dir: {temp_dir}")
                except Exception as cleanup_error:
                    print(f" Failed to remove temp dir: {cleanup_error}")

    print(f" All {retry_count} attempts failed for URL: {url}")
    return None, None

# Extract content using XPath
async def extract_hrefs_and_span_h4_within_class(page, class_name):
    try:
        elements = await page.xpath(f'//div[contains(@class, "{class_name}")]')
        extracted_data = []

        for element in elements:
            href_elements = await element.xpath('.//a[@href]')
            hrefs = [await page.evaluate('(element) => element.href', el) for el in href_elements]

            span_h4_elements = await element.xpath('.//span[contains(@class, "h4")]')
            span_h4_texts = [await page.evaluate('(element) => element.textContent', el) for el in span_h4_elements]

            for href, text in zip(hrefs, span_h4_texts):
                extracted_data.append({'href': href, 'text': text})

        if not extracted_data:
            print(f" No data found within elements with class '{class_name}'.")

        return extracted_data
    except (asyncio.TimeoutError, errors.NetworkError) as e:
        print(f" Error extracting content: {e}")
        return []
    except Exception as e:
        print(f" Unexpected error: {e}")
        return []

# Main Function
async def main():
    """Main function to fetch URLs, scrape content, and log results sequentially."""
    urls = get_sheet_data(SHEET_ID, "CAPE CORAL FINAL!R2:R")

    if not urls:
        print("No URLs found in the spreadsheet.")
        return

    for idx, url in enumerate(urls, start=2):  # Start from row 39
        print(f"\nFetching: {url}")
        page, browser = await fetch_page_html(url)

        if page:
            print("Page fetched successfully!")
            class_name = 'card card-body shadow-form pt-3'
            extracted_data = await extract_hrefs_and_span_h4_within_class(page, class_name)

            if extracted_data:
                print(f" Extracted data: {extracted_data}")
                update_sheet_data(SHEET_ID, idx, extracted_data)
            else:
                print(" No data extracted from the page.")
            
            try:
                await page.close()
                await browser.close()
            except Exception as cleanup_error:
                print(f"Error during cleanup: {cleanup_error}")
        else:
            print("Failed to fetch the page.")

# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
