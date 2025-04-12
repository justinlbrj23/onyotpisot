import os
import asyncio
from pyppeteer import launch
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from pyppeteer.errors import TimeoutError, NetworkError

# Google Sheets setup
SHEET_ID = '1VUB2NdGSY0l3tuQAfkz8QV2XZpOj2khCB69r5zU1E5A'
SHEET_NAME = 'PALM BAY FINAL'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CREDENTIALS_PATH = os.path.join(os.getcwd(), 'credentials.json')
TOKEN_PATH = os.path.join(os.getcwd(), 'token.json')

# Authenticate Google Sheets
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
        with open(TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())
    return creds

# Generate column names dynamically
def get_column_name(index):
    column_name = ""
    while index >= 0:
        column_name = chr(index % 26 + 65) + column_name
        index = index // 26 - 1
    return column_name

# Ensure sheet has enough columns
def expand_sheet_columns_if_needed(credentials, required_column_index):
    service = build('sheets', 'v4', credentials=credentials)
    spreadsheet = service.spreadsheets().get(spreadsheetId=SHEET_ID).execute()

    for sheet in spreadsheet['sheets']:
        if sheet['properties']['title'] == SHEET_NAME:
            current_cols = sheet['properties']['gridProperties']['columnCount']
            if required_column_index + 1 > current_cols:
                requests = [{
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet['properties']['sheetId'],
                            "gridProperties": {
                                "columnCount": required_column_index + 1
                            }
                        },
                        "fields": "gridProperties.columnCount"
                    }
                }]
                service.spreadsheets().batchUpdate(spreadsheetId=SHEET_ID, body={"requests": requests}).execute()
                print(f"Expanded columns to {required_column_index + 1}")
            break

# Batch update Google Sheets
def batch_update_sheet(credentials, data, row_number):
    sheets = build('sheets', 'v4', credentials=credentials)
    body = {'valueInputOption': 'RAW', 'data': []}

    for j, val in enumerate(data):
        col_index = 19 + j  # Start from column S (index 19)
        column_name = get_column_name(col_index)
        expand_sheet_columns_if_needed(credentials, col_index)
        range_str = f'{SHEET_NAME}!{column_name}{row_number}'
        print(f"Updating range: {range_str} with value: {val}")

        body['data'].append({
            'range': range_str,
            'values': [[val]]
        })

    if not body['data']:
        print("No valid data to update.")
        return

    try:
        sheets.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET_ID, body=body
        ).execute()
        print("Sheet update successful!")
    except Exception as e:
        print(f"Error updating sheet: {e}")
        raise e

# Pyppeteer setup with enhanced error handling
async def fetch_page_html(url, retries=3, delay=5):
    browser = await launch(
        headless=True,
        executablePath=r'C:\Program Files\Google\Chrome\Application\chrome.exe'
    )
    page = await browser.newPage()
    await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36")

    try:
        # Retry mechanism for fetching page
        attempt = 0
        while attempt < retries:
            try:
                print(f"Attempting to load {url} (Attempt {attempt + 1}/{retries})")
                await page.goto(url, {'waitUntil': 'networkidle2', 'timeout': 60000})
                break  # Exit retry loop if successful
            except asyncio.TimeoutError:
                print(f"Timeout while fetching {url}, retrying...")
                await asyncio.sleep(delay)
            except asyncio.pyppeteer.errors.NetworkError as e:
                print(f"Network error while fetching {url}: {e}, retrying...")
                await asyncio.sleep(delay)
                attempt += 1
                if attempt == retries:
                    print(f"Failed to load {url} after {retries} attempts.")
                    raise e
        return page, browser
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        await browser.close()
        return None, None

# Ensure that browser is properly closed in case of errors
async def extract_data(page, class_name):
    try:
        elements = await page.xpath(f'//div[contains(@class, "{class_name}")]')
        if not elements:
            print(f"No elements found for class '{class_name}'.")
            return []

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
        print(f"Error extracting data: {e}")
        return []
    
# Simulate human-like mouse movement and click on an input element
async def simulate_mouse_click(page, input_xpath):
    try:
        # Wait for the main content to load
        await page.waitForXPath('//*[@id="content"]', timeout=60000)

        # Wait until the input element is available and visible
        input_element = await page.waitForXPath(input_xpath, timeout=60000)
        is_visible = await page.evaluate('(el) => el.offsetParent !== null', input_element)

        if not is_visible:
            print(f"Element found but not visible: {input_xpath}")
            return

        # Wait until the input element is enabled (clickable)
        await page.waitForFunction(
            '''(xpath) => {
                const element = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
                return element && !element.disabled && element.offsetParent !== null;
            }''',
            {'timeout': 60000},
            input_xpath
        )

        # Get bounding box to calculate coordinates for mouse movement
        box = await input_element.boundingBox()
        if box:
            x = box['x'] + box['width'] / 2
            y = box['y'] + box['height'] / 2

            # Simulate human-like mouse movement using small incremental steps
            await page.evaluate('''
                (x, y) => {
                    const moveMouse = (x, y) => {
                        const event = new MouseEvent('mousemove', {
                            view: window,
                            bubbles: true,
                            cancelable: true,
                            clientX: x,
                            clientY: y
                        });
                        document.dispatchEvent(event);
                    };

                    for (let i = 0; i <= 10; i++) {
                        setTimeout(() => moveMouse(x + i, y + i), i * 20);
                    }
                }
            ''', x, y)

        # Perform the click
        await input_element.click()
        print(f"Successfully clicked on element: {input_xpath}")

    except Exception as e:
        print(f"Error during mouse simulation or click: {e}")

# Main function
async def main():
    credentials = authenticate_google_sheets()
    sheets = build('sheets', 'v4', credentials=credentials)

    range_ = f"{SHEET_NAME}!R2:R"  # URLs from column R
    try:
        result = sheets.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=range_).execute()
        urls = [val[0] for val in result.get('values', []) if val and val[0]]
    except Exception as e:
        print(f"Error fetching URLs: {e}")
        return

    for index, url in enumerate(urls, start=2):
        print(f"\nFetching data for URL: {url}")
        page, browser = await fetch_page_html(url)

        if page:
            class_name = 'card card-body shadow-form pt-3'

            # Check if any elements exist before clicking
            elements = await page.querySelectorAll(f'.{class_name.replace(" ", ".")}')
            if not elements:
                print(f"No elements found for class '{class_name}', attempting simulated click...")
                input_xpath = '//*[@id="aPYp3"]/div/label/input'
                await simulate_mouse_click(page, input_xpath)

            # Re-check after clicking (optional retry after triggering element)
            extracted_data = await extract_data(page, class_name)

            if extracted_data:
                print(f"Data extracted for {url}: {extracted_data}")
                row_data = []
                for item in extracted_data:
                    row_data.append(item['href'])
                    row_data.append(item['text'])
                try:
                    batch_update_sheet(credentials, row_data, index)
                except Exception as e:
                    print(f"Error while updating Google Sheets for {url}: {e}")
            else:
                print(f"No data extracted for {url}")
            await browser.close()
        else:
            print(f"Failed to fetch page for {url}")

asyncio.run(main())
