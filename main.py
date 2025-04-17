import asyncio
import os
import re
import string
import json
import sys
import random
import time
from datetime import datetime

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from playwright_stealth import stealth_async
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
creds = Credentials.from_authorized_user_file('token.json', SCOPES)
sheets_service = build('sheets', 'v4', credentials=creds)

sys.stdout.reconfigure(encoding='utf-8')

def load_auth_state():
    auth_state_file = 'auth_state.json'
    if os.path.exists(auth_state_file):
        with open(auth_state_file, 'r') as file:
            auth_state = json.load(file)
            return auth_state
    else:
        print("auth_state.json not found!")
        return None

# Example of how to use the loaded auth state
auth_state = load_auth_state()
if auth_state:
    print(f"Loaded auth state: {auth_state}")
else:
    print("No auth state found.")


# === Config ===
# Define file paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_PATH = os.path.join(BASE_DIR, "credentials.json")
TOKEN_PATH = os.path.join(BASE_DIR, "token.json")
SHEET_ID = "1VUB2NdGSY0l3tuQAfkz8QV2XZpOj2khCB69r5zU1E5A"
SHEET_NAME = "CAPE CORAL FINAL"
URL_RANGE = "R2:R"
MAX_RETRIES = 3

# === Google Sheets Auth ===
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
    return build('sheets', 'v4', credentials=creds)

def get_sheet_data(sheet_id, range_name):
    try:
        service = authenticate_google_sheets()
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"{SHEET_NAME}!{range_name}"
        ).execute()
        values = result.get("values", [])
        base_row = int(re.search(r"(\d+):\w*", range_name).group(1))

        # Keep track of actual sheet row number
        return [
            (i + base_row, row[0])
            for i, row in enumerate(values)
            if row and row[0]
        ]
    except Exception as e:
        print(f"Error fetching data from Google Sheets: {e}")
        return []
    
def update_sheet_data(sheet_id, row_index, values):
    from string import ascii_uppercase

    # We'll write starting from column 'T'
    start_col_index = ascii_uppercase.index('T')  # 19th letter
    end_col_index = start_col_index + len(values) - 1

    # Handle column letters for target range
    start_col_letter = ascii_uppercase[start_col_index]
    end_col_letter = ascii_uppercase[end_col_index] if end_col_index < len(ascii_uppercase) else 'AM'

    target_range = f"CAPE CORAL FINAL!{start_col_letter}{row_index + 1}:{end_col_letter}{row_index + 1}"

    body = {
        "range": target_range,
        "majorDimension": "ROWS",
        "values": [values]
    }

    sheets_service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=target_range,
        valueInputOption="RAW",
        body=body
    ).execute()

user_agents = [
    # Include at least 10 varied user agents here.
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)...Chrome/120.0.0.0 Safari/537.36",
    # Add more user agents
]

stealth_js = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
window.chrome = { runtime: {} };
window.navigator.chrome = { runtime: {} };
"""

def name_tokens(name):
    return [normalize_and_sort()(part) for part in name.split()]


def normalize_text(text):
    return re.sub(r'\s+', ' ', text.strip().upper())

def normalize_and_sort(text):
    words = re.findall(r'\w+', text.upper())
    return ' '.join(sorted(words))

def is_match(entry_text, ref_names):
    normalized_entry = normalize_and_sort(entry_text)
    for ref in ref_names:
        normalized_ref = normalize_and_sort(ref)
        if normalized_ref in normalized_entry or normalized_entry in normalized_ref:
            return True
    return False

def extract_reference_names(sheet_id, row_index):
    range_ = f'CAPE CORAL FINAL!D{row_index}:J{row_index}'
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=range_
    ).execute()
    values = result.get('values', [[]])[0]
    return [normalize_text(val) for val in values if val.strip()]

def match_entries(extracted, ref_names):
    matched_results = []
    for entry in extracted:
        # Ensure 'link' and 'text' exist
        if "link" in entry and "text" in entry:
            normalized_text = normalize_and_sort(entry["text"])
            for ref in ref_names:
                normalized_ref = normalize_and_sort(ref)
                if normalized_ref in normalized_text or normalized_text in normalized_ref:
                    matched_results.append({
                        "link": entry["link"],
                        "text": entry["text"],
                        "matched_to": ref  # Add the matched reference term
                    })
    return matched_results

def log_matches_to_sheet(sheet_id, row_index, matched_results):
    values = []
    for result in matched_results:
        if "matched_to" in result:
            entry_text = result['text']
            entry_link = result['link']
            match_label = result['matched_to']
            combined_entry = f"{entry_text} (Matched: {match_label})"
            values.extend([combined_entry, entry_link])  # Each pair in two columns

    if values:
        update_sheet_data(sheet_id, row_index, values)


# === Web Scraping with Playwright + Stealth ===
async def fetch_truepeoplesearch_data(url):
    # Load the auth state from auth_state.json
    auth_state = load_auth_state()

    # If auth_state is available, extract cookies and headers for reuse
    cookies = auth_state.get("cookies", []) if auth_state else []
    headers = auth_state.get("headers", {}) if auth_state else {}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with async_playwright() as p:
                selected_agent = random.choice(user_agents)
                browser = await p.chromium.launch(headless=True)

                # Create a new browser context with the cookies and headers from auth_state.json
                context = await browser.new_context(
                    user_agent=selected_agent,
                    cookies=cookies,
                    extra_http_headers=headers
                )
                await context.add_init_script(stealth_js)

                page = await context.new_page()
                await stealth_async(page)

                print(f" Attempt {attempt} to fetch: {url}")
                await page.goto(url, wait_until="networkidle", timeout=30000)

                # Enhanced interaction to mimic human browsing behavior
                await page.wait_for_timeout(random.randint(3000, 5000))
                await page.mouse.move(random.randint(100, 400), random.randint(100, 400), steps=20)
                await page.mouse.wheel(0, random.randint(400, 800))
                await page.wait_for_timeout(random.randint(3000, 5000))

                content = await page.content()
                await browser.close()

                # Check if CAPTCHA or human verification appears on the page
                if "captcha" in content.lower() or "are you a human" in content.lower():
                    print(f" CAPTCHA detected on attempt {attempt}")
                    continue

                return content

        except PlaywrightTimeout as e:
            print(f" Timeout on attempt {attempt}: {e}")
        except Exception as e:
            print(f" Error on attempt {attempt}: {e}")

    print(f"Failed to fetch valid content after {MAX_RETRIES} attempts for {url}")
    return ""

def extract_links(html):
    soup = BeautifulSoup(html, 'html.parser')
    entries = []

    # Modify this to extract link data correctly
    for person_link in soup.find_all("a", href=re.compile(r"^/find/person/")):  # Adjust the target elements as needed
        link = f"https://www.truepeoplesearch.com{person_link['href']}"
        text = person_link.get_text(strip=True)
        entries.append({"link": link, "text": text})

    return entries


# === Sheet Update Logic ===
def get_column_letter(index):
    letters = string.ascii_uppercase
    return letters[index] if index < 26 else letters[(index // 26) - 1] + letters[index % 26]

async def main():
    url_entries = get_sheet_data(SHEET_ID, URL_RANGE)
    if not url_entries:
        print("No URLs fetched from Google Sheets!")
        return

    for row_index, url in url_entries:
        if not url.strip():
            continue

        print(f"\nProcessing Row {row_index}: {url}")
        html = await fetch_truepeoplesearch_data(url)
        if not html:
            print(f"No content fetched for {url}")
            continue

        extracted = extract_links(html)
        if not extracted:
            print("No valid links extracted.")
            continue

        ref_names = extract_reference_names(SHEET_ID, row_index)
        if not ref_names:
            print(f"No reference names in row {row_index} (cols D–J)")
            continue

        matched_results = match_entries(extracted, ref_names)

        if matched_results:
            print(f"Match found. Logging to row {row_index}")
            log_matches_to_sheet(SHEET_ID, row_index, matched_results)
        else:
            print(f"No match found in row {row_index}")

if __name__ == "__main__":
    asyncio.run(main())
