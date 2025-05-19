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
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")
if not SERVICE_ACCOUNT_JSON:
    raise ValueError("SERVICE_ACCOUNT_JSON not found in environment variables.")

# Decode the base64 JSON string into a credentials dict
credentials_dict = json.loads(SERVICE_ACCOUNT_JSON)
creds = Credentials.from_service_account_info(credentials_dict)
sheets_service = build('sheets', 'v4', credentials=creds)

sys.stdout.reconfigure(encoding='utf-8')

# === Config ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SHEET_ID = "1VUB2NdGSY0l3tuQAfkz8QV2XZpOj2khCB69r5zU1E5A"
SHEET_NAME = "CAPE CORAL FINAL"
URL_RANGE = "R2:R"
MAX_RETRIES = 3

# === Google Sheets Interaction ===

def authenticate_google_sheets():
    """Return a built Sheets API service."""
    return sheets_service

def get_sheet_data(sheet_id, range_name):
    try:
        service = authenticate_google_sheets()
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"{SHEET_NAME}!{range_name}"
        ).execute()
        values = result.get("values", [])
        base_row = int(re.search(r"(\d+):\w*", range_name).group(1))

        return [
            (i + base_row, row[0].strip())
            for i, row in enumerate(values)
            if row and len(row) > 0 and row[0].strip()
        ]
    except Exception as e:
        print(f"Error fetching data from Google Sheets: {e}")
        return []

def update_sheet_data(sheet_id, row_index, values):
    from string import ascii_uppercase

    start_col_index = ascii_uppercase.index('T')
    end_col_index = start_col_index + len(values) - 1
    start_col_letter = ascii_uppercase[start_col_index]
    end_col_letter = ascii_uppercase[end_col_index] if end_col_index < len(ascii_uppercase) else 'AM'

    target_range = f"{SHEET_NAME}!{start_col_letter}{row_index + 1}:{end_col_letter}{row_index + 1}"

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

def fetch_data_browserless_graphql(url: str):
    print(f"[~] Using Browserless GraphQL fallback for: {url}")
    endpoint = "https://production-sfo.browserless.io/chrome/bql"
    query_string = {
        "token": BROWSERLESS_TOKEN,
        "proxy": "residential",
        "proxySticky": "true",
        "proxyCountry": "us",
        "humanlike": "true",
        "blockConsentModals": "true",
    }
    headers = {
        "Content-Type": "application/json",
    }

    graphql_query = f"""
    mutation Verify {{
      goto(url: "{url}") {{
        status
      }}
      verify(type: cloudflare) {{
        found
        solved
        time
      }}
      results: elements(selector: "a") {{
        text
        attributes {{
          name
          value
        }}
      }}
    }}
    """

    payload = {
        "query": graphql_query,
        "operationName": "Verify",
    }

    try:
        response = requests.post(endpoint, params=query_string, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        links = []

        if "data" in data and "results" in data["data"]:
            for el in data["data"]["results"]:
                href = next((attr["value"] for attr in el.get("attributes", []) if attr["name"] == "href"), None)
                text = el.get("text", "").strip()
                if href and "www.truepeoplesearch.com/find/person/" in href:
                    links.append(f"{text}\n{href}")

        return links
    except Exception as e:
        print(f"[!] GraphQL fallback failed for {url}: {e}")
        return []

BROWSERLESS_TOKEN = "S9HuggAnITqZTnb98093b0645f2b9ea2d49fcc49e6"

async def fetch_truepeoplesearch_data(url):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with async_playwright() as p:
                selected_agent = random.choice(user_agents)
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(user_agent=selected_agent)
                await context.add_init_script(stealth_js)

                page = await context.new_page()
                await stealth_async(page)

                print(f" Attempt {attempt} to fetch: {url}")
                await page.goto(url, wait_until="networkidle", timeout=30000)

                await page.wait_for_timeout(random.randint(3000, 5000))
                await page.mouse.move(random.randint(100, 400), random.randint(100, 400), steps=20)
                await page.mouse.wheel(0, random.randint(400, 800))
                await page.wait_for_timeout(random.randint(3000, 5000))

                content = await page.content()
                await browser.close()

                if "captcha" in content.lower() or "are you a human" in content.lower():
                    print(f" CAPTCHA detected on attempt {attempt}")
                    continue

                return content

        except PlaywrightTimeout as e:
            print(f" Timeout on attempt {attempt}: {e}")
        except Exception as e:
            print(f" Error on attempt {attempt}: {e}")

    print(f" Falling back to Browserless GraphQL for: {url}")
    return fetch_data_browserless_graphql(url)

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

        # Try Playwright-based fetch first
        html = await fetch_truepeoplesearch_data(url)

        # Fallback to GraphQL if Playwright fails
        if not html:
            print(f"[!] Playwright failed or CAPTCHA detected. Trying Browserless GraphQL for row {row_index}")
            links = fetch_data_browserless_graphql(url)

            if not links:
                print(f"[!] Browserless GraphQL also failed for row {row_index}")
                continue  # Skip to the next row

            extracted = links  # Already structured from GraphQL response
        else:
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
