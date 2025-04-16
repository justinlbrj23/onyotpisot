import asyncio
import os
import re
import string
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# === Config ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_PATH = os.path.join(BASE_DIR, "credentials.json")
TOKEN_PATH = os.path.join(BASE_DIR, "token.json")
SHEET_ID = "1VUB2NdGSY0l3tuQAfkz8QV2XZpOj2khCB69r5zU1E5A"
SHEET_NAME = "CAPE CORAL FINAL"
URL_RANGE = "R15:R"
MAX_RETRIES = 3

# === Google Sheets Auth ===
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
        result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=f"{SHEET_NAME}!{range_name}").execute()
        values = result.get("values", [])
        return [row[0] for row in values if row and row[0]]
    except Exception as e:
        print(f"Error fetching data from Google Sheets: {e}")
        return []

# === Web Scraping with Retry ===
async def fetch_truepeoplesearch_data(url):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                )
                page = await context.new_page()
                print(f" Attempt {attempt} to fetch: {url}")
                await page.goto(url, wait_until="load", timeout=15000)
                await page.wait_for_timeout(5000)
                content = await page.content()
                await browser.close()

                if "captcha" in content.lower():
                    print(f" CAPTCHA detected on attempt {attempt}")
                    continue

                return content

        except PlaywrightTimeout as e:
            print(f"⏱ Timeout on attempt {attempt}: {e}")
        except Exception as e:
            print(f" Error on attempt {attempt}: {e}")

    print(f"⚠ Failed to fetch valid content after {MAX_RETRIES} attempts for {url}")
    return ""

# === HTML Parsing ===
def extract_links(html):
    soup = BeautifulSoup(html, "html.parser")
    people_data = []

    # Debugging: Print sample HTML content
    print("HTML Preview:", html[:1000])  # Check the first 1000 characters

    for link in soup.find_all("a", href=re.compile(r"^/find/person/")):
        href = f"https://www.truepeoplesearch.com{link['href']}"
        web_text = link.get_text(separator=" ", strip=True)
        people_data.append({"href": href, "text": web_text})

    if not people_data:
        print(" No links found in extracted HTML! Check site structure.")

    return people_data

# === Sheet Update Logic ===
def get_column_letter(index):
    letters = string.ascii_uppercase
    return letters[index] if index < 26 else letters[(index // 26) - 1] + letters[index % 26]

def update_sheet_data(sheet_id, row_index, data):
    try:
        service = authenticate_google_sheets()
        sheet = service.spreadsheets()
        for i, item in enumerate(data):
            href_col = get_column_letter(19 + 2 * i)
            text_col = get_column_letter(19 + 2 * i + 1)
            sheet.values().update(
                spreadsheetId=sheet_id,
                range=f"{SHEET_NAME}!{href_col}{row_index}",
                valueInputOption="RAW",
                body={"values": [[item['href']]]}
            ).execute()
            sheet.values().update(
                spreadsheetId=sheet_id,
                range=f"{SHEET_NAME}!{text_col}{row_index}",
                valueInputOption="RAW",
                body={"values": [[item['text'].strip()]]}
            ).execute()
    except Exception as e:
        print(f"Error updating Google Sheet: {e}")

# === Debugging Single URL ===
async def debug_single_url():
    test_url = "https://www.truepeoplesearch.com/find/address/2437-SW-PINE-ISLAND-RD_33991"
    html = await fetch_truepeoplesearch_data(test_url)
    print(html[:1000])  # Print first 1000 characters for inspection
    results = extract_links(html)
    for entry in results:
        print(f"{entry['href']}\n→ {entry['text']}\n")

# === Main Execution ===
async def main():
    urls = get_sheet_data(SHEET_ID, URL_RANGE)

    if not urls:
        print(" No URLs fetched from Google Sheets!")
        return

    for idx, url in enumerate(urls, start=15):  # Assuming row 7 starts the list
        print(f"\n Processing Row {idx}: {url}")
        html = await fetch_truepeoplesearch_data(url)
        if not html:
            print(f"No content fetched for {url}")
            continue
        results = extract_links(html)

        for entry in results:
            print(f"{entry['href']}\n→ {entry['text']}\n")

        # Optional: uncomment to update Google Sheet
        # update_sheet_data(SHEET_ID, idx, results)

if __name__ == "__main__":
    asyncio.run(main())

# Uncomment below to debug a single URL
# asyncio.run(debug_single_url())
