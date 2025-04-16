# main.py

import asyncio
import os
import re
import string
import json
import sys

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from playwright_stealth import stealth_async
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

sys.stdout.reconfigure(encoding='utf-8')

# === Config ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SHEET_ID = "1VUB2NdGSY0l3tuQAfkz8QV2XZpOj2khCB69r5zU1E5A"
SHEET_NAME = "CAPE CORAL FINAL"
URL_RANGE = "R3:R"
MAX_RETRIES = 3

# === Google Sheets Auth ===
def authenticate_google_sheets():
    SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not SERVICE_ACCOUNT_JSON:
        raise ValueError("Missing GOOGLE_SERVICE_ACCOUNT_JSON environment variable")

    service_account_info = json.loads(SERVICE_ACCOUNT_JSON)
    creds = Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)

def get_sheet_data(sheet_id, range_name):
    try:
        service = authenticate_google_sheets()
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"{SHEET_NAME}!{range_name}"
        ).execute()
        values = result.get("values", [])
        return [row[0] for row in values if row and row[0]]
    except Exception as e:
        print(f"Error fetching data from Google Sheets: {e}")
        return []

# === Web Scraping with Playwright + Stealth ===
async def fetch_truepeoplesearch_data(url):
    stealth_js = """
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
    Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
    window.chrome = { runtime: {} };
    """

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
                )
                await context.add_init_script(stealth_js)
                page = await context.new_page()
                await stealth_async(page)

                print(f" Attempt {attempt} to fetch: {url}")
                await page.goto(url, wait_until="networkidle", timeout=20000)
                await page.mouse.move(300, 400)
                await page.mouse.wheel(0, 600)
                await page.wait_for_timeout(3000)
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

    print(f"Failed to fetch valid content after {MAX_RETRIES} attempts for {url}")
    return ""

# === HTML Parsing ===
def extract_links(html):
    soup = BeautifulSoup(html, "html.parser")
    people_data = []
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
            href_col = get_column_letter(19 + 3 * i)
            text_col = get_column_letter(19 + 3 * i + 1)
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

# === Main Execution ===
async def main():
    urls = get_sheet_data(SHEET_ID, URL_RANGE)
    if not urls:
        print(" No URLs fetched from Google Sheets!")
        return

    for idx, url in enumerate(urls, start=3):
        print(f"\n Processing Row {idx}: {url}")
        html = await fetch_truepeoplesearch_data(url)
        if not html:
            print(f"No content fetched for {url}")
            continue
        results = extract_links(html)

        for entry in results:
            print(f"{entry['href']}\n→ {entry['text']}\n")

        # Optional: update_sheet_data(SHEET_ID, idx, results)

if __name__ == "__main__":
    asyncio.run(main())
