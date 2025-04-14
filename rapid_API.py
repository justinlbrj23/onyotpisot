import time
import requests
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Setup
SHEET_ID = "1VUB2NdGSY0l3tuQAfkz8QV2XZpOj2khCB69r5zU1E5A"
SHEET_NAME = "CAPE CORAL FINAL"
RANGE_START_ROW = 2
URL_COLUMN = 18  # Column R
OUTPUT_START_COL = 20  # Column T
MAX_LINKS = 20

# Auth with Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)

# Read all URLs from column R
urls = sheet.col_values(URL_COLUMN)[1:]  # Skip header

def extract_links_from_url(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
                      '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')

        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if "www.truepeoplesearch.com/find/person/" in href:
                links.append(href)

        return list(dict.fromkeys(links))[:MAX_LINKS]  # Remove duplicates and limit
    except Exception as e:
        print(f"Error with {url}: {e}")
        return []

# Iterate and process
for i, url in enumerate(urls):
    if not url.strip():
        continue

    links = extract_links_from_url(url)
    padded_links = links + [''] * (MAX_LINKS - len(links))
    
    cell_list = sheet.range(RANGE_START_ROW + i, OUTPUT_START_COL, RANGE_START_ROW + i, OUTPUT_START_COL + MAX_LINKS - 1)
    for j, cell in enumerate(cell_list):
        cell.value = padded_links[j]
    sheet.update_cells(cell_list)

    time.sleep(1)  # Pause to avoid rate-limiting