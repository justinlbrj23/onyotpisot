import os
import time
import pickle
import re

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


SHEET_ID = '1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA'
SHEET_NAME = 'Palm Beach - Taxdeed'

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


# ========================
# GOOGLE AUTH
# ========================

def authenticate_google_sheets():

    creds = None

    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:

        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())

        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )

            creds = flow.run_local_server(port=0)

        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)

    service = build("sheets", "v4", credentials=creds)

    return service.spreadsheets()


# ========================
# SCRAPER WITH PAGINATION
# ========================

def scrape_data(url):

    options = Options()
    options.add_argument("--start-maximized")

    driver = webdriver.Edge(options=options)

    wait = WebDriverWait(driver, 20)

    data = []

    driver.get(url)

    wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div[aid]"))
    )

    current_page = 1
    total_pages = 1


    while True:

        print(f"Scraping page {current_page}")

        wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[aid]"))
        )

        items = driver.find_elements(By.CSS_SELECTOR, "div[aid]")


        for item in items:

            def get_text(selector):

                try:
                    return item.find_element(By.CSS_SELECTOR, selector).text.strip()
                except:
                    return ""


            case_number = get_text("th:contains('Cause Number:') + td")

            assessed_value = get_text("th:contains('Adjudged Value:') + td")

            opening_bid = get_text("th:contains('Est. Min. Bid:') + td")

            parcel_id = get_text("th:contains('Account Number:') + td")

            street = get_text("th:contains('Property Address:') + td")

            city_state_zip = get_text("tr:nth-of-type(8) td")


            data.append([
                case_number,
                assessed_value,
                opening_bid,
                parcel_id,
                street,
                city_state_zip
            ])


        # detect total pages
        try:

            page_text = driver.find_element(
                By.CSS_SELECTOR,
                "span.PageText"
            ).text

            match = re.search(r'Page \d+ of (\d+)', page_text)

            if match:
                total_pages = int(match.group(1))

        except:
            pass


        print(f"Page {current_page} / {total_pages}")


        if current_page >= total_pages:

            break


        # click next
        try:

            next_button = driver.find_element(
                By.CSS_SELECTOR,
                "span.PageRight"
            )

            driver.execute_script(
                "arguments[0].click();",
                next_button
            )

            time.sleep(3)

            current_page += 1

        except:

            break


    driver.quit()

    return data


# ========================
# GOOGLE SHEETS LOG
# ========================

def log_data_to_google_sheets(data):

    sheets = authenticate_google_sheets()

    headers = [

        "Case Number",
        "Adjudged Value",
        "Opening Bid",
        "Parcel ID",
        "Street",
        "City State Zip"

    ]


    existing = sheets.values().get(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_NAME}!A1:F1"
    ).execute()


    if not existing.get("values"):

        sheets.values().update(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A1:F1",
            valueInputOption="RAW",
            body={"values": [headers]}
        ).execute()


    last = sheets.values().get(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_NAME}!A:A"
    ).execute()


    last_row = len(last.get("values", [])) + 1


    sheets.values().update(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_NAME}!A{last_row}:F{last_row + len(data)-1}",
        valueInputOption="RAW",
        body={"values": data}
    ).execute()


# ========================
# MAIN
# ========================

if __name__ == "__main__":

    url = "https://dallas.texas.sheriffsaleauctions.com/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE=02/03/2026"

    results = scrape_data(url)

    print("Total records:", len(results))

    log_data_to_google_sheets(results)
