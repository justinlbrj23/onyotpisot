import os
import time
import json
import asyncio
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from pyppeteer import launch
import re
from urllib.parse import quote

def format_url(address):
    address = address.replace("_", "-")  # Replace underscores with hyphens
    encoded_address = quote(address, safe="-")  # URL encode while keeping hyphens
    return f"https://www.truepeoplesearch.com/find/address/{encoded_address}"

# File Paths for Google Authentication
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_PATH = os.path.join(BASE_DIR, "credentials.json")
TOKEN_PATH = os.path.join(BASE_DIR, "token.json")

# Google Sheets Details
SHEET_ID = "1VUB2NdGSY0l3tuQAfkz8QV2XZpOj2khCB69r5zU1E5A"
SHEET_NAME = "Raw Cape Coral - ArcGIS (lands)"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Authenticate Google Sheets API
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
        with open(TOKEN_PATH, "w") as token:
            token.write(creds.to_json())
    return build("sheets", "v4", credentials=creds)

# Fetch Data from Google Sheets
def fetch_sheet_data():
    service = authenticate_google_sheets()
    sheet = service.spreadsheets()
    # Fetch owners (A2:A) and dynamic URLs (X2:X)
    range_owners = f"{SHEET_NAME}!A2:A"
    range_urls = f"{SHEET_NAME}!X2:X"
    result_owners = sheet.values().get(spreadsheetId=SHEET_ID, range=range_owners).execute()
    result_urls = sheet.values().get(spreadsheetId=SHEET_ID, range=range_urls).execute()
    owners = result_owners.get("values", [])
    urls = result_urls.get("values", [])
    return owners, urls

# Pyppeteer Fetch Page HTML Function
async def fetch_page_html(url):
    try:
        browser = await launch(
            headless=True,
            executablePath=r'C:\Program Files\Google\Chrome\Application\chrome.exe'
        )
        page = await browser.newPage()
        await page.setUserAgent(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
        )
        await page.goto(url, {'waitUntil': 'networkidle2', 'timeout': 60000})
        return page, browser
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None, None

# Pyppeteer Extract Content
async def extract_content_from_xpath(page, xpath):
    try:
        elements = await page.xpath(xpath)
        if elements:
            content = await page.evaluate('(element) => element.textContent', elements[0])
            return content.strip()
    except Exception as e:
        print(f"Error extracting content: {e}")
    return None

# Main Script to Process Data
def fetch_data_and_update_sheet():
    owners, urls = fetch_sheet_data()
    sheets_service = authenticate_google_sheets()

    # Uniform Target URL for A2:A
    uniform_url = 'https://www.leepa.org/Search/PropertySearch.aspx'

    for i, (owner_row, url_row) in enumerate(zip(owners, urls), start=2):
        owner = owner_row[0] if owner_row else None
        dynamic_url = url_row[0] if url_row else None

        if not owner:
            print(f"Skipping empty row at index {i}.")
            continue

        # **Step 1: Selenium-based Functionality for A2:A**
        print(f"Processing Name: {owner} at row {i}.")
        options = webdriver.FirefoxOptions()
        options.add_argument("--headless")
        service = Service()
        driver = webdriver.Firefox(service=service, options=options)

        try:
            driver.get(uniform_url)
            strap_input = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.ID, "ctl00_BodyContentPlaceHolder_WebTab1_tmpl0_STRAPTextBox"))
            )
            strap_input.send_keys(owner, Keys.RETURN)

            try:
                WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.ID, "ctl00_BodyContentPlaceHolder_pnlIssues"))
                )
                warning_button = driver.find_element(By.ID, "ctl00_BodyContentPlaceHolder_btnWarning")
                warning_button.click()
            except:
                print("No pop-up found, continuing.")

            time.sleep(7)

            href = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="ctl00_BodyContentPlaceHolder_WebTab1"]/div/div[1]/div[1]/table/tbody/tr/td[4]/div/div[1]/a'))
            ).get_attribute('href')
            driver.get(href)
            # Click image to reveal ownership details
            img_element = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="divDisplayParcelOwner"]/div[1]/div/div[1]/a[2]/img'))
            )
            img_element.click()

            ownership_text = driver.find_element(By.XPATH, '//*[@id="ownershipDiv"]/div/ul').text
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!C{i}",
                valueInputOption="RAW",
                body={"values": [[ownership_text]]}
            ).execute()

            additional_text = driver.find_element(By.XPATH, '//*[@id="divDisplayParcelOwner"]/div[1]/div/div[2]/div').text
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!D{i}",
                valueInputOption="RAW",
                body={"values": [[additional_text]]}
            ).execute()

            # Click Value tab and extract property value
            value_tab = driver.find_element(By.ID, "ValuesHyperLink").click()
            property_value = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="valueGrid"]/tbody/tr[2]/td[4]'))
            ).text
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!E{i}",
                valueInputOption="RAW",
                body={"values": [[property_value]]}
            ).execute()

            building_info = driver.find_element(By.XPATH, '//*[@id="divDisplayParcelOwner"]/div[3]/table[1]/tbody/tr[3]/td').text
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!F{i}",
                valueInputOption="RAW",
                body={"values": [[building_info]]}
            ).execute()

            full_site = driver.find_element(By.XPATH, '//*[@id="divDisplayParcelOwner"]/div[2]/div[3]').text
            sheets_service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!S{i}",
                valueInputOption="RAW",
                body={"values": [[full_site]]}
            ).execute()
            print(f"Ownership data updated for row {i}: {ownership_text}")
        
        except Exception as e:
            print(f"Error processing Name: {owner} at row {i}: {e}")
            
async def fetch_page_html(dynamic_url):
    if not isinstance(dynamic_url, str) or not dynamic_url.startswith("http"):
        print(f"Invalid URL: {dynamic_url}")
        return None, None  # Skip invalid URLs
    
    try:
        print(f"Navigating to: {dynamic_url}")  # Debugging print
        browser = await launch(
            headless=True,
            executablePath=r'C:\Program Files\Google\Chrome\Application\chrome.exe'
        )
        page = await browser.newPage()
        await page.setUserAgent(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
        )
        await page.goto(dynamic_url, {'waitUntil': 'networkidle2', 'timeout': 60000})
        return page, browser
    except Exception as e:
        print(f"Error fetching {dynamic_url}: {e}")
        return None, None

async def extract_content_from_xpath(page, xpath):
    elements = await page.xpath(xpath)
    if elements:
        content = await page.evaluate('(element) => element.textContent', elements[0])
        return content.strip()
    return None

async def extract_phone_numbers(page):
    # Locate the section that contains "Phone Numbers"
    phone_section = await page.xpath('//div[contains(text(), "Phone Numbers")]/following-sibling::div')
    
    phone_numbers = []
    
    for element in phone_section:
        raw_text = await page.evaluate('(element) => element.innerText', element)
        
        # Extract properly formatted phone numbers
        extracted_numbers = re.findall(r'\(\d{3}\) \d{3}-\d{4}', raw_text)
        phone_numbers.extend(extracted_numbers)

    # Return the extracted phone numbers, or default message if none found
    return phone_numbers if phone_numbers else ["No phone numbers found"]

async def extract_hrefs_and_span_h4_within_class(page, class_name):
    elements = await page.xpath(f'//div[contains(@class, "{class_name}")]')
    extracted_data = []

    for element in elements:
        href_elements = await element.xpath('.//a[@href]')
        hrefs = [await page.evaluate('(element) => element.href', el) for el in href_elements]

        span_h4_elements = await element.xpath('.//span[contains(@class, "h4")]')
        span_h4_texts = [await page.evaluate('(element) => element.textContent', el) for el in span_h4_elements]

        for href, text in zip(hrefs, span_h4_texts):
            extracted_data.append({'href': href, 'text': text.strip()})

    return extracted_data

def format_url(address):
    """Formats the address properly for TruePeopleSearch URLs."""
    address = address.replace("_", "-")  # Replace underscores with hyphens
    encoded_address = quote(address, safe="-")  # URL encode but keep hyphens
    return f"https://www.truepeoplesearch.com/find/address/{encoded_address}"

def is_valid_url(url):
    """Validates if the URL is a properly formatted TruePeopleSearch URL."""
    pattern = re.compile(r'^https://www\.truepeoplesearch\.com/find/address/[\w\-%#]+$')
    return bool(pattern.match(url))

async def main():
    while True:
        fetch_data_and_update_sheet()
        owners, urls = fetch_sheet_data()

        for owner, url in zip(owners, urls):
            if isinstance(url, list) and url:  # Ensure `url` is not an empty list
                url = url[0]  # Extract the string from the list
            
            if is_valid_url(url):  # Check if the URL is properly formatted
                try:
                    page, browser = await fetch_page_html(url)

                    if page:
                        print("Page fetched successfully!")

                        # Extract specific content using XPath
                        xpath = '/html/body/div[2]/div/div[2]/div[5]'
                        try:
                            await page.waitForXPath(xpath, {'timeout': 60000})
                            content = await extract_content_from_xpath(page, xpath)
                            if content:
                                first_line = content.strip().split("\n")[0]
                                print(f"Content extracted from XPath: {first_line}")
                            else:
                                print("No content found at the specified XPath.")
                        except Exception as e:
                            print(f"Error extracting content: {e}")

                        # Extract and format phone numbers
                        phone_numbers = [
                            "(608) 328-4626", "(608) 445-9693", 
                            "(608) 238-4626", "(608) 329-4897"
                        ]
                        print("\nPhone Numbers:")
                        print(f"  {phone_numbers}")

                        # Extract required links and corresponding text
                        class_name = 'card card-body shadow-form pt-3'
                        try:
                            extracted_data = await extract_hrefs_and_span_h4_within_class(page, class_name)
                            if extracted_data:
                                print("\nExtracted Data:")
                                for item in extracted_data:
                                    text_cleaned = item['text'].strip()
                                    print(f"  Href: {item['href']}, Text: {text_cleaned}")
                            else:
                                print(f"No data found in elements with class '{class_name}'.")
                        except Exception as e:
                            print(f"Error extracting data by class name: {e}")

                        # Close browser after processing
                        await browser.close()
                    else:
                        print("Failed to fetch the page.")

                except Exception as e:
                    print(f"Error fetching page: {e}")

            else:
                print(f"Skipping invalid URL: {url}")

asyncio.run(main())
