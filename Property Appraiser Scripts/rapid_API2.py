import asyncio
from pyppeteer import launch

async def fetch_page_html(url):
    browser = await launch(
        headless=True,
        executablePath=r'C:\Program Files\Google\Chrome\Application\chrome.exe'  # Path to Chrome
    )
    page = await browser.newPage()

    # Set a realistic user-agent
    await page.setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36")
    
    try:
        # Navigate to the page with a timeout
        await page.goto(url, {'waitUntil': 'networkidle2', 'timeout': 60000})
    except asyncio.TimeoutError:
        print(f"Timeout while fetching {url}")
    except Exception as e:
        print(f"Error fetching {url}: {e}")

    return page, browser

async def extract_content_from_xpath(page, xpath):
    # Use the XPath to find the content
    elements = await page.xpath(xpath)
    if elements:
        # Extract text content from the element using page.evaluate
        content = await page.evaluate('(element) => element.textContent', elements[0])
        return content.strip()
    return None

async def modify_iframe_padding(page, iframe_xpath, padding_value):
    # Use XPath to find the iframe
    iframe_elements = await page.xpath(iframe_xpath)
    if iframe_elements:
        iframe = iframe_elements[0]
        # Evaluate a script to modify the iframe padding
        await page.evaluate('(iframe, padding) => { iframe.style.padding = padding; }', iframe, padding_value)
        print(f"Iframe padding modified to {padding_value}")
    else:
        print("No iframe found at the specified XPath.")

async def extract_hrefs_and_span_h4_within_class(page, class_name):
    # Use XPath to find the container elements with the specified class name
    elements = await page.xpath(f'//div[contains(@class, "{class_name}")]')

    extracted_data = []

    for element in elements:
        # Extract hrefs
        href_elements = await element.xpath('.//a[@href]')
        hrefs = [await page.evaluate('(element) => element.href', el) for el in href_elements]

        # Extract span.h4 texts using CSS selector
        span_h4_elements = await element.xpath('.//span[contains(@class, "h4")]')
        span_h4_texts = [await page.evaluate('(element) => element.textContent', el) for el in span_h4_elements]

        # Combine hrefs and span.h4 texts
        for href, text in zip(hrefs, span_h4_texts):
            extracted_data.append({'href': href, 'text': text})

    return extracted_data

async def main():
    url = "https://www.truepeoplesearch.com/find/address/w5861-clar-ken-rd_monroe-wi-53566"
    page, browser = await fetch_page_html(url)
    
    if page:
        print("Page fetched successfully!")

        # XPath you provided
        xpath = '/html/body/div[2]/div/div[2]/div[5]'

        # Extract content using XPath
        content = await extract_content_from_xpath(page, xpath)

        if content:
            print(f"Content extracted from XPath: {content}")
        else:
            print("No content found at the specified XPath.")

        # Extract hrefs and span.h4 texts within elements with class "card card-body shadow-form pt-3"
        class_name = 'card card-body shadow-form pt-3'
        extracted_data = await extract_hrefs_and_span_h4_within_class(page, class_name)
        
        if extracted_data:
            print(f"Data extracted within elements with class '{class_name}':")
            for item in extracted_data:
                print(f"Href: {item['href']}, Text: {item['text']}")
        else:
            print(f"No data found within elements with class '{class_name}'.")

        # Modify iframe padding if needed
        iframe_xpath = '/html/body/iframe[1]'  # Replace with the actual XPath to the iframe
        await modify_iframe_padding(page, iframe_xpath, '10px')
    else:
        print("Failed to fetch the page.")

    await browser.close()

asyncio.run(main())
