const { chromium } = require('playwright');

(async () => {
    const browser = await chromium.launch({
        headless: true
    });

    const page = await browser.newPage();

    await page.goto(
        'https://www.publicnoticeoregon.com/Search.aspx',
        {
            waitUntil: 'networkidle',
            timeout: 120000
        }
    );

    const selector =
        '#ctl00_ContentPlaceHolder1_WSExtendedGridNP1_GridView1 > tbody > tr:nth-child(3)';

    await page.waitForSelector(selector, {
        timeout: 60000
    });

    const extractedText = await page.$eval(
        selector,
        el => el.textContent.trim()
    );

    console.log(extractedText);

    await browser.close();
})();
