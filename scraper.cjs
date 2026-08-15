const fs = require('fs');
const { chromium } = require('playwright');

(async () => {
    const browser = await chromium.launch({
        headless: true
    });

    const page = await browser.newPage();

    await page.goto(
        'https://www.publicnoticeoregon.com/(S(ayzufrbyvqdnk3fqqajn0b4c))/Search.aspx',
        {
            waitUntil: 'networkidle',
            timeout: 120000
        }
    );

    const bodyText = await page.locator('body').innerText();

    console.log(bodyText.substring(0, 5000));

    fs.writeFileSync(
        'page.html',
        await page.content()
    );

    await page.screenshot({
        path: 'page.png',
        fullPage: true
    });

    await browser.close();
})();
