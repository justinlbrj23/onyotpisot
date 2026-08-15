const fs = require('fs');
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

    await page.screenshot({
        path: 'debug.png',
        fullPage: true
    });

    const ids = await page.$$eval(
        '[id]',
        els => els.map(el => el.id)
    );

    fs.writeFileSync(
        'element-ids.json',
        JSON.stringify(ids, null, 2)
    );

    fs.writeFileSync(
        'page.html',
        await page.content()
    );

    console.log('Found IDs:', ids.length);

    await browser.close();
})();
