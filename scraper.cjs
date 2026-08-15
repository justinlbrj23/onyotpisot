const fs = require('fs');
const { chromium } = require('playwright');

(async () => {
    const browser = await chromium.launch({
        headless: true
    });

    const page = await browser.newPage();

    await page.goto(
        'https://www.publicnoticeoregon.com/(S(favgjx24ximbftgkkdyisixq))/Search.aspx',
        {
            waitUntil: 'networkidle',
            timeout: 120000
        }
    );

    console.log('URL:', page.url());

    const html = await page.content();

    fs.writeFileSync('page.html', html);

    const tableIds = await page.$$eval(
        'table',
        tables => tables.map(t => ({
            id: t.id,
            class: t.className
        }))
    );

    console.log(JSON.stringify(tableIds, null, 2));

    await page.screenshot({
        path: 'page.png',
        fullPage: true
    });

    await browser.close();
})();
