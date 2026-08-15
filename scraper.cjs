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
            waitUntil: 'domcontentloaded',
            timeout: 120000
        }
    );

    console.log('Final URL:', page.url());

    await page.screenshot({
        path: 'debug-page.png',
        fullPage: true
    });

    fs.writeFileSync(
        'debug-page.html',
        await page.content()
    );

    const tableExists = await page.locator(
        '#ctl00_ContentPlaceHolder1_WSExtendedGridNP1_GridView1'
    ).count();

    console.log('Table exists:', tableExists);

    await browser.close();
})();
