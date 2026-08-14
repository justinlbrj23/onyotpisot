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

    const selector =
        '#ctl00_ContentPlaceHolder1_WSExtendedGridNP1_GridView1 > tbody > tr';

    await page.waitForSelector(selector, {
        timeout: 60000
    });

    const rows = await page.$$eval(selector, trs =>
        trs.map(tr => ({
            text: tr.innerText.trim()
        }))
    );

    const csv = [
        'Text',
        ...rows.map(row =>
            `"${row.text.replace(/"/g, '""').replace(/\n/g, ' ')}"`
        )
    ].join('\n');

    fs.writeFileSync('output.csv', csv);

    console.log(`Extracted ${rows.length} rows`);
    console.log('Saved to output.csv');

    await browser.close();
})();
