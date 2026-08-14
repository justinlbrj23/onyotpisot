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

    const tableSelector =
        '#ctl00_ContentPlaceHolder1_WSExtendedGridNP1_GridView1';

    await page.waitForSelector(tableSelector, {
        timeout: 60000
    });

    const rows = await page.$$eval(
        '#ctl00_ContentPlaceHolder1_WSExtendedGridNP1_GridView1 > tbody > tr',
        trs => {
            return trs.map(tr => {
                const cells = [...tr.querySelectorAll('th, td')];

                return cells.map(cell =>
                    cell.innerText
                        .replace(/\r?\n/g, ' ')
                        .replace(/\s+/g, ' ')
                        .trim()
                );
            });
        }
    );

    const csv = rows
        .map(row =>
            row
                .map(cell => `"${String(cell).replace(/"/g, '""')}"`)
                .join(',')
        )
        .join('\n');

    fs.writeFileSync('output.csv', csv, 'utf8');

    console.log(`Saved ${rows.length} rows to output.csv`);

    await browser.close();
})();
