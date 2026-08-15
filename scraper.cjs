const fs = require('fs');
const { chromium } = require('playwright');

(async () => {
    const browser = await chromium.launch({
        headless: true
    });

    try {
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

                    return cells.map(cell => {
                        const link = cell.querySelector('a');

                        return {
                            text: cell.innerText
                                .replace(/\r?\n/g, ' ')
                                .replace(/\s+/g, ' ')
                                .trim(),
                            url: link ? link.href : ''
                        };
                    });
                });
            }
        );

        if (!rows.length) {
            throw new Error('No rows found in target table.');
        }

        const maxCols = Math.max(
            ...rows.map(row => row.length)
        );

        const csvRows = rows.map(row => {
            const values = [];

            row.forEach(cell => {
                values.push(cell.text);

                if (cell.url) {
                    values.push(cell.url);
                }
            });

            return values;
        });

        const maxCsvCols = Math.max(
            ...csvRows.map(row => row.length)
        );

        const csv = csvRows
            .map(row => {
                while (row.length < maxCsvCols) {
                    row.push('');
                }

                return row
                    .map(value =>
                        `"${String(value).replace(/"/g, '""')}"`
                    )
                    .join(',');
            })
            .join('\n');

        fs.writeFileSync(
            'output.csv',
            csv,
            'utf8'
        );

        console.log(`Rows extracted: ${rows.length}`);
        console.log(`Max table columns: ${maxCols}`);
        console.log('Saved: output.csv');

    } catch (err) {
        console.error('ERROR:', err);
        process.exit(1);
    } finally {
        await browser.close();
    }
})();
