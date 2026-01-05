// inspectWebpage.cjs
// Requires:
// npm install puppeteer

const puppeteer = require('puppeteer');
const fs = require('fs');

// =========================
// CONFIG
// =========================
const TARGET_URL = 'https://sacramento.mytaxsale.com/reports/total_sales';
const OUTPUT_FILE = 'raw-scrape.json';

// =========================
// FUNCTION: Scrape Paginated Table
// =========================
async function scrapePaginatedTable(url) {
  let browser;

  try {
    browser = await puppeteer.launch({
      headless: 'new',
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
      ],
    });

    const page = await browser.newPage();
    page.setDefaultNavigationTimeout(120000);
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 120000 });

    const allRows = [];
    let pageIndex = 1;

    while (true) {
      console.log(`🔄 Scraping page ${pageIndex}...`);
      await page.waitForSelector('table tr td', { timeout: 60000 });

      // Extract rows from current page
      const rows = await page.$$eval('table tr', trs =>
        trs
          .map(tr => {
            const tds = Array.from(tr.querySelectorAll('td'));
            if (tds.length < 3) return null; // skip header or malformed rows
            return {
              apn: tds[0]?.innerText.trim() || '',
              caseNumber: tds[1]?.innerText.trim() || '',
              saleDate: tds[2]?.innerText.trim() || '',
              // optional: add more if table has extra columns
            };
          })
          .filter(Boolean)
      );

      allRows.push(...rows);
      console.log(`📦 Page ${pageIndex} rows: ${rows.length}`);

      // Try to find "Next" button in paginator
      const nextButton = await page.$('a.next, button.next, a[aria-label="Next"]');
      if (!nextButton) {
        console.log('⏹ No Next button found, stopping.');
        break;
      }

      const isDisabled = await page.evaluate(el =>
        el.hasAttribute('disabled') || el.classList.contains('disabled'),
        nextButton
      );
      if (isDisabled) {
        console.log('⏹ Next button disabled, stopping.');
        break;
      }

      // Click next and wait for table to reload
      await Promise.all([
        nextButton.click(),
        page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 60000 }),
      ]);

      pageIndex++;
    }

    return allRows;
  } catch (err) {
    console.error('❌ Error during paginated scrape:', err);
    return [];
  } finally {
    if (browser) {
      await browser.close();
    }
  }
}

// =========================
// MAIN EXECUTION
// =========================
(async () => {
  console.log('🔍 Scraping paginated table from webpage...');
  const results = await scrapePaginatedTable(TARGET_URL);

  console.log(`📦 Total rows extracted: ${results.length}`);
  if (results.length > 0) {
    console.log('🧪 Sample row:', results[0]);
  }

  try {
    fs.writeFileSync(OUTPUT_FILE, JSON.stringify(results, null, 2));
    console.log(`✅ Saved to ${OUTPUT_FILE}`);
  } catch (err) {
    console.error('❌ Error writing JSON file:', err);
  }

  console.log('🏁 Done.');
})();