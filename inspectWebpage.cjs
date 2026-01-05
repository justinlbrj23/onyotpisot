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
// Helper: parse currency string → number
// =========================
function parseCurrency(str) {
  if (!str) return null;
  const num = parseFloat(str.replace(/[^0-9.-]/g, ''));
  return isNaN(num) ? null : num;
}

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
            // Adjusted for 3-column table: APN, Case Number, Auction Date
            if (tds.length < 3) return null;
            return {
              apn: tds[0]?.innerText.trim() || '',
              caseNumber: tds[1]?.innerText.trim() || '',
              saleDate: tds[2]?.innerText.trim() || '',
              // If extra columns exist (Opening Bid, Winning Bid, Notes), capture them
              openingBid: tds[3]?.innerText.trim() || '',
              winningBid: tds[4]?.innerText.trim() || '',
              notes: tds[5]?.innerText.trim() || '',
            };
          })
          .filter(Boolean)
      );

      // Add surplus calculation if bid columns exist
      rows.forEach(r => {
        const open = parseCurrency(r.openingBid);
        const win = parseCurrency(r.winningBid);
        if (open !== null && win !== null) {
          r.surplus = win - open;
          r.meetsMinimumSurplus = r.surplus > 0 ? 'Yes' : 'No';
        } else {
          r.surplus = null;
          r.meetsMinimumSurplus = '';
        }
      });

      allRows.push(...rows);
      console.log(`📦 Page ${pageIndex} rows: ${rows.length}`);

      // Capture current table HTML to detect change
      const previousTable = await page.$eval('table', el => el.innerHTML);

      // Try to find "Next" button using XPath
      const nextButton = await page.$x("//a[contains(text(),'Next') or contains(text(),'Next »')]");
      if (nextButton.length === 0) {
        console.log('⏹ No Next button found, stopping.');
        break;
      }

      const isDisabled = await page.evaluate(el =>
        el.hasAttribute('disabled') || el.classList.contains('disabled'),
        nextButton[0]
      );
      if (isDisabled) {
        console.log('⏹ Next button disabled, stopping.');
        break;
      }

      // Click next and wait for table content to change
      await Promise.all([
        nextButton[0].click(),
        page.waitForFunction(
          prev => document.querySelector('table')?.innerHTML !== prev,
          { timeout: 60000 },
          previousTable
        ),
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