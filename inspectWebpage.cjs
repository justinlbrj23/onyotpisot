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
const MAX_PAGES = 50; // safety limit

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

    while (pageIndex <= MAX_PAGES) {
      console.log(`🔄 Scraping page ${pageIndex}...`);
      await page.waitForSelector('table tr td', { timeout: 60000 });

      // -------------------------
      // Extract VALID data rows only
      // -------------------------
      const rows = await page.$$eval('table tr', trs =>
        trs
          .map(tr => {
            const tds = Array.from(tr.querySelectorAll('td'));
            if (tds.length < 6) return null;

            const id = tds[0].innerText.trim();
            const apn = tds[1].innerText.trim();
            const saleDate = tds[2].innerText.trim();
            const openingBid = tds[3].innerText.trim();
            const winningBid = tds[4].innerText.trim();
            const notes = tds[5].innerText.trim();

            // -------------------------
            // HARD VALIDATION
            // -------------------------
            if (!/^\d+$/.test(id)) return null;          // ID# must be numeric
            if (!saleDate.includes('/')) return null;   // must look like a date
            if (!openingBid.includes('$')) return null; // must be money

            return {
              id,
              apn,
              saleDate,
              openingBid,
              winningBid,
              notes,
            };
          })
          .filter(Boolean)
      );

      // -------------------------
      // Surplus calculation
      // -------------------------
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
      console.log(`📦 Page ${pageIndex} valid rows: ${rows.length}`);

      // -------------------------
      // Pagination handling
      // -------------------------
      const previousTable = await page.$eval('table', el => el.innerHTML);

      const nextHandle = await page.evaluateHandle(() => {
        const links = Array.from(document.querySelectorAll('a'));
        return links.find(a =>
          a.textContent.trim().toLowerCase().startsWith('next')
        ) || null;
      });

      const nextExists = await nextHandle.jsonValue();
      if (!nextExists) {
        console.log('⏹ No Next button found, stopping.');
        break;
      }

      const isDisabled = await page.evaluate(
        el => el.hasAttribute('disabled') || el.classList.contains('disabled'),
        nextHandle
      );

      if (isDisabled) {
        console.log('⏹ Next button disabled, stopping.');
        break;
      }

      await Promise.all([
        nextHandle.click(),
        page.waitForFunction(
          prev => document.querySelector('table')?.innerHTML !== prev,
          { timeout: 60000 },
          previousTable
        ),
      ]);

      pageIndex++;
    }

    if (pageIndex > MAX_PAGES) {
      console.log(`⚠️ Stopped after reaching MAX_PAGES=${MAX_PAGES}`);
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

  console.log(`📦 Total VALID rows extracted: ${results.length}`);
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