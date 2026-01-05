// inspectWebpage.cjs
// Requires:
// npm install puppeteer cheerio

const puppeteer = require('puppeteer');
const cheerio = require('cheerio');
const fs = require('fs');

// =========================
// CONFIG
// =========================
const TARGET_URL = 'https://sacramento.mytaxsale.com/reports/total_sales';
const OUTPUT_FILE = 'raw-scrape.json';

// =========================
// FUNCTION: Scrape Table Rows
// =========================
async function scrapeTable(url) {
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

    await page.goto(url, {
      waitUntil: 'domcontentloaded',
      timeout: 120000,
    });

    await page.waitForSelector('table', { timeout: 60000 });

    const html = await page.content();
    const $ = cheerio.load(html);

    const rows = [];

    $('table tr').each((i, el) => {
      const cells = $(el).find('td');
      if (cells.length === 0) return; // skip header row

      const row = {
        id: $(cells[0]).text().trim(),
        apn: $(cells[1]).text().trim(),
        saleDate: $(cells[2]).text().trim(),
        openingBid: $(cells[3]).text().trim(),
        winningBid: $(cells[4]).text().trim(),
        notes: $(cells[5]).text().trim(),
      };

      rows.push(row);
    });

    return rows;
  } catch (err) {
    console.error('❌ Error during table scrape:', err);
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
  console.log('🔍 Scraping table from webpage...');
  const results = await scrapeTable(TARGET_URL);

  console.log(`📦 Total rows extracted: ${results.length}`);
  console.log('🧪 Sample row:', results[0]);

  try {
    fs.writeFileSync(OUTPUT_FILE, JSON.stringify(results, null, 2));
    console.log(`✅ Saved to ${OUTPUT_FILE}`);
  } catch (err) {
    console.error('❌ Error writing JSON file:', err);
  }

  console.log('🏁 Done.');
})();