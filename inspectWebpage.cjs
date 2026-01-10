// inspectWebpage.cjs
// Requires:
// npm install puppeteer googleapis

const puppeteer = require('puppeteer');
const fs = require('fs');
const { google } = require('googleapis');

// =========================
// CONFIG
// =========================
const SPREADSHEET_ID = '1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA';
const SHEET_NAME = 'web_tda';
const URL_RANGE = 'C2:C';

const OUTPUT_FILE = 'raw-scrape.json';
const MAX_PAGES = 50;
const MIN_SURPLUS = 25000; // ✅ BUSINESS RULE

// =========================
// Helper: parse currency
// =========================
function parseCurrency(str) {
  if (!str) return null;
  const num = parseFloat(str.replace(/[^0-9.-]/g, ''));
  return isNaN(num) ? null : num;
}

// =========================
// Load URLs from Google Sheets
// =========================
async function loadTargetUrls() {
  const auth = new google.auth.GoogleAuth({
    keyFile: 'service-account.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  });

  const sheets = google.sheets({ version: 'v4', auth });

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!${URL_RANGE}`,
  });

  return (res.data.values || [])
    .flat()
    .map(v => v.trim())
    .filter(v => v.startsWith('http'));
}

// =========================
// Scrape paginated table (DETACHED-NODE SAFE)
// =========================
async function scrapePaginatedTable(browser, url) {
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  console.log(`🌐 Visiting ${url}`);
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 120000 });

  const collected = [];
  let pageIndex = 1;

  while (pageIndex <= MAX_PAGES) {
    console.log(`🔄 Page ${pageIndex}`);

    await page.waitForSelector('body', { timeout: 60000 });

    // Fingerprint table to detect real change
    const tableFingerprint = await page.$$eval(
      'table tr td:first-child',
      tds => tds.map(td => td.innerText.trim()).join('|')
    );

    // -------------------------
    // Extract VALID rows only
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

          // HARD VALIDATION
          if (!/^\d+$/.test(id)) return null;
          if (!saleDate.includes('/')) return null;
          if (!openingBid.includes('$')) return null;

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
    // Surplus calculation (MIN = $25,000)
    // -------------------------
    rows.forEach(r => {
      const open = parseCurrency(r.openingBid);
      const win = parseCurrency(r.winningBid);

      if (open !== null && win !== null) {
        r.surplus = win - open;
        r.meetsMinimumSurplus = r.surplus >= MIN_SURPLUS ? 'Yes' : 'No';
      } else {
        r.surplus = null;
        r.meetsMinimumSurplus = 'No';
      }
    });

    collected.push(...rows);
    console.log(`📦 Valid rows: ${rows.length}`);

    // -------------------------
    // SAFE pagination (no ElementHandle)
    // -------------------------
    const hasNext = await page.evaluate(() => {
      const next = [...document.querySelectorAll('a')]
        .find(a => a.textContent.trim().toLowerCase().startsWith('next'));
      if (!next) return false;
      if (next.classList.contains('disabled')) return false;
      if (next.hasAttribute('disabled')) return false;
      next.click();
      return true;
    });

    if (!hasNext) {
      console.log('⏹ No Next page available');
      break;
    }

    await page.waitForFunction(
      prev => {
        const cells = [...document.querySelectorAll('table tr td:first-child')]
          .map(td => td.innerText.trim())
          .join('|');
        return cells !== prev;
      },
      { timeout: 60000 },
      tableFingerprint
    );

    pageIndex++;
  }

  await page.close();
  return collected;
}

// =========================
// MAIN
// =========================
(async () => {
  console.log('📥 Loading URLs from Google Sheets...');
  const urls = await loadTargetUrls();

  if (!urls.length) {
    console.error('❌ No URLs found in sheet');
    process.exit(1);
  }

  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
    ],
  });

  const allResults = [];

  for (const url of urls) {
    try {
      const rows = await scrapePaginatedTable(browser, url);
      rows.forEach(r => (r.sourceUrl = url));
      allResults.push(...rows);
    } catch (err) {
      console.error(`❌ Failed scraping ${url}`, err.message);
    }
  }

  await browser.close();

  // Deduplicate
  const deduped = Array.from(
    new Map(
      allResults.map(r => [`${r.id}-${r.apn}-${r.saleDate}`, r])
    ).values()
  );

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(deduped, null, 2));
  console.log(`✅ Saved ${deduped.length} rows to ${OUTPUT_FILE}`);
  console.log('🏁 Done');
})();