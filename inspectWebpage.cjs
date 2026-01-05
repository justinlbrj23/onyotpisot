// inspectWebpage.cjs
// Requires:
// npm install puppeteer googleapis

const puppeteer = require('puppeteer');
const fs = require('fs');
const { google } = require('googleapis');

// =========================
// CONFIG
// =========================
const SPREADSHEET_ID = process.env.GSHEET_ID; // REQUIRED
const SHEET_NAME = 'web_tda';
const URL_RANGE = 'C2:C';

const OUTPUT_FILE = 'raw-scrape.json';
const MAX_PAGES = 50;

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
// Scrape paginated table
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
    await page.waitForSelector('table tr td', { timeout: 60000 });

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

          // HARD VALIDATION (kills headers, footers, announcements)
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

    collected.push(...rows);
    console.log(`📦 Valid rows: ${rows.length}`);

    const previousTable = await page.$eval('table', el => el.innerHTML);

    const nextHandle = await page.evaluateHandle(() => {
      return [...document.querySelectorAll('a')]
        .find(a => a.textContent.trim().toLowerCase().startsWith('next')) || null;
    });

    const nextExists = await nextHandle.jsonValue();
    if (!nextExists) break;

    const disabled = await page.evaluate(
      el => el.hasAttribute('disabled') || el.classList.contains('disabled'),
      nextHandle
    );
    if (disabled) break;

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

  await page.close();
  return collected;
}

// =========================
// MAIN
// =========================
(async () => {
  console.log('📥 Loading target URLs from Google Sheets...');
  const targetUrls = await loadTargetUrls();

  if (!targetUrls.length) {
    console.error('❌ No URLs found in sheet.');
    process.exit(1);
  }

  console.log(`✅ ${targetUrls.length} URLs loaded`);

  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
    ],
  });

  const allResults = [];

  for (const url of targetUrls) {
    try {
      const rows = await scrapePaginatedTable(browser, url);
      rows.forEach(r => (r.sourceUrl = url));
      allResults.push(...rows);
    } catch (err) {
      console.error(`❌ Failed scraping ${url}`, err);
    }
  }

  await browser.close();

  // Deduplicate (ID + APN + Sale Date)
  const deduped = Array.from(
    new Map(
      allResults.map(r => [`${r.id}-${r.apn}-${r.saleDate}`, r])
    ).values()
  );

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(deduped, null, 2));

  console.log(`✅ Saved ${deduped.length} VALID rows to ${OUTPUT_FILE}`);
  console.log('🏁 Done.');
})();