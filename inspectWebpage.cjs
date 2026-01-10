// inspectWebpage.cjs
// Requires:
// npm install puppeteer googleapis cheerio

const puppeteer = require('puppeteer');
const fs = require('fs');
const cheerio = require('cheerio');
const { google } = require('googleapis');

// =========================
// CONFIG
// =========================
const SPREADSHEET_ID = '1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA';
const SHEET_NAME = 'web_tda';
const URL_RANGE = 'C2:C';

const OUTPUT_FILE = 'raw-scrape.json';
const MAX_PAGES = 50;
const MIN_SURPLUS = 25000;

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
// Harden page against bot detection
// =========================
async function hardenPage(page) {
  await page.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
      'AppleWebKit/537.36 (KHTML, like Gecko) ' +
      'Chrome/120.0.0.0 Safari/537.36'
  );

  await page.setExtraHTTPHeaders({
    'Accept-Language': 'en-US,en;q=0.9',
    Accept:
      'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
  });

  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, 'webdriver', {
      get: () => false,
    });
  });
}

// =========================
// Scrape paginated table (HTML + Cheerio)
// =========================
async function scrapePaginatedTable(browser, url) {
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  await hardenPage(page);

  console.log(`🌐 Visiting ${url}`);
  await page.goto(url, { waitUntil: 'networkidle2', timeout: 120000 });

  // WAF / Cloudflare delay
  await new Promise(r => setTimeout(r, 8000));

  const collected = [];
  let pageIndex = 1;
  let lastFingerprint = null;

  while (pageIndex <= MAX_PAGES) {
    console.log(`🔄 Page ${pageIndex}`);

    const html = await page.content();

    // -------------------------
    // Detect hard block
    // -------------------------
    if (
      html.includes('403 Forbidden') ||
      html.includes('Access Denied') ||
      html.toLowerCase().includes('forbidden')
    ) {
      throw new Error('Blocked by target website (403)');
    }

    const $ = cheerio.load(html);

    // -------------------------
    // Extract rows via parsing
    // -------------------------
    const rows = [];

    $('table tr').each((_, tr) => {
      const tds = $(tr).find('td');
      if (tds.length < 6) return;

      const id = $(tds[0]).text().trim();
      const apn = $(tds[1]).text().trim();
      const saleDate = $(tds[2]).text().trim();
      const openingBid = $(tds[3]).text().trim();
      const winningBid = $(tds[4]).text().trim();
      const notes = $(tds[5]).text().trim();

      if (!/^\d+$/.test(id)) return;
      if (!saleDate.includes('/')) return;
      if (!openingBid.includes('$')) return;

      const open = parseCurrency(openingBid);
      const win = parseCurrency(winningBid);

      const surplus =
        open !== null && win !== null ? win - open : null;

      rows.push({
        id,
        apn,
        saleDate,
        openingBid,
        winningBid,
        notes,
        surplus,
        meetsMinimumSurplus:
          surplus !== null && surplus >= MIN_SURPLUS ? 'Yes' : 'No',
      });
    });

    console.log(`📦 Valid rows: ${rows.length}`);
    collected.push(...rows);

    // -------------------------
    // Pagination fingerprint
    // -------------------------
    const fingerprint = rows.map(r => r.id).join('|');
    if (fingerprint === lastFingerprint) {
      console.log('⏹ No data change detected');
      break;
    }
    lastFingerprint = fingerprint;

    // -------------------------
    // Safe pagination click
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

    await page.waitForTimeout(6000);
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
      '--disable-blink-features=AutomationControlled',
    ],
  });

  const allResults = [];

  for (const url of urls) {
    try {
      const rows = await scrapePaginatedTable(browser, url);
      rows.forEach(r => (r.sourceUrl = url));
      allResults.push(...rows);
    } catch (err) {
      console.error(`❌ Failed scraping ${url}:`, err.message);
    }
  }

  await browser.close();

  // -------------------------
  // Deduplicate
  // -------------------------
  const deduped = Array.from(
    new Map(
      allResults.map(r => [`${r.id}-${r.apn}-${r.saleDate}`, r])
    ).values()
  );

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(deduped, null, 2));
  console.log(`✅ Saved ${deduped.length} rows to ${OUTPUT_FILE}`);
  console.log('🏁 Done');
})();