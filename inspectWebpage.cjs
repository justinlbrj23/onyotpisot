// inspectWebpage.cjs
// Requires:
// npm install puppeteer cheerio googleapis

const puppeteer = require('puppeteer');
const cheerio = require('cheerio');
const fs = require('fs');
const { google } = require('googleapis');

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = './service-account.json';
const SPREADSHEET_ID = '1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA';
const SHEET_NAME = 'web_tda';
const URL_RANGE = 'C2:C';

const OUTPUT_ELEMENTS_FILE = 'raw-elements.json';
const OUTPUT_ROWS_FILE = 'parsed-auctions.json';
const OUTPUT_ERRORS_FILE = 'errors.json';

const MIN_SURPLUS = 25000;

// =========================
// GOOGLE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
});
const sheets = google.sheets({ version: 'v4', auth });

// =========================
// Load URLs
// =========================
async function loadTargetUrls() {
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
// Helpers
// =========================
function parseCurrency(str) {
  if (!str) return null;
  const n = parseFloat(str.replace(/[^0-9.-]/g, ''));
  return isNaN(n) ? null : n;
}

function withPage(url, pageNum) {
  const u = new URL(url);
  u.searchParams.set('PAGE', pageNum);
  return u.toString();
}

// ✅ REAL pagination detection
function extractTotalPages($) {
  let maxPage = 1;
  $('a[href*="PAGE="]').each((_, a) => {
    const m = $(a).attr('href').match(/PAGE=(\d+)/i);
    if (m) maxPage = Math.max(maxPage, parseInt(m[1], 10));
  });
  return maxPage;
}

// =========================
// Inspect + Parse ONE PAGE
// =========================
async function inspectSinglePage(browser, url) {
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  try {
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
        '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    );
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, 'webdriver', { get: () => false });
    });

    console.log(`🌐 Visiting ${url}`);
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 120000 });
    await new Promise(r => setTimeout(r, 6000));

    const html = await page.content();
    const $ = cheerio.load(html);

    // ----------------------------
    // SOLD AUCTIONS (STRUCTURAL)
    // ----------------------------
    const parsedRows = [];

    $('.AuctionSold').each((_, soldBlock) => {
      const container = $(soldBlock).closest('div');

      const sale = parseCurrency(
        container.find(':contains("Amount")').next().text()
      );
      const assessed = parseCurrency(
        container.find(':contains("Assessed Value")').next().text()
      );

      if (!sale || !assessed) return;

      const surplus = assessed - sale;
      if (surplus < MIN_SURPLUS) return;

      const parcelId = container.find('a[href*="Parcel"]').text().trim();
      const caseNumber = container
        .find(':contains("Case #")')
        .next()
        .text()
        .trim();

      if (!parcelId || !caseNumber) return;

      parsedRows.push({
        sourceUrl: url,
        auctionStatus: 'Sold',
        auctionType: 'Tax Sale',
        caseNumber,
        parcelId,
        propertyAddress: container
          .find(':contains("Property Address")')
          .next()
          .text()
          .trim(),
        salePrice: sale,
        assessedValue: assessed,
        surplus,
        meetsMinimumSurplus: 'Yes',
      });
    });

    const totalPages = extractTotalPages($);
    console.log(`📦 SOLD+SURPLUS: ${parsedRows.length} | Pages: ${totalPages}`);

    return { parsedRows, totalPages };
  } catch (err) {
    return { parsedRows: [], totalPages: 1, error: err.message };
  } finally {
    await page.close();
  }
}

// =========================
// MAIN
// =========================
(async () => {
  console.log('📥 Loading URLs...');
  const urls = await loadTargetUrls();

  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });

  const allRows = [];
  const errors = [];
  const seen = new Set();

  for (const baseUrl of urls) {
    const first = await inspectSinglePage(browser, withPage(baseUrl, 1));

    for (const r of first.parsedRows) {
      const k = `${r.caseNumber}|${r.parcelId}`;
      if (!seen.has(k)) {
        seen.add(k);
        allRows.push(r);
      }
    }

    for (let p = 2; p <= first.totalPages; p++) {
      const res = await inspectSinglePage(browser, withPage(baseUrl, p));
      for (const r of res.parsedRows) {
        const k = `${r.caseNumber}|${r.parcelId}`;
        if (!seen.has(k)) {
          seen.add(k);
          allRows.push(r);
        }
      }
      if (res.error) errors.push({ url: baseUrl, page: p, error: res.error });
    }
  }

  await browser.close();

  fs.writeFileSync(OUTPUT_ROWS_FILE, JSON.stringify(allRows, null, 2));
  if (errors.length)
    fs.writeFileSync(OUTPUT_ERRORS_FILE, JSON.stringify(errors, null, 2));

  console.log(`✅ Saved ${allRows.length} SOLD + SURPLUS auctions`);
  console.log('🏁 Done');
})();