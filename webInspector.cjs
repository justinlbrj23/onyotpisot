// webInspector.cjs
// RealForeclose SOLD-card parser (DOM-anchored, pagination-safe)

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
function money(val) {
  if (!val) return null;
  const n = parseFloat(val.replace(/[^0-9.-]/g, ''));
  return isNaN(n) ? null : n;
}

function getValue($card, label) {
  const labelNode = $card.find(`:contains("${label}")`).first();
  if (!labelNode.length) return '';
  return labelNode.next().text().trim();
}

function detectTotalPages($) {
  let max = 1;
  $('a[href*="PAGE="]').each((_, a) => {
    const m = $(a).attr('href').match(/PAGE=(\d+)/);
    if (m) max = Math.max(max, parseInt(m[1], 10));
  });
  return max;
}

// =========================
// Inspect + Parse
// =========================
async function inspectPage(browser, url) {
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  try {
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
        '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    );

    console.log(`🌐 Visiting ${url}`);
    await page.goto(url, { waitUntil: 'networkidle2' });
    await new Promise(r => setTimeout(r, 5000));

    const html = await page.content();
    const $ = cheerio.load(html);

    const rows = [];

    // ✅ TRUE SOLD CARD ANCHOR
    $('.AuctionSold').each((_, soldBadge) => {
      const card = $(soldBadge).closest('.auctionItem, div');

      const sale = money(getValue(card, 'Amount'));
      const assessed = money(getValue(card, 'Assessed Value'));
      if (!sale || !assessed) return;

      const surplus = assessed - sale;
      if (surplus < MIN_SURPLUS) return;

      const row = {
        sourceUrl: url,
        auctionStatus: 'Sold',
        auctionType: getValue(card, 'Auction Type') || 'Tax Sale',
        caseNumber: getValue(card, 'Case #'),
        parcelId: getValue(card, 'Parcel ID').split('|')[0].trim(),
        propertyAddress: getValue(card, 'Property Address'),
        salePrice: sale,
        assessedValue: assessed,
        surplus,
        meetsMinimumSurplus: 'Yes',
      };

      if (row.caseNumber && row.parcelId) {
        rows.push(row);
      }
    });

    const totalPages = detectTotalPages($);
    console.log(`📦 SOLD+SURPLUS: ${rows.length} | Pages: ${totalPages}`);

    return { rows, totalPages };
  } catch (err) {
    return { rows: [], totalPages: 1, error: err.message };
  } finally {
    await page.close();
  }
}

// =========================
// MAIN
// =========================
(async () => {
  const urls = await loadTargetUrls();
  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox'],
  });

  const all = [];
  const seen = new Set();
  const errors = [];

  for (const baseUrl of urls) {
    const first = await inspectPage(browser, baseUrl);
    const pages = first.totalPages || 1;

    for (const r of first.rows) {
      const k = `${r.caseNumber}|${r.parcelId}`;
      if (!seen.has(k)) {
        seen.add(k);
        all.push(r);
      }
    }

    for (let p = 2; p <= pages; p++) {
      const res = await inspectPage(browser, `${baseUrl}&PAGE=${p}`);
      for (const r of res.rows) {
        const k = `${r.caseNumber}|${r.parcelId}`;
        if (!seen.has(k)) {
          seen.add(k);
          all.push(r);
        }
      }
      if (res.error) errors.push(res.error);
    }
  }

  await browser.close();

  fs.writeFileSync(OUTPUT_ROWS_FILE, JSON.stringify(all, null, 2));
  if (errors.length)
    fs.writeFileSync(OUTPUT_ERRORS_FILE, JSON.stringify(errors, null, 2));

  console.log(`✅ Saved ${all.length} SOLD + SURPLUS auctions`);
  console.log('🏁 Done');
})();