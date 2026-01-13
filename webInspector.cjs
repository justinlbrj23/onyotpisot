// webInspector.cjs
// RealForeclose SOLD auction parser (DOM-anchored, pagination-safe)

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
// LOAD URLS
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
// HELPERS
// =========================
function money(val) {
  if (!val) return null;
  const n = parseFloat(val.replace(/[^0-9.-]/g, ''));
  return isNaN(n) ? null : n;
}

function getField($table, label) {
  const row = $table
    .find('th.AD_LBL')
    .filter((_, th) => $(th).text().trim() === label)
    .closest('tr');

  return row.find('td.AD_DTA').text().trim();
}

function detectTotalPages($) {
  let max = 1;
  $('a[href*="PAGE="]').each((_, a) => {
    const m = $(a).attr('href')?.match(/PAGE=(\d+)/i);
    if (m) max = Math.max(max, parseInt(m[1], 10));
  });
  return max;
}

function withPage(url, page) {
  const u = new URL(url);
  u.searchParams.set('PAGE', page);
  return u.toString();
}

// =========================
// INSPECT ONE PAGE
// =========================
async function inspectPage(browser, url) {
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  try {
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
        'AppleWebKit/537.36 (KHTML, like Gecko) ' +
        'Chrome/120.0.0.0 Safari/537.36'
    );

    console.log(`🌐 Visiting ${url}`);
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 120000 });
    await new Promise(r => setTimeout(r, 6000));

    const html = await page.content();
    const $ = cheerio.load(html);

    const rows = [];

    // =========================
    // REAL AUCTION PARSER
    // =========================
    $('table.ad_tab').each((_, table) => {
      const $table = $(table);

      const statusText = $table
        .closest('div')
        .find('.ASTAT_MSB')
        .text()
        .replace(/\s+/g, ' ')
        .trim();

      const isSold =
        /Redeemed|Closed|Cancelled/i.test(statusText);

      if (!isSold) return;

      const caseNumber = getField($table, 'Case #:');
      const parcelId = getField($table, 'Parcel ID:')
        .split('|')[0]
        .trim();
      const propertyAddress = getField($table, 'Property Address:');
      const openingBidStr = getField($table, 'Opening Bid:');
      const assessedValueStr = getField($table, 'Assessed Value:');

      const openingBid = money(openingBidStr);
      const assessedValue = money(assessedValueStr);

      if (
        !caseNumber ||
        !parcelId ||
        openingBid === null ||
        assessedValue === null
      )
        return;

      const surplus = assessedValue - openingBid;
      if (surplus < MIN_SURPLUS) return;

      rows.push({
        sourceUrl: url,
        auctionStatus: 'Sold',
        auctionType: 'Tax Sale',
        caseNumber,
        parcelId,
        propertyAddress,
        openingBid: openingBidStr,
        salePrice: openingBidStr, // Redeemed == paid opening bid
        assessedValue: assessedValueStr,
        surplus,
        meetsMinimumSurplus: 'Yes',
      });
    });

    const totalPages = detectTotalPages($);
    console.log(`📦 SOLD+SURPLUS: ${rows.length} | Pages: ${totalPages}`);

    return { rows, totalPages };
  } catch (err) {
    console.error(`❌ Error on ${url}: ${err.message}`);
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
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });

  const all = [];
  const seen = new Set();
  const errors = [];

  for (const baseUrl of urls) {
    const first = await inspectPage(browser, withPage(baseUrl, 1));

    for (const r of first.rows) {
      const k = `${r.caseNumber}|${r.parcelId}`;
      if (!seen.has(k)) {
        seen.add(k);
        all.push(r);
      }
    }

    for (let p = 2; p <= first.totalPages; p++) {
      const res = await inspectPage(browser, withPage(baseUrl, p));

      for (const r of res.rows) {
        const k = `${r.caseNumber}|${r.parcelId}`;
        if (!seen.has(k)) {
          seen.add(k);
          all.push(r);
        }
      }

      if (res.error) errors.push({ url: baseUrl, page: p, error: res.error });
    }
  }

  await browser.close();

  fs.writeFileSync(OUTPUT_ROWS_FILE, JSON.stringify(all, null, 2));
  if (errors.length)
    fs.writeFileSync(OUTPUT_ERRORS_FILE, JSON.stringify(errors, null, 2));

  console.log(`✅ Saved ${all.length} SOLD + SURPLUS auctions`);
  console.log('🏁 Done');
})();