// webInspector.cjs (page intelligence + auction parser for SOLD cards only)
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
const OUTPUT_SUMMARY_FILE = 'summary.json';

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
// Currency parser
// =========================
function parseCurrency(str) {
  if (!str) return null;
  const n = parseFloat(str.replace(/[^0-9.-]/g, ''));
  return isNaN(n) ? null : n;
}

// =========================
// Helpers to extract fields
// =========================
function extractBetween(text, startLabel, stopLabels = []) {
  const idx = text.toLowerCase().indexOf(startLabel.toLowerCase());
  if (idx === -1) return '';

  let substr = text.slice(idx + startLabel.length).trim();

  let stopIndex = substr.length;
  for (const stop of stopLabels) {
    const i = substr.toLowerCase().indexOf(stop.toLowerCase());
    if (i !== -1 && i < stopIndex) stopIndex = i;
  }

  return substr.slice(0, stopIndex).trim();
}

function extractDate(text) {
  // Flexible date capture
  const m = text.match(/Date:\s*([0-9/]{10}|[0-9-]{10})/i);
  return m ? m[1] : '';
}

function extractAmountAfter(text, label) {
  const regex = new RegExp(label + '\\s*\\$[\\d,]+\\.\\d{2}', 'i');
  const m = text.match(regex);
  if (!m) return '';
  const moneyMatch = m[0].match(/\$[\d,]+\.\d{2}/);
  return moneyMatch ? moneyMatch[0] : '';
}

// =========================
// Schema validation
// =========================
function validateRow(row) {
  return (
    row.caseNumber &&
    row.parcelId &&
    row.openingBid &&
    row.salePrice &&
    row.assessedValue
  );
}

// =========================
// Inspect + Parse Page (SOLD only)
// =========================
async function inspectAndParse(browser, url) {
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  try {
    // Anti-bot hardening
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
      Object.defineProperty(navigator, 'webdriver', { get: () => false });
    });

    console.log(`🌐 Visiting ${url}`);
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 120000 });
    await new Promise(r => setTimeout(r, 8000));

    const html = await page.content();

    if (
      html.includes('403 Forbidden') ||
      html.includes('Access Denied') ||
      html.toLowerCase().includes('forbidden')
    ) {
      throw new Error('Blocked by target website (403)');
    }

    const $ = cheerio.load(html);

    // (1) FILTER: auction-relevant elements ONLY (for diagnostics)
    const relevantElements = [];
    const auctionTextRegex =
      /(\$\d{1,3}(,\d{3})+)|(\bAPN\b)|(\bParcel\b)|(\bAuction\b)|(\bCase\b)/i;

    $('*').each((_, el) => {
      const tag = el.tagName;
      const text = $(el).text().replace(/\s+/g, ' ').trim();
      const attrs = el.attribs || {};
      if (text && auctionTextRegex.test(text)) {
        relevantElements.push({ sourceUrl: url, tag, text, attrs });
      }
    });

    // (2) CARD-BASED AUCTION PARSER (SOLD ONLY)
    const parsedRows = [];
    const seen = new Set();

    $('div').each((_, container) => {
      const blockText = $(container).text().replace(/\s+/g, ' ').trim();
      if (!blockText.includes('Auction Sold')) return; // SOLD cards only

      const auctionStatus = 'Sold';

      const auctionType = extractBetween(blockText, 'Auction Type:', [
        'Case #',
        'Certificate',
        'Opening Bid',
      ]);

      const rawCase = extractBetween(blockText, 'Case #:', [
        'Certificate',
        'Opening Bid',
      ]);
      const caseNumber = rawCase.split(/\s+/)[0].trim();

      const openingBidStr = extractAmountAfter(blockText, 'Opening Bid:');
      const assessedValueStr = extractBetween(blockText, 'Assessed Value:', []);
      const assessedMoneyMatch = assessedValueStr.match(/\$[\d,]+\.\d{2}/);
      const assessedValue = assessedMoneyMatch
        ? assessedMoneyMatch[0]
        : '';

      const salePriceStr = extractAmountAfter(blockText, 'Amount:');

      const parcelRaw = extractBetween(blockText, 'Parcel ID:', [
        'Property Address',
        'Assessed Value',
      ]);
      const parcelId = parcelRaw.split('|')[0].trim();

      const propertyAddress = extractBetween(blockText, 'Property Address:', [
        'Assessed Value',
      ]);

      const auctionDate = extractDate(blockText);

      const row = {
        sourceUrl: url,
        auctionStatus,
        auctionType: auctionType || 'Tax Sale',
        caseNumber,
        parcelId,
        propertyAddress,
        openingBid: openingBidStr,
        salePrice: salePriceStr,
        assessedValue,
        auctionDate,
      };

      if (!validateRow(row)) return;

      const open = parseCurrency(openingBidStr);
      const assess = parseCurrency(assessedValue);
      const salePrice = parseCurrency(salePriceStr);

      let surplusAssessVsSale = null;
      let surplusSaleVsOpen = null;
      if (assess !== null && salePrice !== null) {
        surplusAssessVsSale = assess - salePrice;
      }
      if (salePrice !== null && open !== null) {
        surplusSaleVsOpen = salePrice - open;
      }

      row.surplusAssessVsSale = surplusAssessVsSale;
      row.surplusSaleVsOpen = surplusSaleVsOpen;
      row.meetsMinimumSurplus =
        surplusAssessVsSale !== null && surplusAssessVsSale >= MIN_SURPLUS
          ? 'Yes'
          : 'No';

      const dedupeKey = `${url}|${caseNumber}|${parcelId}`;
      if (seen.has(dedupeKey)) return;
      seen.add(dedupeKey);

      parsedRows.push(row);
    });

    console.log(`📦 Elements: ${relevantElements.length} | SOLD auctions: ${parsedRows.length}`);
    return { relevantElements, parsedRows };
  } catch (err) {
    console.error(`❌ Error on ${url}:`, err.message);
    return { relevantElements: [], parsedRows: [], error: { url, message: err.message } };
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
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-blink-features=AutomationControlled',
      '--ignore-certificate-errors',
    ],
  });

  const allElements = [];
  const allRows = [];
  const errors = [];

  for (const url of urls) {
    const { relevantElements, parsedRows, error } = await inspectAndParse(browser, url);
    allElements.push(...relevantElements);
    allRows.push(...parsedRows);
    if (error) errors.push(error);
  }

  await browser.close();

  // Global dedupe just in case
  const uniqueMap = new Map();
  for (const row of allRows) {
    const key = `${row.sourceUrl}|${row.caseNumber}|${row.parcelId}`;
    if (!uniqueMap.has(key)) uniqueMap.set(key, row);
  }
  const finalRows = [...uniqueMap.values()];

  // Diagnostic summary
  const summary = {
    totalUrls: urls.length,
    totalElements: allElements.length,
    totalRowsRaw: allRows.length,
    totalRowsFinal: finalRows.length,
    errorsCount: errors.length,
    surplusAboveThreshold: finalRows.filter(r => r.meetsMinimumSurplus === 'Yes').length,
    surplusBelowThreshold: finalRows.filter(r => r.meetsMinimumSurplus === 'No').length,
  };

  // Write artifacts
  fs.writeFileSync(OUTPUT_ELEMENTS_FILE, JSON.stringify(allElements, null, 2));
  fs.writeFileSync(OUTPUT_ROWS_FILE, JSON.stringify(finalRows, null, 2));
  fs.writeFileSync(OUTPUT_SUMMARY_FILE, JSON.stringify(summary, null, 2));
  if (errors.length) {
    fs.writeFileSync(OUTPUT_ERRORS_FILE, JSON.stringify(errors, null, 2));
    console.log(`⚠️ Saved ${errors.length} errors → ${OUTPUT_ERRORS_FILE}`);
  }

  console.log(`✅ Saved ${allElements.length} elements → ${OUTPUT_ELEMENTS_FILE}`);
  console.log(`✅ Saved ${finalRows.length} SOLD auctions → ${OUTPUT_ROWS_FILE}`);
  console.log(`📊 Saved summary → ${OUTPUT_SUMMARY_FILE}`);
  console.log('🏁 Done');
})();