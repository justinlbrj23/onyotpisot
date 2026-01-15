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
// Helpers
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

function extractDateFlexible(text) {
  const patterns = [
    /Date\/Time\s*:?\s*([0-9]{2}\/[0-9]{2}\/[0-9]{4}\s+[0-9]{1,2}:[0-9]{2}\s*(?:AM|PM)\s*\w*)/i,
    /Auction Date\s*:?\s*([0-9]{2}\/[0-9]{2}\/[0-9]{4})/i,
    /Sale Date\s*:?\s*([0-9]{2}\/[0-9]{2}\/[0-9]{4})/i,
    /Date Sold\s*:?\s*([0-9]{2}\/[0-9]{2}\/[0-9]{4})/i,
    /([0-9]{4}-[0-9]{2}-[0-9]{2}\s+[0-9]{2}:[0-9]{2}(?::[0-9]{2})?\s*(?:AM|PM)?)/i,
  ];
  for (const re of patterns) {
    const m = text.match(re);
    if (m && m[1]) return m[1].trim();
  }
  return '';
}

function extractAmountAfter(text, label) {
  const regex = new RegExp(label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\s*:?' + '\\s*\\$[\\d,]+(?:\\.\\d{2})?', 'i');
  const m = text.match(regex);
  if (!m) return '';
  const moneyMatch = m[0].match(/\$[\d,]+(?:\.\d{2})?/);
  return moneyMatch ? moneyMatch[0] : '';
}

function extractCurrencyNearLabels(text, labels, window = 60) {
  const lower = text.toLowerCase();
  for (const label of labels) {
    const idx = lower.indexOf(label.toLowerCase());
    if (idx !== -1) {
      const slice = text.slice(idx, Math.min(text.length, idx + window));
      const m = slice.match(/\$[\d,]+(?:\.\d{2})?/);
      if (m) return m[0];
    }
  }
  return '';
}

function extractSalePrice(text) {
  const labels = [
    'Amount',
    'Sale Price',
    'Sold Price',
    'Sold Amount',
    'Sold Amount/Sold Price',
    'Winning Bid',
    'Winning Bid Amount',
    'Sold For',
    'Final Bid',
    'Final Sale Price',
    'Winning Amount',
    'Winning Offer',
  ];
  for (const label of labels) {
    const v = extractAmountAfter(text, label);
    if (v) return v;
  }
  const near = extractCurrencyNearLabels(text, labels, 80);
  if (near) return near;
  const allMoney = [...text.matchAll(/\$[\d,]+(?:\.\d{2})?/g)].map(m => m[0]);
  if (allMoney.length) {
    const sorted = allMoney.map(s => ({ s, n: parseCurrency(s) }))
      .filter(x => x.n !== null)
      .sort((a, b) => b.n - a.n);
    return sorted.length ? sorted[0].s : '';
  }
  return '';
}

function validateRow(row) {
  return (
    row.caseNumber &&
    row.parcelId &&
    row.openingBid &&
    row.salePrice &&
    row.assessedValue
  );
}

function buildLabelValueMap($container) {
  const map = {};
  const textNodes = [];
  $container.find('*').each((_, el) => {
    const t = $container.find(el).text().replace(/\s+/g, ' ').trim();
    if (t) textNodes.push(t);
  });
  for (const t of textNodes) {
    const parts = t.split(/[:|-]\s*/);
    if (parts.length >= 2) {
      const label = parts[0].trim().toLowerCase();
      const value = parts.slice(1).join(':').trim();
      if (label && value) map[label] = value;
    }
  }
  return { map, text: textNodes.join(' | ') };
}

// =========================
// Inspect + Parse Page (SOLD only)
// =========================
async function inspectAndParse(page, url) {
  try {
    console.log(`🌐 Visiting ${url}`);
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 120000 });
    await new Promise(r => setTimeout(r, 5000));

    const html = await page.content();
    if (html.includes('403 Forbidden') || html.includes('Access Denied')) {
      throw new Error('Blocked by target website (403)');
    }

    const $ = cheerio.load(html);
    const relevantElements = [];
    const auctionTextRegex = /(\$\d{1,3}(,\d{3})+)|(\bAPN\b)|(\bParcel\b)|(\bAuction\b)|(\bCase\b)|(\bWinning Bid\b)|(\bSale Price\b)/i;

    $('*').each((_, el) => {
      const tag = el.tagName;
      const text = $(el).text().replace(/\s+/g, ' ').trim();
      const attrs = el.attribs || {};
      if (text && auctionTextRegex.test(text)) {
        relevantElements.push({ sourceUrl: url, tag, text, attrs });
      }
    });

    const parsedRows = [];
    const seen = new Set();

    $('div').each((_, container) => {
      const $container = $(container);
      const blockText = $container.text().replace(/\s+/g, ' ').trim();
      if (!/Auction Sold/i.test(blockText)) return;

      const { map: kv, text: joinedText } = buildLabelValueMap($container);
      const auctionStatus = 'Sold';

      const auctionType =
        extractBetween(blockText, 'Auction Type:', ['Case #', 'Certificate', 'Opening Bid']) ||
        (kv['auction type'] || '');

      const rawCase =
        extractBetween(blockText, 'Case #:', ['Certificate', 'Opening Bid']) ||
        (kv['case #'] || kv['case'] || '');
      const caseNumber = rawCase.split(/\s+/)[0].trim();

      const openingBidStr =
        extractAmountAfter(blockText, 'Opening Bid') ||
        extractAmountAfter(joinedText, 'Opening Bid') ||
        (kv['opening bid'] && kv['opening bid'].match(/\$[\d,]+(?:\.\d{2})?/)?.[0]) ||
        '';

      const assessedValueStr =
        extractBetween(blockText, 'Assessed Value:', []) ||
        (kv['assessed value'] || '');
      const assessedMoneyMatch = assessedValueStr.match(/\$[\d,]+(?:\.\d{2})?/);
      const assessedValue = assessedMoneyMatch ? assessedMoneyMatch[0] : '';

      const salePriceStr =
        extractSalePrice(blockText) ||
        extractSalePrice(joinedText) ||
        (kv['sale price'] && kv['sale price'].match(/\$[\d,]+(?:\.\d{2})?/)?.[0]) ||
        (kv['sold price'] && kv['sold price'].match(/\$[\d,]+(?:\.\d{2})?/)?.[0]) ||
        (kv['winning bid'] && kv['winning bid'].match(/\$[\d,]+(?:\.\d{2})?/)?.[0]) ||
        (kv['sold amount'] && kv['sold amount'].match(/\$[\d,]+(?:\.\d{2})?/)?.[0]) ||
        '';

      const parcelRaw =
        extractBetween(blockText, 'Parcel ID:', ['Property Address', 'Assessed Value']) ||
        (kv['parcel id'] || kv['apn'] || '');
      const parcelId = parcelRaw.split('|')[0].trim();

      const propertyAddress =
        extractBetween(blockText, 'Property Address:', ['Assessed Value']) ||
        (kv['property address'] || kv['address'] || '');

      const auctionDate =
        extractDateFlexible(blockText) ||
        extractDateFlexible(joinedText) ||
        (kv['auction date'] || kv['date sold'] || kv['sale date'] || kv['date'] || '');

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

      row.surplusAssessVsSale =
        assess !== null && salePrice !== null ? assess - salePrice : null;

      row.surplusSaleVsOpen =
        salePrice !== null && open !== null ? salePrice - open : null;

      row.meetsMinimumSurplus =
        row.surplusAssessVsSale !== null &&
        row.surplusAssessVsSale >= MIN_SURPLUS
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
  }
}

// =========================
// Scrape all pages (pagination)
// =========================
async function scrapeAllPages(browser, startUrl) {
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  const allElements = [];
  const allRows = [];
  const errors = [];

  let currentUrl = startUrl;
  let pageIndex = 1;

  while (true) {
    const { relevantElements, parsedRows, error } = await inspectAndParse(page, currentUrl);
    allElements.push(...relevantElements);
    allRows.push(...parsedRows);
    if (error) errors.push(error);

    // Detect "Next Page" button
    const nextButton = await page.$('a[aria-label="Next"], a.pagination-next, button.next');
    if (!nextButton) {
      console.log("⛔ No more pages");
      break;
    }

    await Promise.all([
      nextButton.click(),
      page.waitForNavigation({ waitUntil: "networkidle2" })
    ]);

    currentUrl = page.url();
    pageIndex++;
  }

  await page.close();
  return { allElements, allRows, errors };
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
    const result = await scrapeAllPages(browser, url);
    allElements.push(...result.allElements);
    allRows.push(...result.allRows);
    errors.push(...result.errors);
  }

  await browser.close();

  // Global dedupe
  const uniqueMap = new Map();
  for (const row of allRows) {
    const key = `${row.sourceUrl}|${row.caseNumber}|${row.parcelId}`;
    if (!uniqueMap.has(key)) uniqueMap.set(key, row);
  }
  const finalRows = [...uniqueMap.values()];

  // Summary artifact
  const summary = {
    totalUrls: urls.length,
    totalElements: allElements.length,
    totalRowsRaw: allRows.length,
    totalRowsFinal: finalRows.length,
    errorsCount: errors.length,
    surplusAboveThreshold: finalRows.filter(r => r.meetsMinimumSurplus === 'Yes').length,
    surplusBelowThreshold: finalRows.filter(r => r.meetsMinimumSurplus === 'No').length,
    blanks: {
      salePriceBlank: finalRows.filter(r => !r.salePrice).length,
      auctionDateBlank: finalRows.filter(r => !r.auctionDate).length,
    },
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