// webInspector.cjs (RealForeclose SOLD parser - improved block splitting)
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
// Extract helpers (regex-based, RealForeclose-safe)
// =========================
function extract(regex, text) {
  const m = text.match(regex);
  return m ? m[1].trim() : '';
}

function extractMoney(regex, text) {
  const m = text.match(regex);
  return m ? m[1] : '';
}

// =========================
// Split page into auction blocks
// =========================
function splitIntoAuctionBlocks(fullText) {
  const blocks = fullText
    .split(/Auction Status/i)
    .map(b => b.trim())
    .filter(b => b.length > 100);

  return blocks.map(b => "Auction Status " + b);
}

// =========================
// Parse single block (SOLD only)
// =========================
function parseAuctionBlock(block, sourceUrl) {
  if (!/Auction Sold/i.test(block)) return null;

  const caseNumber = extract(/Cause Number:\s*([A-Z0-9\-]+)/i, block);

  const parcelId = extract(/Account Number:\s*([0-9A-Z]+)/i, block);

  const propertyAddress = extract(
    /Property Address:\s*([^A]+?\d{5}(?:-\d{4})?)/i,
    block
  );

  const assessedValue = extractMoney(
    /Adjudged Value:\s*(\$[\d,]+(?:\.\d{2})?)/i,
    block
  );

  const openingBid = extractMoney(
    /Est\.?\s*Min\.?\s*Bid:\s*(\$[\d,]+(?:\.\d{2})?)/i,
    block
  );

  const salePrice = extractMoney(
    /Amount\s*(\$[\d,]+(?:\.\d{2})?)/i,
    block
  );

  const auctionDate = extract(
    /Auction Sold\s*([0-9\/:\sAMPCT]+)/i,
    block
  );

  if (!caseNumber || !parcelId || !openingBid || !salePrice || !assessedValue)
    return null;

  const open = parseCurrency(openingBid);
  const assess = parseCurrency(assessedValue);
  const sale = parseCurrency(salePrice);

  const surplusAssessVsSale =
    assess !== null && sale !== null ? assess - sale : null;

  const surplusSaleVsOpen =
    sale !== null && open !== null ? sale - open : null;

  return {
    sourceUrl,
    auctionStatus: 'Sold',
    auctionType: 'Tax Sale',
    caseNumber,
    parcelId,
    propertyAddress,
    openingBid,
    salePrice,
    assessedValue,
    auctionDate,
    surplusAssessVsSale,
    surplusSaleVsOpen,
    meetsMinimumSurplus:
      surplusAssessVsSale !== null &&
      surplusAssessVsSale >= MIN_SURPLUS
        ? 'Yes'
        : 'No',
  };
}

// =========================
// Inspect + Parse
// =========================
async function inspectAndParse(browser, url) {
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  try {
    console.log(`🌐 Visiting ${url}`);
    await page.goto(url, { waitUntil: 'networkidle2' });
    await new Promise(r => setTimeout(r, 8000));

    const html = await page.content();
    const $ = cheerio.load(html);

    const fullText = $('body').text().replace(/\s+/g, ' ').trim();

    const blocks = splitIntoAuctionBlocks(fullText);

    const rows = [];
    for (const block of blocks) {
      const parsed = parseAuctionBlock(block, url);
      if (parsed) rows.push(parsed);
    }

    return { relevantElements: [], parsedRows: rows };
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
    ],
  });

  const allRows = [];
  const errors = [];

  for (const url of urls) {
    const { parsedRows, error } = await inspectAndParse(browser, url);
    allRows.push(...parsedRows);
    if (error) errors.push(error);
  }

  await browser.close();

  // Global dedupe
  const uniqueMap = new Map();
  for (const row of allRows) {
    const key = `${row.sourceUrl}|${row.caseNumber}|${row.parcelId}`;
    if (!uniqueMap.has(key)) uniqueMap.set(key, row);
  }

  const finalRows = [...uniqueMap.values()];

  const summary = {
    totalUrls: urls.length,
    totalRowsFinal: finalRows.length,
    errorsCount: errors.length,
    surplusAboveThreshold: finalRows.filter(r => r.meetsMinimumSurplus === 'Yes').length,
  };

  fs.writeFileSync(OUTPUT_ROWS_FILE, JSON.stringify(finalRows, null, 2));
  fs.writeFileSync(OUTPUT_SUMMARY_FILE, JSON.stringify(summary, null, 2));

  if (errors.length) {
    fs.writeFileSync(OUTPUT_ERRORS_FILE, JSON.stringify(errors, null, 2));
  }

  console.log(`✅ Saved ${finalRows.length} SOLD auctions → ${OUTPUT_ROWS_FILE}`);
  console.log('🏁 Done');
})();