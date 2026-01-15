// inspectWebpage.cjs (Stage 2: evaluation + filtration)
// Requires:
// npm install cheerio googleapis

const fs = require('fs');
const cheerio = require('cheerio');
const { google } = require('googleapis');

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = './service-account.json';
const SPREADSHEET_ID = '1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA';
const SHEET_NAME = 'web_tda';
const HEADER_RANGE = 'A1:Z1'; // adjust if needed

const INPUT_ELEMENTS_FILE = 'raw-elements.json';   // from webInspector.cjs
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
// Load header criteria from sheet
// =========================
async function loadHeaderCriteria() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!${HEADER_RANGE}`,
  });
  return (res.data.values && res.data.values[0]) || [];
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
// Auction row builder
// =========================
function buildAuctionRow(elementText) {
  const blockText = elementText.replace(/\s+/g, ' ').trim();
  if (!blockText.includes('Auction Type')) return null;

  const extract = label => {
    const regex = new RegExp(`${label}\\s*:?\\s*([^\\n$]+)`, 'i');
    const m = blockText.match(regex);
    return m ? m[1].trim() : '';
  };

  const auctionStatus =
    blockText.includes('Redeemed')
      ? 'Redeemed'
      : blockText.includes('Auction Sold')
      ? 'Sold'
      : 'Active';

  const openingBidMatch = blockText.match(/\$[\d,]+\.\d{2}/);
  const openingBid = openingBidMatch ? openingBidMatch[0] : '';

  const assessedValueMatch = blockText.match(/Assessed Value:\s*\$[\d,]+\.\d{2}/i);
  const assessedValue = assessedValueMatch
    ? assessedValueMatch[0].replace(/Assessed Value:/i, '').trim()
    : '';

  const parcelIdMatch = blockText.match(/Parcel\s*ID\s*:?[\s\S]*?(\d{6,})/i);
  const parcelId = parcelIdMatch ? parcelIdMatch[1].trim() : '';

  if (!parcelId || !openingBid) return null;

  const open = parseCurrency(openingBid);
  const assess = parseCurrency(assessedValue);
  const surplus = open !== null && assess !== null ? assess - open : null;

  return {
    auctionStatus,
    auctionType: extract('Auction Type'),
    caseNumber: extract('Case #'),
    parcelId,
    propertyAddress: extract('Property Address'),
    openingBid,
    assessedValue,
    surplus,
    meetsMinimumSurplus: surplus !== null && surplus >= MIN_SURPLUS ? 'Yes' : 'No',
  };
}

// =========================
// MAIN
// =========================
(async () => {
  console.log('📥 Loading header criteria...');
  const headers = await loadHeaderCriteria();
  console.log(`✅ Loaded ${headers.length} headers from sheet`);

  console.log('📥 Reading raw elements...');
  const rawData = fs.readFileSync(INPUT_ELEMENTS_FILE, 'utf8');
  const elements = JSON.parse(rawData);

  const parsedRows = [];
  const errors = [];

  for (const el of elements) {
    try {
      const row = buildAuctionRow(el.text);
      if (row) {
        // Apply header criteria: only include fields present in sheet headers
        const filteredRow = {};
        for (const header of headers) {
          const key = header.trim().toLowerCase();
          if (row.hasOwnProperty(key)) {
            filteredRow[key] = row[key];
          }
        }
        parsedRows.push({ sourceUrl: el.sourceUrl, ...row });
      }
    } catch (err) {
      errors.push({ sourceUrl: el.sourceUrl, message: err.message });
    }
  }

  // Summary artifact
  const summary = {
    totalElements: elements.length,
    totalRowsFinal: parsedRows.length,
    errorsCount: errors.length,
    surplusAboveThreshold: parsedRows.filter(r => r.meetsMinimumSurplus === 'Yes').length,
    surplusBelowThreshold: parsedRows.filter(r => r.meetsMinimumSurplus === 'No').length,
    blanks: {
      openingBidBlank: parsedRows.filter(r => !r.openingBid).length,
      assessedValueBlank: parsedRows.filter(r => !r.assessedValue).length,
      parcelIdBlank: parsedRows.filter(r => !r.parcelId).length,
    },
  };

  // Write artifacts
  fs.writeFileSync(OUTPUT_ROWS_FILE, JSON.stringify(parsedRows, null, 2));
  fs.writeFileSync(OUTPUT_SUMMARY_FILE, JSON.stringify(summary, null, 2));
  if (errors.length) {
    fs.writeFileSync(OUTPUT_ERRORS_FILE, JSON.stringify(errors, null, 2));
    console.log(`⚠️ Saved ${errors.length} errors → ${OUTPUT_ERRORS_FILE}`);
  }

  console.log(`✅ Saved ${parsedRows.length} parsed auctions → ${OUTPUT_ROWS_FILE}`);
  console.log(`📊 Saved summary → ${OUTPUT_SUMMARY_FILE}`);
  console.log('🏁 Done');
})();