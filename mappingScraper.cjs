#!/usr/bin/env node
// mappingScraper.cjs
// Accepts parsed-auctions.json as NDJSON or JSON array and maps to TSSF headers.
// Features:
// - Streaming NDJSON loader (memory efficient)
// - Configurable surplus formula (SURPLUS_FORMULA)
// - Dry-run mode (DRY_RUN=true) to avoid writing to Google Sheets
// - Idempotency via existing sheet keys (optional)
// - Batched appends with retries and exponential backoff

const fs = require('fs');
const readline = require('readline');
const { google } = require('googleapis');

// =========================
// CONFIG (env + args)
const SERVICE_ACCOUNT_FILE = process.env.SERVICE_ACCOUNT_FILE || './service-account.json';
const SPREADSHEET_ID = process.env.SPREADSHEET_ID || '';
const SHEET_NAME_URLS = process.env.SHEET_NAME_URLS || 'web_tda';
const SHEET_NAME_RAW = process.env.SHEET_NAME_RAW || 'raw_main';
const INPUT_FILE = process.argv[2] || process.env.INPUT_FILE || 'parsed-auctions.json';
const OUTPUT_FILE = process.env.OUTPUT_FILE || 'mapped-output.json';
const ANOMALY_FILE = process.env.ANOMALY_FILE || 'mapping-anomalies.json';

const MIN_SURPLUS = parseFloat(process.env.MIN_SURPLUS || '25000');
const SURPLUS_FORMULA = process.env.SURPLUS_FORMULA || 'sale-open'; // options: sale-open, open-sale
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '100', 10);
const RETRY_MAX = parseInt(process.env.RETRY_MAX || '5', 10);
const RETRY_BASE_MS = parseInt(process.env.RETRY_BASE_MS || '500', 10);
const ENABLE_SHEET_IDEMPOTENCY = (process.env.ENABLE_SHEET_IDEMPOTENCY || 'false').toLowerCase() === 'true';
const DRY_RUN = (process.env.DRY_RUN || 'false').toLowerCase() === 'true';

// =========================
// Google Sheets client (lazy)
let sheets = null;
let SHOULD_WRITE_SHEETS = Boolean(SPREADSHEET_ID && SPREADSHEET_ID.trim()) && !DRY_RUN;
if (!SHOULD_WRITE_SHEETS) {
  console.warn('⚠️ SPREADSHEET_ID not set or DRY_RUN enabled — skipping Google Sheets writes.');
} else {
  try {
    const auth = new google.auth.GoogleAuth({
      keyFile: SERVICE_ACCOUNT_FILE,
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    sheets = google.sheets({ version: 'v4', auth });
  } catch (err) {
    console.warn('⚠️ Google Sheets client initialization failed; sheet writes will be skipped.', err && err.message ? err.message : err);
    SHOULD_WRITE_SHEETS = false;
  }
}

// =========================
// HEADERS
const HEADERS = [
  "State","County","Property Address","City","ZIP Code","Parcel / APN Number","Case Number",
  "Auction Date","Sale Finalized (Yes/No)","Sale Price","Opening / Minimum Bid","Estimated Surplus","Meets Minimum Surplus? (Yes/No)",
  "Last Owner Name (as on Deed)","Additional Owner(s)","Ownership Type","Deed Type","Owner Deed Recording Date","Owner Deed Instrument #",
  "Mortgage Lender Name","Mortgage Amount","Mortgage Recording Date","Mortgage Satisfied? (Yes/No)","Mortgage Release Recording #","Mortgage Still Owed Amount",
  "Lien / Judgment Type","Creditor Name","Lien Amount","Lien Recording Date","Lien Expired? (Yes/No)","Lien Satisfied? (Yes/No)",
  "Total Open Debt","Final Estimated Surplus to Owner","Deal Viable? (Yes/No)",
  "Ownership Deed Collected? (Yes/No)","Foreclosure Deed Collected? (Yes/No)","Proof of Sale Collected? (Yes/No)","Debt Search Screenshot Collected? (Yes/No)","Tax Assessor Page Collected? (Yes/No)","File Complete? (Yes/No)",
  "File Submitted? (Yes/No)","Submission Date","Accepted / Rejected","Kickback Reason","Researcher Name",
];

// =========================
// Utilities
function yn(val) {
  if (val === true || val === 'Yes') return 'Yes';
  if (val === false || val === 'No') return 'No';
  if (typeof val === 'string') {
    const v = val.trim().toLowerCase();
    if (v === 'yes') return 'Yes';
    if (v === 'no') return 'No';
  }
  return '';
}

function parseCurrency(str) {
  if (!str && str !== 0) return null;
  const n = parseFloat(String(str).replace(/[^0-9.-]/g, ''));
  return isNaN(n) ? null : n;
}

async function withRetry(fn, retries = RETRY_MAX, baseMs = RETRY_BASE_MS) {
  for (let i = 0; i <= retries; i++) {
    try { return await fn(); }
    catch (err) {
      const retriable = err && (err.code === 429 || (err.code >= 500 && err.code < 600));
      if (!retriable || i === retries) throw err;
      const wait = baseMs * Math.pow(2, i);
      console.warn(`Retry ${i + 1}/${retries} after ${wait}ms due to error: ${err && err.message ? err.message : err}`);
      await new Promise(r => setTimeout(r, wait));
    }
  }
}

// =========================
// URL mapping (sheet)
async function getUrlMapping() {
  if (!SHOULD_WRITE_SHEETS) return {};
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME_URLS}!A2:C`,
    });
    const rows = res.data.values || [];
    const mapping = {};
    rows.forEach(([county, state, url]) => {
      if (url) mapping[url.trim()] = { county: county || '', state: state || '' };
    });
    return mapping;
  } catch (err) {
    console.warn('⚠️ Could not fetch URL mapping from sheet:', err && err.message ? err.message : err);
    return {};
  }
}

// =========================
// Fetch existing keys for idempotency
async function fetchExistingKeys() {
  if (!SHOULD_WRITE_SHEETS || !ENABLE_SHEET_IDEMPOTENCY) return new Set();
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME_RAW}!A2:Z`,
    });
    const rows = res.data.values || [];
    const keys = new Set();
    for (const r of rows) {
      const parcel = r[5] || '';
      const caseNum = r[6] || '';
      const date = r[7] || '';
      const key = `${(parcel || '').toString().trim()}|${(caseNum || '').toString().trim()}|${(date || '').toString().trim()}`;
      if (key !== '||') keys.add(key);
    }
    console.log(`🔎 Fetched ${keys.size} existing keys from sheet for idempotency`);
    return keys;
  } catch (err) {
    console.warn('⚠️ Could not fetch existing sheet rows for idempotency:', err && err.message ? err.message : err);
    return new Set();
  }
}

// =========================
// Mapping logic
function mapRow(raw, urlMapping, anomalies) {
  if (!raw) return null;
  const mapped = {};
  HEADERS.forEach(h => (mapped[h] = ''));

  const baseUrl = (raw.sourceUrl || '').split('&page=')[0];
  const geo = urlMapping[baseUrl] || { county: '', state: '' };

  mapped['State'] = geo.state;
  mapped['County'] = geo.county;
  mapped['Property Address'] = raw.propertyAddress || '';
  mapped['Parcel / APN Number'] = raw.parcelId || '';
  mapped['Case Number'] = raw.caseNumber || '';
  mapped['Auction Date'] = raw.auctionDate || raw.date || '';
  mapped['Sale Finalized (Yes/No)'] = raw.auctionStatus === 'Sold' ? 'Yes' : 'No';

  const salePrice = raw.salePrice || raw.amount || '';
  const openingBid = raw.openingBid || '';

  mapped['Sale Price'] = salePrice;
  mapped['Opening / Minimum Bid'] = openingBid;

  const sale = parseCurrency(salePrice);
  const open = parseCurrency(openingBid);

  // Configurable surplus formula
  let estimatedSurplus = null;
  if (sale !== null && open !== null) {
    if (SURPLUS_FORMULA === 'sale-open') estimatedSurplus = sale - open;
    else if (SURPLUS_FORMULA === 'open-sale') estimatedSurplus = open - sale;
    else estimatedSurplus = sale - open;
  }

  if (sale === null && mapped['Sale Finalized (Yes/No)'] === 'Yes') {
    if (Array.isArray(anomalies)) anomalies.push({
      type: 'MissingSalePrice',
      parcelId: raw.parcelId,
      sourceUrl: raw.sourceUrl
    });
  }

  mapped['Estimated Surplus'] = estimatedSurplus !== null ? String(estimatedSurplus) : '';
  mapped['Final Estimated Surplus to Owner'] = estimatedSurplus !== null ? String(estimatedSurplus) : '';

  const meetsMinimum = estimatedSurplus !== null && estimatedSurplus >= MIN_SURPLUS;
  mapped['Meets Minimum Surplus? (Yes/No)'] = yn(meetsMinimum ? 'Yes' : 'No');
  mapped['Deal Viable? (Yes/No)'] = meetsMinimum ? 'Yes' : 'No';

  // default flags
  mapped['Ownership Deed Collected? (Yes/No)'] = 'No';
  mapped['Foreclosure Deed Collected? (Yes/No)'] = 'No';
  mapped['Proof of Sale Collected? (Yes/No)'] = 'No';
  mapped['Debt Search Screenshot Collected? (Yes/No)'] = 'No';
  mapped['Tax Assessor Page Collected? (Yes/No)'] = 'No';
  mapped['File Complete? (Yes/No)'] = 'No';
  mapped['File Submitted? (Yes/No)'] = 'No';

  return mapped;
}

// =========================
// Append rows to sheet in batches
async function appendRowsBatched(rows) {
  if (!rows.length) return;
  if (!SHOULD_WRITE_SHEETS) {
    console.log(`ℹ️ DRY_RUN or missing SPREADSHEET_ID — skipping append of ${rows.length} rows.`);
    return;
  }

  const batches = [];
  for (let i = 0; i < rows.length; i += BATCH_SIZE) batches.push(rows.slice(i, i + BATCH_SIZE));

  for (let i = 0; i < batches.length; i++) {
    const values = batches[i].map(row => HEADERS.map(h => row[h] || ''));
    await withRetry(() => sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: SHEET_NAME_RAW,
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values },
    }));
    console.log(`✅ Appended batch ${i + 1}/${batches.length} (${values.length} rows)`);
    await new Promise(r => setTimeout(r, 200));
  }
}

// =========================
// Streaming NDJSON loader (also accepts JSON array)
async function loadParsedInputStream(filePath, sampleLimit = Infinity) {
  const stat = fs.statSync(filePath);
  if (stat.size === 0) return [];

  const firstChunk = fs.readFileSync(filePath, { encoding: 'utf8', flag: 'r' , length: 2 }).trim();
  // If file starts with '[' treat as JSON array (load fully)
  const firstNonWs = fs.readFileSync(filePath, 'utf8').trimLeft()[0];
  if (firstNonWs === '[') {
    try {
      const arr = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      return Array.isArray(arr) ? arr : [];
    } catch (e) {
      console.warn('⚠️ Failed to parse JSON array input; falling back to NDJSON streaming.');
    }
  }

  // NDJSON streaming
  const out = [];
  const rl = readline.createInterface({ input: fs.createReadStream(filePath), crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line || !line.trim()) continue;
    try {
      const obj = JSON.parse(line);
      out.push(obj);
      if (out.length >= sampleLimit) break;
    } catch (e) {
      // skip invalid lines but log occasionally
      if (Math.random() < 0.01) console.warn('⚠️ Skipping invalid JSON line in NDJSON input');
    }
  }
  return out;
}

// =========================
// MAIN
(async () => {
  try {
    if (!fs.existsSync(INPUT_FILE)) {
      console.error('❌ Input file not found:', INPUT_FILE);
      process.exit(1);
    }

    console.log(`📦 Loading parsed input from ${INPUT_FILE} (streaming) ...`);
    // For full runs, stream everything; for very large files you can change behavior to process line-by-line
    const rawData = await loadParsedInputStream(INPUT_FILE);
    console.log(`📦 Loaded ${rawData.length} parsed rows from ${INPUT_FILE}`);

    const urlMapping = await getUrlMapping();
    console.log(`🌐 Fetched ${Object.keys(urlMapping).length} URL → County/State mappings`);

    const anomalies = [];
    const uniqueMap = new Map();

    const existingKeys = ENABLE_SHEET_IDEMPOTENCY ? await fetchExistingKeys() : new Set();

    let skippedByIdempotency = 0;
    let duplicates = 0;
    let mappedCount = 0;

    for (const raw of rawData) {
      const baseUrl = (raw.sourceUrl || '').split('&page=')[0];
      const key = `${baseUrl}|${raw.caseNumber || ''}|${raw.parcelId || ''}`;
      const sheetKey = `${(raw.parcelId || '').toString().trim()}|${(raw.caseNumber || '').toString().trim()}|${(raw.auctionDate || raw.date || '').toString().trim()}`;

      if (uniqueMap.has(key)) { duplicates++; continue; }
      if (existingKeys.has(sheetKey)) { skippedByIdempotency++; continue; }

      const mapped = mapRow(raw, urlMapping, anomalies);
      if (mapped) {
        uniqueMap.set(key, mapped);
        mappedCount++;
      }
    }

    const mappedRows = [...uniqueMap.values()];
    fs.writeFileSync(OUTPUT_FILE, JSON.stringify(mappedRows, null, 2));
    console.log(`💾 Saved mapped rows → ${OUTPUT_FILE} (${mappedRows.length} rows)`);

    if (anomalies.length) {
      fs.writeFileSync(ANOMALY_FILE, JSON.stringify(anomalies, null, 2));
      console.log(`⚠️ Saved ${anomalies.length} anomalies → ${ANOMALY_FILE}`);
    }

    console.log(`ℹ️ Stats: mapped=${mappedCount} duplicates=${duplicates} skippedByIdempotency=${skippedByIdempotency} anomalies=${anomalies.length}`);

    if (mappedRows.length && SHOULD_WRITE_SHEETS) {
      await appendRowsBatched(mappedRows);
      console.log('✅ Appended mapped rows to sheet');
    } else if (mappedRows.length && !SHOULD_WRITE_SHEETS) {
      console.log('ℹ️ Mapped rows present but sheet writes disabled (DRY_RUN or missing SPREADSHEET_ID).');
    } else {
      console.log('ℹ️ No mapped rows to append after dedupe/idempotency checks.');
    }

    console.log('🏁 Done.');
    process.exit(0);
  } catch (err) {
    console.error('❌ Fatal error in mappingScraper:', err && err.message ? err.message : err);
    process.exit(1);
  }
})();