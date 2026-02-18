// mappingScraper.cjs
// Accepts parsed-auctions.json as either JSON array or NDJSON (one JSON object per line)
// Maps parsed auctions → Google Sheets (batched) and writes mapped-output.json + mapping-anomalies.json

const fs = require('fs');
const { google } = require('googleapis');

const SERVICE_ACCOUNT_FILE = process.env.SERVICE_ACCOUNT_FILE || './service-account.json';
const SPREADSHEET_ID = process.env.SPREADSHEET_ID || '1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA';
const SHEET_NAME_URLS = process.env.SHEET_NAME_URLS || 'web_tda';
const SHEET_NAME_RAW = process.env.SHEET_NAME_RAW || 'raw_main';
const INPUT_FILE = process.argv[2] || process.env.INPUT_FILE || 'parsed-auctions.json';
const OUTPUT_FILE = process.env.OUTPUT_FILE || 'mapped-output.json';
const ANOMALY_FILE = process.env.ANOMALY_FILE || 'mapping-anomalies.json';

const MIN_SURPLUS = parseFloat(process.env.MIN_SURPLUS || '25000');
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '100', 10);
const RETRY_MAX = parseInt(process.env.RETRY_MAX || '5', 10);
const RETRY_BASE_MS = parseInt(process.env.RETRY_BASE_MS || '500', 10);
const ENABLE_SHEET_IDEMPOTENCY = (process.env.ENABLE_SHEET_IDEMPOTENCY || 'false').toLowerCase() === 'true';

const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ['https://www.googleapis.com/auth/spreadsheets'],
});
const sheets = google.sheets({ version: 'v4', auth });

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
  if (!str) return null;
  const n = parseFloat(String(str).replace(/[^0-9.-]/g, ''));
  return isNaN(n) ? null : n;
}

async function getUrlMapping() {
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
    console.warn('Could not fetch URL mapping from sheet:', err.message || err);
    return {};
  }
}

async function withRetry(fn, retries = RETRY_MAX, baseMs = RETRY_BASE_MS) {
  for (let i = 0; i <= retries; i++) {
    try { return await fn(); }
    catch (err) {
      const retriable = err && (err.code === 429 || (err.code >= 500 && err.code < 600));
      if (!retriable || i === retries) throw err;
      const wait = baseMs * Math.pow(2, i);
      await new Promise(r => setTimeout(r, wait));
    }
  }
}

async function fetchExistingKeys() {
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
    return keys;
  } catch (err) {
    return new Set();
  }
}

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
  const estimatedSurplus = (sale !== null && open !== null) ? (open - sale) : (sale !== null ? null : null);

  if (sale === null && mapped['Sale Finalized (Yes/No)'] === 'Yes') {
    anomalies.push({ type: 'MissingSalePrice', parcelId: raw.parcelId, sourceUrl: raw.sourceUrl });
  }

  mapped['Estimated Surplus'] = estimatedSurplus !== null ? String(estimatedSurplus) : '';
  mapped['Final Estimated Surplus to Owner'] = estimatedSurplus !== null ? String(estimatedSurplus) : '';

  const meetsMinimum = estimatedSurplus !== null && estimatedSurplus >= MIN_SURPLUS;
  mapped['Meets Minimum Surplus? (Yes/No)'] = yn(meetsMinimum ? 'Yes' : 'No');
  mapped['Deal Viable? (Yes/No)'] = meetsMinimum ? 'Yes' : 'No';

  mapped['Ownership Deed Collected? (Yes/No)'] = 'No';
  mapped['Foreclosure Deed Collected? (Yes/No)'] = 'No';
  mapped['Proof of Sale Collected? (Yes/No)'] = 'No';
  mapped['Debt Search Screenshot Collected? (Yes/No)'] = 'No';
  mapped['Tax Assessor Page Collected? (Yes/No)'] = 'No';
  mapped['File Complete? (Yes/No)'] = 'No';
  mapped['File Submitted? (Yes/No)'] = 'No';

  return mapped;
}

async function appendRowsBatched(rows) {
  if (!rows.length) return;
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
    await new Promise(r => setTimeout(r, 200));
  }
}

function loadParsedInput(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8').trim();
  if (!raw) return [];
  // If file starts with '[' treat as JSON array
  if (raw[0] === '[') {
    try { return JSON.parse(raw); } catch (e) { /* fallthrough */ }
  }
  // Otherwise treat as NDJSON (one JSON object per line)
  const lines = raw.split(/\r?\n/).filter(Boolean);
  const out = [];
  for (const line of lines) {
    try { out.push(JSON.parse(line)); } catch (e) { /* skip invalid lines */ }
  }
  return out;
}

(async () => {
  try {
    if (!fs.existsSync(INPUT_FILE)) {
      console.error('Input file not found:', INPUT_FILE);
      process.exit(1);
    }

    const rawData = loadParsedInput(INPUT_FILE);
    console.log(`📦 Loaded ${rawData.length} parsed rows from ${INPUT_FILE}`);

    const urlMapping = await getUrlMapping();
    console.log(`🌐 Fetched ${Object.keys(urlMapping).length} URL → County/State mappings`);

    const anomalies = [];
    const uniqueMap = new Map();
    const existingKeys = ENABLE_SHEET_IDEMPOTENCY ? await fetchExistingKeys() : new Set();

    rawData.forEach(raw => {
      const baseUrl = (raw.sourceUrl || '').split('&page=')[0];
      const key = `${baseUrl}|${raw.caseNumber || ''}|${raw.parcelId || ''}`;
      const sheetKey = `${(raw.parcelId || '').toString().trim()}|${(raw.caseNumber || '').toString().trim()}|${(raw.auctionDate || raw.date || '').toString().trim()}`;

      if (uniqueMap.has(key)) return;
      if (existingKeys.has(sheetKey)) return;

      const mapped = mapRow(raw, urlMapping, anomalies);
      if (mapped) uniqueMap.set(key, mapped);
    });

    const mappedRows = [...uniqueMap.values()];
    fs.writeFileSync(OUTPUT_FILE, JSON.stringify(mappedRows, null, 2));
    console.log(`💾 Saved mapped rows → ${OUTPUT_FILE}`);

    if (anomalies.length) {
      fs.writeFileSync(ANOMALY_FILE, JSON.stringify(anomalies, null, 2));
      console.log(`⚠️ Saved ${anomalies.length} anomalies → ${ANOMALY_FILE}`);
    }

    if (mappedRows.length && ENABLE_SHEET_IDEMPOTENCY) {
      await appendRowsBatched(mappedRows);
      console.log('✅ Appended mapped rows to sheet');
    } else if (mappedRows.length) {
      console.log('ℹ️ Mapped rows present but idempotency disabled; not appending to sheet automatically.');
    } else {
      console.log('ℹ️ No mapped rows to append after dedupe/idempotency checks.');
    }

    console.log('🏁 Done.');
  } catch (err) {
    console.error('Fatal error in mappingScraper:', err && err.message ? err.message : err);
    process.exit(1);
  }
})();