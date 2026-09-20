// mappingScraper.cjs
// Maps finalized parsed-auction rows to raw_main.
// Rules:
// - Use only raw.surplus from webInspector.
// - Do not calculate surplus from assessed value or alternate fields.
// - Append finalized/sold rows even when surplus is unavailable.
// Requires: npm install googleapis

const fs = require('fs');
const { google } = require('googleapis');

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = './service-account.json';
const SPREADSHEET_ID = '1DvpL59xxVVihpRVhxKUomCpugHLPeokvjKVWKMPAcnw';
const SHEET_NAME_URLS = 'web_tda';
const SHEET_NAME_RAW = 'raw_main';

const INPUT_FILE = process.argv[2] || 'parsed-auctions.json';
const OUTPUT_FILE = 'mapped-output.json';
const ANOMALY_FILE = 'mapping-anomalies.json';
const MIN_SURPLUS = 25000;

const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ['https://www.googleapis.com/auth/spreadsheets'],
});
const sheets = google.sheets({ version: 'v4', auth });

const HEADERS = [
  'State','County','Property Address','City','ZIP Code','Parcel / APN Number','Case Number',
  'Auction Date','Sale Finalized (Yes/No)','Sale Price','Opening / Minimum Bid','Estimated Surplus',
  'Meets Minimum Surplus? (Yes/No)',
  'Last Owner Name (as on Deed)','Additional Owner(s)','Ownership Type','Deed Type',
  'Owner Deed Recording Date','Owner Deed Instrument #',
  'Mortgage Lender Name','Mortgage Amount','Mortgage Recording Date',
  'Mortgage Satisfied? (Yes/No)','Mortgage Release Recording #','Mortgage Still Owed Amount',
  'Lien / Judgment Type','Creditor Name','Lien Amount','Lien Recording Date',
  'Lien Expired? (Yes/No)','Lien Satisfied? (Yes/No)',
  'Total Open Debt','Final Estimated Surplus to Owner','Deal Viable? (Yes/No)',
  'Ownership Deed Collected? (Yes/No)','Foreclosure Deed Collected? (Yes/No)',
  'Proof of Sale Collected? (Yes/No)','Debt Search Screenshot Collected? (Yes/No)',
  'Tax Assessor Page Collected? (Yes/No)','File Complete? (Yes/No)',
  'File Submitted? (Yes/No)','Submission Date','Accepted / Rejected','Kickback Reason',
  'Researcher Name'
];

function clean(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function cleanCaseNumber(value) {
  return clean(value)
    .replace(/\s*\(.*?\)\s*/g, ' ')
    .replace(/\s+/g, ' ')
    .replace(/^[\s\-._]+|[\s\-._]+$/g, '')
    .trim();
}

function normalizeCaseKey(value) {
  return cleanCaseNumber(value).toUpperCase();
}

function decodeAmp(value) {
  return String(value || '')
    .replace(/&amp;amp;/gi, '&amp;')
    .replace(/&amp;/gi, '&');
}

function parseCurrency(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  const text = String(value).trim();
  if (!text) return null;
  const parsed = Number.parseFloat(text.replace(/[^0-9.-]/g, ''));
  return Number.isFinite(parsed) ? parsed : null;
}

function parseCityZip(value) {
  const result = { city: '', zip: '' };
  const text = clean(value);
  if (!text) return result;

  const match = text.match(/\b(\d{5})(?:-\d{4})?\b/);
  if (!match) return result;

  result.zip = match[1];
  result.city = text
    .slice(0, match.index)
    .replace(/,?\s+[A-Za-z]{2}\s*-?\s*$/, '')
    .replace(/,+$/, '')
    .trim();
  return result;
}

function normalizeBaseUrl(value) {
  const raw = decodeAmp(value).trim();
  if (!raw) return '';

  try {
    const url = new URL(raw);
    const stripNames = new Set([
      'page','pagenum','p','pg','pageno','start','startrow','offset',
      'auctiondate','zmethod'
    ]);

    for (const key of [...url.searchParams.keys()]) {
      if (stripNames.has(key.toLowerCase())) url.searchParams.delete(key);
    }

    const path = url.pathname.replace(/\/+$/, '');
    const query = [...url.searchParams.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, val]) => `${encodeURIComponent(key)}=${encodeURIComponent(val)}`)
      .join('&');

    return `${url.protocol}//${url.hostname}${path}${query ? `?${query}` : ''}`.toLowerCase();
  } catch {
    return raw.replace(/\/+$/, '').toLowerCase();
  }
}

function getOrigin(value) {
  try { return new URL(decodeAmp(value)).origin.toLowerCase(); }
  catch { return ''; }
}

function inferCountyStateFromHost(hostname) {
  const result = { county: '', state: '' };
  const parts = String(hostname || '').toLowerCase().split('.');
  if (parts.length < 3) return result;

  const stateMap = {
    alabama:'AL',alaska:'AK',arizona:'AZ',arkansas:'AR',california:'CA',colorado:'CO',
    connecticut:'CT',delaware:'DE',florida:'FL',georgia:'GA',hawaii:'HI',idaho:'ID',
    illinois:'IL',indiana:'IN',iowa:'IA',kansas:'KS',kentucky:'KY',louisiana:'LA',
    maine:'ME',maryland:'MD',massachusetts:'MA',michigan:'MI',minnesota:'MN',
    mississippi:'MS',missouri:'MO',montana:'MT',nebraska:'NE',nevada:'NV',
    newhampshire:'NH',newjersey:'NJ',newmexico:'NM',newyork:'NY',northcarolina:'NC',
    northdakota:'ND',ohio:'OH',oklahoma:'OK',oregon:'OR',pennsylvania:'PA',
    rhodeisland:'RI',southcarolina:'SC',southdakota:'SD',tennessee:'TN',texas:'TX',
    utah:'UT',vermont:'VT',virginia:'VA',washington:'WA',westvirginia:'WV',
    wisconsin:'WI',wyoming:'WY',districtofcolumbia:'DC',dc:'DC'
  };

  const countyPart = parts[0];
  const stateKey = parts[1].replace(/[^a-z]/g, '');
  result.county = countyPart.replace(/[-_]/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  result.state = stateMap[stateKey] || '';
  return result;
}

async function getUrlMapping() {
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME_URLS}!A2:C`,
  });

  const mapping = {};
  for (const row of response.data.values || []) {
    const county = clean(row[0]);
    const state = clean(row[1]).toUpperCase();
    const url = clean(row[2]);
    if (!url) continue;

    const item = { county, state };
    const base = normalizeBaseUrl(url);
    const origin = getOrigin(url);
    if (base) mapping[base] = item;
    if (origin && !mapping[origin]) mapping[origin] = item;
  }
  return mapping;
}

function isFinalizedRow(raw) {
  const combined = clean(`${raw.status || ''} ${raw.auctionStatus || ''}`).toLowerCase();
  const preview = combined.includes('preview') || combined.includes('upcoming');
  if (preview) return false;

  return combined.includes('sold') || combined.includes('paid') || combined.includes('closed');
}

function resolveGeo(raw, urlMapping) {
  const sourceUrl = raw.sourceUrl || '';
  let geo = urlMapping[normalizeBaseUrl(sourceUrl)] || {};

  if ((!geo.county || !geo.state) && sourceUrl) {
    geo = { ...geo, ...(urlMapping[getOrigin(sourceUrl)] || {}) };
  }

  if ((!geo.county || !geo.state) && sourceUrl) {
    try {
      const inferred = inferCountyStateFromHost(new URL(decodeAmp(sourceUrl)).hostname);
      geo = {
        county: geo.county || inferred.county,
        state: geo.state || inferred.state,
      };
    } catch {}
  }

  return geo;
}

function mapRow(raw, urlMapping, anomalies) {
  if (!raw || !isFinalizedRow(raw)) return null;

  const mapped = Object.fromEntries(HEADERS.map(header => [header, '']));
  const geo = resolveGeo(raw, urlMapping);

  let { city, zip } = parseCityZip(raw.cityStateZip);
  if ((!city || !zip) && raw.propertyAddress) {
    const fallback = parseCityZip(raw.propertyAddress);
    city ||= fallback.city;
    zip ||= fallback.zip;
  }

  const salePrice = parseCurrency(raw.salePrice);
  const estimatedSurplus = parseCurrency(raw.surplus); // Authoritative source only.

  if (salePrice === null) {
    anomalies.push({
      type: 'MissingSalePrice',
      message: 'Sale price missing for finalized sale.',
      parcelId: raw.parcelId || '',
      caseNumber: raw.caseNumber || '',
      sourceUrl: raw.sourceUrl || '',
    });
  }

  if (estimatedSurplus === null) {
    anomalies.push({
      type: 'MissingSurplus',
      message: 'Canonical raw.surplus is unavailable.',
      parcelId: raw.parcelId || '',
      caseNumber: raw.caseNumber || '',
      sourceUrl: raw.sourceUrl || '',
    });
  }

  mapped['State'] = geo.state || '';
  mapped['County'] = geo.county || '';
  mapped['Property Address'] = raw.propertyAddress || '';
  mapped['City'] = city;
  mapped['ZIP Code'] = zip;
  mapped['Parcel / APN Number'] = raw.parcelId || '';
  mapped['Case Number'] = cleanCaseNumber(raw.caseNumber);
  mapped['Auction Date'] = raw.auctionDate || '';
  mapped['Sale Finalized (Yes/No)'] = 'Yes';
  mapped['Sale Price'] = salePrice === null ? 'Unavailable' : String(salePrice);
  mapped['Opening / Minimum Bid'] = raw.openingBid || '';
  mapped['Estimated Surplus'] = estimatedSurplus === null ? '' : String(estimatedSurplus);
  mapped['Final Estimated Surplus to Owner'] = estimatedSurplus === null ? '' : String(estimatedSurplus);

  const meets = estimatedSurplus !== null && estimatedSurplus >= MIN_SURPLUS;
  mapped['Meets Minimum Surplus? (Yes/No)'] = meets ? 'Yes' : 'No';
  mapped['Deal Viable? (Yes/No)'] = meets ? 'Yes' : 'No';

  for (const header of [
    'Ownership Deed Collected? (Yes/No)',
    'Foreclosure Deed Collected? (Yes/No)',
    'Proof of Sale Collected? (Yes/No)',
    'Debt Search Screenshot Collected? (Yes/No)',
    'Tax Assessor Page Collected? (Yes/No)',
    'File Complete? (Yes/No)',
    'File Submitted? (Yes/No)',
  ]) mapped[header] = 'No';

  if (salePrice === null) mapped['Kickback Reason'] = `Sale price unavailable; status: ${raw.status || raw.auctionStatus || 'unknown'}`;
  else if (estimatedSurplus === null) mapped['Kickback Reason'] = 'Canonical surplus unavailable';

  return mapped;
}

async function ensureHeaderRow() {
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME_RAW}!A1:AZ1`,
  });
  const existing = response.data.values?.[0] || [];
  const mismatch = existing.length < HEADERS.length || HEADERS.some((header, index) => existing[index] !== header);

  if (mismatch) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME_RAW}!A1`,
      valueInputOption: 'RAW',
      requestBody: { values: [HEADERS] },
    });
    console.log(`Header row written to ${SHEET_NAME_RAW}.`);
  }
}

async function getExistingCaseNumbers() {
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME_RAW}!G2:G`,
  });

  return new Set((response.data.values || [])
    .map(row => normalizeCaseKey(row?.[0]))
    .filter(Boolean));
}

async function filterOutExistingCases(rows) {
  if (!rows.length) return [];
  const existing = await getExistingCaseNumbers();
  const accepted = [];
  const withinBatch = new Set();

  for (const row of rows) {
    const key = normalizeCaseKey(row['Case Number']);
    if (key && (existing.has(key) || withinBatch.has(key))) {
      console.log(`Skipping existing/duplicate Case Number: ${row['Case Number']}`);
      continue;
    }
    if (key) withinBatch.add(key);
    accepted.push(row);
  }
  return accepted;
}

async function appendRows(rows) {
  if (!rows.length) return;
  await ensureHeaderRow();

  const values = rows.map(row => HEADERS.map(header => row[header] ?? ''));
  const maxAttempts = 4;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await sheets.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME_RAW}!A:AS`,
        valueInputOption: 'RAW',
        insertDataOption: 'INSERT_ROWS',
        requestBody: { values },
      });
      console.log(`Appended ${values.length} mapped row(s).`);
      return;
    } catch (error) {
      console.error(`Sheets append attempt ${attempt} failed:`, error.message || error);
      if (attempt === maxAttempts) throw error;
      const waitMs = Math.min(2000 * attempt, 8000);
      await new Promise(resolve => setTimeout(resolve, waitMs));
    }
  }
}

function loadInputRows(filename) {
  const parsed = JSON.parse(fs.readFileSync(filename, 'utf8'));
  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed.rows)) return parsed.rows;
  if (Array.isArray(parsed.parsedRows)) return parsed.parsedRows;
  return [];
}

(async () => {
  try {
    if (!fs.existsSync(INPUT_FILE)) throw new Error(`Input file not found: ${INPUT_FILE}`);

    const rawData = loadInputRows(INPUT_FILE);
    console.log(`Loaded ${rawData.length} parsed row(s) from ${INPUT_FILE}.`);
    console.log('Sample parsed row:', rawData[0] || '<<no rows>>');

    const urlMapping = await getUrlMapping();
    console.log(`Loaded ${Object.keys(urlMapping).length} URL mapping key(s).`);

    const anomalies = [];
    const unique = new Map();
    let filteredOutCount = 0;

    for (const original of rawData) {
      const raw = { ...original, caseNumber: cleanCaseNumber(original.caseNumber) };
      const mapped = mapRow(raw, urlMapping, anomalies);

      if (!mapped) {
        filteredOutCount += 1;
        continue;
      }

      const key = [
        normalizeBaseUrl(raw.sourceUrl),
        normalizeCaseKey(raw.caseNumber),
        clean(raw.parcelId).toUpperCase(),
        clean(raw.auctionId),
      ].join('|');

      if (!unique.has(key)) unique.set(key, mapped);
    }

    const mappedRows = [...unique.values()];
    console.log(`Filtered out ${filteredOutCount} non-finalized/invalid row(s).`);
    console.log(`Produced ${mappedRows.length} mapped row(s).`);

    fs.writeFileSync(OUTPUT_FILE, JSON.stringify(mappedRows, null, 2));
    fs.writeFileSync(ANOMALY_FILE, JSON.stringify(anomalies, null, 2));
    console.log(`Saved mapped rows -> ${OUTPUT_FILE}`);
    console.log(`Saved ${anomalies.length} anomaly record(s) -> ${ANOMALY_FILE}`);

    const rowsToAppend = await filterOutExistingCases(mappedRows);
    if (!rowsToAppend.length) console.log('No new rows to append after checking existing Case Numbers.');
    else await appendRows(rowsToAppend);

    console.log('DONE - mappingScraper completed successfully.');
  } catch (error) {
    console.error('mappingScraper failed:', error.message || error);
    process.exitCode = 1;
  }
})();
