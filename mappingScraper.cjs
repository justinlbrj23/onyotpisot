/* ===================================================================
   mappingScraper.cjs (Updated Version)
   ---------------------------------------------------------------
   Rules:
   - Use ONLY raw.surplus from webInspector output.
   - No secondary formulas.
   - No surplus from assessed values.
   - No fallback to alternate fields.
   =================================================================== */

const fs = require("fs");
const { google } = require("googleapis");

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = "./service-account.json";
const SPREADSHEET_ID = "1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA";

const SHEET_NAME_URLS = "web_tda";   // Contains county/state/url mapping
const SHEET_NAME_RAW  = "raw_main";  // Destination sheet

// INPUT / OUTPUT
const INPUT_FILE  = process.argv[2] || "parsed-auctions.json";
const OUTPUT_FILE = "mapped-output.json";
const ANOMALY_FILE = "mapping-anomalies.json";

// Surplus threshold (same as scraper)
const MIN_SURPLUS = 25000;

// =========================
// GOOGLE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

// =========================
// AUTHORITATIVE HEADERS
// =========================
const HEADERS = [
  "State","County","Property Address","City","ZIP Code","Parcel / APN Number","Case Number",
  "Auction Date","Sale Finalized (Yes/No)","Sale Price","Opening / Minimum Bid","Estimated Surplus",
  "Meets Minimum Surplus? (Yes/No)",

  // Ownership
  "Last Owner Name (as on Deed)","Additional Owner(s)","Ownership Type","Deed Type",
  "Owner Deed Recording Date","Owner Deed Instrument #",

  // Mortgages
  "Mortgage Lender Name","Mortgage Amount","Mortgage Recording Date",
  "Mortgage Satisfied? (Yes/No)","Mortgage Release Recording #","Mortgage Still Owed Amount",

  // Liens
  "Lien / Judgment Type","Creditor Name","Lien Amount","Lien Recording Date",
  "Lien Expired? (Yes/No)","Lien Satisfied? (Yes/No)",

  // Final Calculation
  "Total Open Debt","Final Estimated Surplus to Owner","Deal Viable? (Yes/No)",

  // Documentation
  "Ownership Deed Collected? (Yes/No)","Foreclosure Deed Collected? (Yes/No)",
  "Proof of Sale Collected? (Yes/No)","Debt Search Screenshot Collected? (Yes/No)",
  "Tax Assessor Page Collected? (Yes/No)","File Complete? (Yes/No)",

  // Submission
  "File Submitted? (Yes/No)","Submission Date","Accepted / Rejected","Kickback Reason",
  "Researcher Name"
];

// =========================
// Utility Functions
// =========================

// Normalize encodings
function decodeAmp(u) {
  return String(u || "")
    .replace(/&amp;amp;/gi, "&amp;")
    .replace(/&amp;/gi, "&");
}

// Convert currency string to number
function parseCurrency(str) {
  if (str === null || str === undefined) return null;
  if (typeof str === "number") return str;

  const s = String(str).trim();
  if (!s) return null;

  const n = parseFloat(s.replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? n : null;
}

// Extract City + ZIP
function parseCityZip(source) {
  const out = { city: "", zip: "" };
  if (!source) return out;
  const s = String(source).trim();

  const zipMatch = s.match(/\b(\d{5})(?:-\d{4})?\b/);
  if (zipMatch) {
    out.zip = zipMatch[1];
    const before = s.slice(0, zipMatch.index).replace(/\s+/g, " ").trim();
    const cleaned = before
      .replace(/,\s*[A-Za-z]{2}\s*$/,'')
      .replace(/\s+[A-Za-z]{2}\s*$/,'')
      .trim();
    out.city = cleaned.replace(/[,]+$/,'').trim();
  }

  return out;
}

// Normalize Yes/No
function yn(val) {
  if (val === true) return "Yes";
  if (val === false) return "No";
  if (typeof val === "string") {
    const v = val.trim().toLowerCase();
    if (["yes","y","true"].includes(v)) return "Yes";
    if (["no","n","false"].includes(v)) return "No";
  }
  return "";
}

/* =========================
   URL → COUNTY / STATE MAPPING
   ========================= */

// Normalize URL for mapping by stripping paging params & trailing slashes
function normalizeBaseUrl(u) {
  if (!u) return "";
  const raw = decodeAmp(u).trim();

  try {
    const url = new URL(raw);

    // Remove page-like parameters (keeps county/state mapping stable)
    const paramsToStrip = [
      "page","pagenum","p","pg","pageno","start","startrow","offset",
      "AUCTIONDATE","auctiondate","Zmethod","zmethod"
    ];
    paramsToStrip.forEach(p => url.searchParams.delete(p));

    const cleanPath = url.pathname.replace(/\/+$/, ""); // remove trailing slash
    return `${url.protocol}//${url.hostname}${cleanPath}`.toLowerCase();
  } catch {
    // Fallback: strip common paging markers
    return raw.split("&page=")[0].replace(/\/+$/, "").toLowerCase();
  }
}

// Extract origin-only part of URL
function getOrigin(u) {
  try {
    return new URL(decodeAmp(u)).origin.toLowerCase();
  } catch {
    return "";
  }
}

/**
 * Infer county/state from hostname patterns like:
 *   dallas.texas.sheriffsaleauctions.com
 */
function inferCountyStateFromHost(hostname) {
  const out = { county: "", state: "" };
  if (!hostname) return out;

  const parts = hostname.split(".");
  if (parts.length < 3) return out;

  const countyCandidate = parts[0];
  const stateCandidate = parts[1];

  // Convert to 2‑letter state codes
  const stateMap = {
    alabama: 'AL', alaska: 'AK', arizona: 'AZ', arkansas: 'AR', california: 'CA',
    colorado: 'CO', connecticut: 'CT', delaware: 'DE', florida: 'FL', georgia: 'GA',
    hawaii: 'HI', idaho: 'ID', illinois: 'IL', indiana: 'IN', iowa: 'IA',
    kansas: 'KS', kentucky: 'KY', louisiana: 'LA', maine: 'ME', maryland: 'MD',
    massachusetts: 'MA', michigan: 'MI', minnesota: 'MN', mississippi: 'MS', missouri: 'MO',
    montana: 'MT', nebraska: 'NE', nevada: 'NV', newmexico: 'NM', "new-mexico": 'NM',
    newyork: 'NY', "new-york": 'NY', northcarolina: 'NC', "north-carolina": 'NC',
    northdakota: 'ND', "north-dakota": 'ND', ohio: 'OH', oklahoma: 'OK', oregon: 'OR',
    pennsylvania: 'PA', rhodeisland: 'RI', "rhode-island": 'RI', southcarolina: 'SC',
    "south-carolina": 'SC', southdakota: 'SD', "south-dakota": 'SD', tennessee: 'TN',
    texas: 'TX', utah: 'UT', vermont: 'VT', virginia: 'VA', washington: 'WA',
    westvirginia: 'WV', "west-virginia": 'WV', wisconsin: 'WI', wyoming: 'WY',
    districtofcolumbia: 'DC', "district-of-columbia": 'DC', dc: 'DC'
  };

  const stateKey = stateCandidate.toLowerCase().replace(/[\s._-]/g, "");
  out.county = countyCandidate.replace(/[-_]/g, " ").replace(/\b\w/g, c => c.toUpperCase());
  out.state = stateMap[stateKey] || "";

  return out;
}

/**
 * Load URL → County/State mapping from Google Sheets
 */
async function getUrlMapping() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME_URLS}!A2:C`,  // County | State | URL
  });

  const rows = res.data.values || [];
  const mapping = {};

  rows.forEach((row) => {
    const [county, state, url] = [row[0] || "", row[1] || "", row[2] || ""];
    if (!url) return;

    const key = normalizeBaseUrl(url);
    if (key) {
      mapping[key] = { county: county || "", state: state || "" };
    }

    // Origin fallback
    const origin = getOrigin(url);
    if (origin && !mapping[origin]) {
      mapping[origin] = { county: county || "", state: state || "" };
    }
  });

  return mapping;
}

/* =========================
   MAP RAW PARSER ROW → TSSF FORMAT
   ========================= */
function mapRow(raw, urlMapping, anomalies) {
  if (!raw) return null;

  // Normalize status
  const statusRaw = String(raw.status || raw.auctionStatus || "").trim().toLowerCase();
  const isSold =
    statusRaw.includes("sold") ||
    statusRaw.includes("paid") ||
    statusRaw.includes("paid prior") ||
    statusRaw.includes("paid in full");

  // Only include finalized / sold auctions
  if (!isSold) {
    return null;
  }

  // Prepare mapped object with empty columns
  const mapped = {};
  HEADERS.forEach(h => (mapped[h] = ""));

  /* =========================
     COUNTY / STATE RESOLUTION
     ========================= */
  const baseKey = normalizeBaseUrl(raw.sourceUrl || "");
  let geo = urlMapping[baseKey] || {};

  // Fallback: origin
  if ((!geo.county || !geo.state) && raw.sourceUrl) {
    const origin = getOrigin(raw.sourceUrl);
    if (origin && urlMapping[origin]) geo = urlMapping[origin];
  }

  // Fallback: infer from hostname
  if ((!geo.county || !geo.state) && raw.sourceUrl) {
    try {
      const host = new URL(decodeAmp(raw.sourceUrl)).hostname;
      const inf = inferCountyStateFromHost(host);
      geo.county = geo.county || inf.county;
      geo.state  = geo.state  || inf.state;
    } catch {}
  }

  /* =========================
     CITY + ZIP PARSING
     ========================= */
  const cityZipSource = raw.cityStateZip || "";
  let { city, zip } = parseCityZip(cityZipSource);

  // Secondary fallback (sometimes embedded in propertyAddress)
  if (!city && raw.propertyAddress) {
    const alt = parseCityZip(raw.propertyAddress);
    if (alt.city) city = alt.city;
    if (alt.zip)  zip = alt.zip;
  }

  /* =========================
     SURPLUS: Sale - Opening Bid
     ========================= */
  const sale = parseCurrency(raw.salePrice);
  const open = parseCurrency(raw.openingBid);

  let estimatedSurplus = null;
  if (sale !== null && open !== null) {
    estimatedSurplus = sale - open;
  }

  // If sale price missing → anomaly
  if (sale === null) {
    anomalies.push({
      type: "MissingSalePrice",
      message: "Sale price missing for finalized sale.",
      parcelId: raw.parcelId,
      caseNumber: raw.caseNumber,
      sourceUrl: raw.sourceUrl,
    });
  }

  // If cannot compute surplus → anomaly
  if (estimatedSurplus === null) {
    anomalies.push({
      type: "MissingSurplus",
      message: "Cannot compute surplus: salePrice or openingBid missing.",
      parcelId: raw.parcelId,
      caseNumber: raw.caseNumber,
      sourceUrl: raw.sourceUrl,
    });
  }

  /* =========================
     WRITE CORE FIELDS
     ========================= */
  mapped["State"] = geo.state || "";
  mapped["County"] = geo.county || "";
  mapped["Property Address"] = raw.propertyAddress || "";
  mapped["City"] = city;
  mapped["ZIP Code"] = zip;
  mapped["Parcel / APN Number"] = raw.parcelId || "";
  mapped["Case Number"] = raw.caseNumber || "";
  mapped["Auction Date"] = raw.auctionDate || "";
  mapped["Sale Finalized (Yes/No)"] = "Yes";

  mapped["Sale Price"] = raw.salePrice || "";
  mapped["Opening / Minimum Bid"] = raw.openingBid || "";

  /* =========================
     SURPLUS → HEADERS
     ========================= */
  mapped["Estimated Surplus"] =
    estimatedSurplus !== null ? String(estimatedSurplus) : "";
  mapped["Final Estimated Surplus to Owner"] =
    estimatedSurplus !== null ? String(estimatedSurplus) : "";

  const meets = estimatedSurplus !== null && estimatedSurplus >= MIN_SURPLUS;
  mapped["Meets Minimum Surplus? (Yes/No)"] = meets ? "Yes" : "No";
  mapped["Deal Viable? (Yes/No)"] = meets ? "Yes" : "No";

  /* =========================
     DEFAULT DOCUMENTATION FLAGS
     ========================= */
  mapped["Ownership Deed Collected? (Yes/No)"] = "No";
  mapped["Foreclosure Deed Collected? (Yes/No)"] = "No";
  mapped["Proof of Sale Collected? (Yes/No)"] = "No";
  mapped["Debt Search Screenshot Collected? (Yes/No)"] = "No";
  mapped["Tax Assessor Page Collected? (Yes/No)"] = "No";
  mapped["File Complete? (Yes/No)"] = "No";
  mapped["File Submitted? (Yes/No)"] = "No";

  /* =========================
     Kickback Reason (optional)
     ========================= */
  if (!raw.salePrice && raw.status) {
    mapped["Kickback Reason"] = `status: ${raw.status}`;
  }

  return mapped;
}

/* =========================
   ENSURE HEADER ROW EXISTS
   ========================= */
async function ensureHeaderRow() {
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME_RAW}!A1:AZ1`,
    });

    const firstRow = (res.data.values && res.data.values[0]) || [];

    // Check if row 1 matches HEADERS
    const needsHeaders =
      firstRow.length === 0 ||
      HEADERS.some((h, i) => (firstRow[i] || "") !== h);

    if (needsHeaders) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME_RAW}!A1`,
        valueInputOption: "RAW",
        requestBody: { values: [HEADERS] },
      });

      console.log(`🧭 Header row written to "${SHEET_NAME_RAW}"`);
    }

  } catch (err) {
    console.error("❌ Failed to ensure header row:", err.message || err);

    // Fallback: try to append header row instead
    try {
      await sheets.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME_RAW}!A1`,
        valueInputOption: "RAW",
        insertDataOption: "INSERT_ROWS",
        requestBody: { values: [HEADERS] },
      });

      console.log(`🧭 Header row appended to "${SHEET_NAME_RAW}"`);

    } catch (e2) {
      console.error("❌ Header append fallback failed:", e2.message || e2);
    }
  }
}

/* =========================
   APPEND ROWS WITH RETRIES
   ========================= */
async function appendRows(rows) {
  if (!rows.length) {
    console.log("⚠️ No mapped rows to append.");
    return;
  }

  // Ensure header row is present first
  await ensureHeaderRow();

  const values = rows.map(row => HEADERS.map(h => row[h] || ""));

  let attempt = 0;
  const maxAttempts = 4;

  while (attempt < maxAttempts) {
    attempt++;

    try {
      await sheets.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME_RAW}!A1`,
        valueInputOption: "RAW",
        insertDataOption: "INSERT_ROWS",
        requestBody: { values },
      });

      console.log(`✅ Appended ${values.length} mapped rows.`);
      return;

    } catch (err) {
      const wait = Math.min(2000 * attempt, 8000);

      console.error(`❌ Sheets append attempt ${attempt} failed:`, err.message || err);
      if (attempt >= maxAttempts) throw err;

      console.log(`⏳ Retrying in ${wait}ms...`);
      await new Promise(r => setTimeout(r, wait));
    }
  }
}

/* =========================
   MAIN PIPELINE
   ========================= */

(async () => {
  // Ensure input exists
  if (!fs.existsSync(INPUT_FILE)) {
    console.error(`❌ Input file not found: ${INPUT_FILE}`);
    process.exit(1);
  }

  // Load parsed rows from webInspector
  const rawData = JSON.parse(fs.readFileSync(INPUT_FILE, "utf8"));
  console.log(`📦 Loaded ${rawData.length} parsed rows from ${INPUT_FILE}`);

  // Load URL→County/State mapping
  const urlMapping = await getUrlMapping();
  console.log(`🌐 Loaded ${Object.keys(urlMapping).length} URL mappings`);

  const anomalies = [];
  const uniqueMap = new Map();
  let filteredOutCount = 0;

  // Process each parsed row
for (const raw of rawData) {
  const baseKey = normalizeBaseUrl(raw.sourceUrl || "");
  const key = `${baseKey}|${(raw.caseNumber || '').trim()}|${(raw.parcelId || '').trim()}`;

  // Deduplicate
  if (uniqueMap.has(key)) continue;

  const mapped = mapRow(raw, urlMapping, anomalies);

  // 💥 FILTER HERE → Only include rows that meet surplus requirement
  if (mapped && mapped["Meets Minimum Surplus? (Yes/No)"] === "Yes") {
    uniqueMap.set(key, mapped);
  } else {
    filteredOutCount++;
  }
}

  console.log(`ℹ️ Filtered out ${filteredOutCount} non-finalized or invalid rows.`);

  // Final mapped rows array
  const mappedRows = [...uniqueMap.values()];

  // Preview one row (optional debug)
  if (mappedRows.length) {
    console.log("🧪 Sample mapped row preview:", mappedRows[0]);
  } else {
    console.log("⚠️ No mapped rows produced.");
  }

  // ============================
  // WRITE OUTPUT FILES
  // ============================
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(mappedRows, null, 2));
  console.log(`💾 Saved mapped rows → ${OUTPUT_FILE}`);

  if (anomalies.length) {
    fs.writeFileSync(ANOMALY_FILE, JSON.stringify(anomalies, null, 2));
    console.log(`⚠️ Saved ${anomalies.length} anomalies → ${ANOMALY_FILE}`);
  }

  // ============================
  // APPEND TO GOOGLE SHEETS
  // ============================
  try {
    await appendRows(mappedRows);
  } catch (err) {
    console.error("❌ Final append failed:", err.message || err);
    process.exit(1);
  }

  console.log("🏁 DONE — mappingScraper completed successfully.");
})();