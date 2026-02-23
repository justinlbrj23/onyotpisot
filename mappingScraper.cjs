// mappingScraper.cjs (Stage 3: map parsed auctions → Google Sheets + artifact output)
// Requires: npm install googleapis

const fs = require("fs");
const { google } = require("googleapis");

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = "./service-account.json";
const SPREADSHEET_ID = "1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA";

const SHEET_NAME_URLS = "web_tda";  // contains county/state/url mapping
const SHEET_NAME_RAW  = "raw_main"; // TSSF-compliant rows destination

const INPUT_FILE  = process.argv[2] || "parsed-auctions.json";
const OUTPUT_FILE = "mapped-output.json";
const ANOMALY_FILE = "mapping-anomalies.json";

// Surplus threshold (keep consistent with your scraper)
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
// HEADERS (TSSF-compliant)
// =========================
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
// Helpers
// =========================

// Decode common encodings from Sheets/copy-paste
function decodeAmp(u) {
  return String(u || "")
    .replace(/&amp;amp;/gi, "&amp;")
    .replace(/&amp;/gi, "&");
}

// Normalize URL for mapping: remove paging & temporal params; lowercase host+path
function normalizeBaseUrl(u) {
  if (!u) return "";
  const raw = decodeAmp(u).trim();
  try {
    const url = new URL(raw);
    // Remove page-like params
    const paramsToStrip = [
      "page","pagenum","p","pg","pageno","start","startrow","offset",
      "AUCTIONDATE","auctiondate","Zmethod","zmethod" // optional if you want date-agnostic mapping
    ];
    paramsToStrip.forEach(p => url.searchParams.delete(p));

    // Clean path (no trailing slashes)
    const cleanPath = url.pathname.replace(/\/+$/, "");
    return `${url.protocol}//${url.hostname}${cleanPath}`.toLowerCase();
  } catch {
    // Fallback: dead-simple strip &page= and trailing slash
    return raw.split("&page=")[0].replace(/\/+$/, "").toLowerCase();
  }
}

// Origin-only key (fallback)
function getOrigin(u) {
  try {
    return new URL(decodeAmp(u)).origin.toLowerCase();
  } catch {
    return "";
  }
}

// Parse "City, ST ZIP" or "City ST ZIP" or "... ZIP"
function parseCityZip(source) {
  const out = { city: "", zip: "" };
  if (!source) return out;
  const s = String(source).trim();

  // Common form: "Dallas, TX 75201" OR "Dallas TX 75201" OR "Dallas 75201"
  const zipMatch = s.match(/\b(\d{5})(?:-\d{4})?\b/);
  if (zipMatch) {
    out.zip = zipMatch[1];
    // Take the part before ZIP, drop state if present
    const before = s.slice(0, zipMatch.index).replace(/\s+/g, " ").trim();
    // Try to remove trailing state abbreviations like ", TX" / " TX"
    const cleaned = before.replace(/,\s*[A-Za-z]{2}\s*$/,'').replace(/\s+[A-Za-z]{2}\s*$/,'').trim();
    out.city = cleaned.replace(/[,]+$/,'').trim();
  }
  return out;
}

// Normalize Yes/No strings
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

// Currency to number
function parseCurrency(str) {
  if (str === null || str === undefined) return null;
  if (typeof str === "number") return str;
  const s = String(str).trim();
  if (!s) return null;
  const n = parseFloat(s.replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? n : null;
}

// Optional: infer county/state from host like "dallas.texas.sheriffsaleauctions.com"
function inferCountyStateFromHost(hostname) {
  // Basic heuristic—customize/extend as desired
  const out = { county: "", state: "" };
  if (!hostname) return out;

  const parts = hostname.split("."); // e.g., ['dallas','texas','sheriffsaleauctions','com']
  if (parts.length >= 3) {
    const countyCandidate = parts[0];   // 'dallas'
    const stateCandidate  = parts[1];   // 'texas' (maybe)
    const stateMap = {
      alabama: 'AL', alaska: 'AK', arizona: 'AZ', arkansas: 'AR', california: 'CA',
      colorado: 'CO', connecticut: 'CT', delaware: 'DE', florida: 'FL', georgia: 'GA',
      hawaii: 'HI', idaho: 'ID', illinois: 'IL', indiana: 'IN', iowa: 'IA',
      kansas: 'KS', kentucky: 'KY', louisiana: 'LA', maine: 'ME', maryland: 'MD',
      massachusetts: 'MA', michigan: 'MI', minnesota: 'MN', mississippi: 'MS', missouri: 'MO',
      montana: 'MT', nebraska: 'NE', nevada: 'NV', "newmexico": 'NM', "new-mexico": 'NM',
      "newyork": 'NY', "new-york": 'NY', "northcarolina": 'NC', "north-carolina": 'NC',
      "northdakota": 'ND', "north-dakota": 'ND', ohio: 'OH', oklahoma: 'OK', oregon: 'OR',
      pennsylvania: 'PA', "rhodeisland": 'RI', "rhode-island": 'RI', "southcarolina": 'SC',
      "south-carolina": 'SC', "southdakota": 'SD', "south-dakota": 'SD', tennessee: 'TN',
      texas: 'TX', utah: 'UT', vermont: 'VT', virginia: 'VA', washington: 'WA',
      "westvirginia": 'WV', "west-virginia": 'WV', wisconsin: 'WI', wyoming: 'WY',
      // DC
      "districtofcolumbia": 'DC', "district-of-columbia": 'DC', dc: 'DC'
    };
    const stateKey = stateCandidate.toLowerCase().replace(/[\s._-]/g, "");
    out.county = countyCandidate ? countyCandidate.replace(/[-_]/g, " ").replace(/\b\w/g, c => c.toUpperCase()) : "";
    out.state = stateMap[stateKey] || "";
  }
  return out;
}

// =========================
// County/State mapping
// =========================
async function getUrlMapping() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME_URLS}!A2:C`,
  });

  const rows = res.data.values || [];
  const mapping = {};

  rows.forEach((row) => {
    const [county, state, url] = [row[0] || "", row[1] || "", row[2] || ""];
    if (!url) return;
    const key = normalizeBaseUrl(url);
    if (!key) return;
    mapping[key] = { county: county || "", state: state || "" };

    // Also origin-only fallback
    const origin = getOrigin(url);
    if (origin && !mapping[origin]) {
      mapping[origin] = { county: county || "", state: state || "" };
    }
  });

  return mapping;
}

// =========================
// Map parsed auction row → TSSF headers
// =========================
function mapRow(raw, urlMapping, anomalies) {
  if (!raw) return null;

  // Normalize auctionStatus/status
  const statusRaw  = String(raw.auctionStatus || raw.status || raw.statusNote || "").trim();
  const statusNorm = statusRaw.toLowerCase();

  // Sold or "paid" family of statuses are considered finalized
  const isSold       = statusNorm.includes("sold");
  const isPaidStatus = /paid in full|paid prior to sale|paid/i.test(statusNorm);

  if (!isSold && !isPaidStatus) return null;

  const mapped = {};
  HEADERS.forEach(h => (mapped[h] = ""));

  // Resolve mapping
  const baseKey = normalizeBaseUrl(raw.sourceUrl || "");
  let geo = urlMapping[baseKey] || {};

  // Origin-only fallback
  if ((!geo.county || !geo.state) && raw.sourceUrl) {
    const origin = getOrigin(raw.sourceUrl);
    if (origin && urlMapping[origin]) geo = urlMapping[origin];
  }

  // Optional heuristic fallback from host if still missing
  if ((!geo.county || !geo.state) && raw.sourceUrl) {
    try {
      const host = new URL(decodeAmp(raw.sourceUrl)).hostname;
      const inferred = inferCountyStateFromHost(host);
      if (!geo.county && inferred.county) geo.county = inferred.county;
      if (!geo.state && inferred.state)   geo.state   = inferred.state;
    } catch { /* ignore */ }
  }

  const salePriceRaw  = raw.salePrice || raw.amount || "";
  const openingBidRaw = raw.openingBid || "";
  const assessedRaw   = raw.assessedValue || raw.assessed || "";
  const cityZipSource = raw.cityStateZip || "";

  // City/ZIP parse with fallback
  let { city, zip } = parseCityZip(cityZipSource);
  if (!city && raw.propertyAddress) {
    // last chance: if propertyAddress ends with "..., City ST ZIP"
    const cz = parseCityZip(raw.propertyAddress);
    if (cz.city) city = cz.city;
    if (cz.zip) zip = cz.zip;
  }

  mapped["State"] = geo.state || "";
  mapped["County"] = geo.county || "";
  mapped["Property Address"] = raw.propertyAddress || "";
  mapped["City"] = city;
  mapped["ZIP Code"] = zip;
  mapped["Parcel / APN Number"] = raw.parcelId || "";
  mapped["Case Number"] = raw.caseNumber || "";
  mapped["Auction Date"] = raw.auctionDate || raw.date || "";
  mapped["Sale Finalized (Yes/No)"] = "Yes";
  mapped["Sale Price"] = salePriceRaw || "";
  mapped["Opening / Minimum Bid"] = openingBidRaw || "";

  // Prefer parser-provided surplus; else compute
  let estimatedSurplus = null;
  if (raw.surplusAssessVsSale !== undefined && raw.surplusAssessVsSale !== null) {
    const n = Number(raw.surplusAssessVsSale);
    if (Number.isFinite(n)) estimatedSurplus = n;
  } else if (raw.surplusSaleVsOpen !== undefined && raw.surplusSaleVsOpen !== null) {
    const n = Number(raw.surplusSaleVsOpen);
    if (Number.isFinite(n)) estimatedSurplus = n;
  } else {
    const assessed = parseCurrency(assessedRaw);
    const sale     = parseCurrency(salePriceRaw);
    const open     = parseCurrency(openingBidRaw);

    if (assessed !== null && sale !== null) {
      estimatedSurplus = assessed - sale;
    } else if (sale !== null && open !== null) {
      estimatedSurplus = sale - open;
    }
  }

  const saleNum = parseCurrency(salePriceRaw);
  if (saleNum === null && !isPaidStatus) {
    anomalies.push({
      type: "MissingSalePrice",
      message: "Sold auction missing sale price (parser output or extracted).",
      parcelId: raw.parcelId,
      caseNumber: raw.caseNumber,
      sourceUrl: raw.sourceUrl
    });
  }

  if (estimatedSurplus === null) {
    anomalies.push({
      type: "MissingSurplus",
      message: "Could not compute surplus (no parser surplus and insufficient fields).",
      parcelId: raw.parcelId,
      caseNumber: raw.caseNumber,
      sourceUrl: raw.sourceUrl
    });
  }

  mapped["Estimated Surplus"] = estimatedSurplus !== null ? String(estimatedSurplus) : "";
  mapped["Final Estimated Surplus to Owner"] = estimatedSurplus !== null ? String(estimatedSurplus) : "";

  // Meets minimum
  let meetsMinimum = null;
  if (raw.meetsMinimumSurplus !== undefined && raw.meetsMinimumSurplus !== null) {
    meetsMinimum = String(raw.meetsMinimumSurplus).trim().toLowerCase() === "yes";
  } else if (estimatedSurplus !== null) {
    meetsMinimum = estimatedSurplus >= MIN_SURPLUS;
  }

  mapped["Meets Minimum Surplus? (Yes/No)"] = meetsMinimum === null ? "" : (meetsMinimum ? "Yes" : "No");
  mapped["Deal Viable? (Yes/No)"] = meetsMinimum ? "Yes" : "No";

  // default collection flags
  mapped["Ownership Deed Collected? (Yes/No)"] = "No";
  mapped["Foreclosure Deed Collected? (Yes/No)"] = "No";
  mapped["Proof of Sale Collected? (Yes/No)"] = "No";
  mapped["Debt Search Screenshot Collected? (Yes/No)"] = "No";
  mapped["Tax Assessor Page Collected? (Yes/No)"] = "No";
  mapped["File Complete? (Yes/No)"] = "No";
  mapped["File Submitted? (Yes/No)"] = "No";

  // Status note for manual review
  if (!salePriceRaw && statusRaw) {
    mapped["Kickback Reason"] = `status: ${statusRaw}`;
  }

  return mapped;
}

// =========================
// Ensure header row exists
// =========================
async function ensureHeaderRow() {
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME_RAW}!A1:AZ1`,
    });
    const firstRow = (res.data.values && res.data.values[0]) || [];
    // If empty or mismatched, write HEADERS into row 1
    const needsHeaders = firstRow.length === 0 || HEADERS.some((h, i) => (firstRow[i] || "") !== h);
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
    // Try creating the header anyway via append (sheet might be missing)
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
      console.error("❌ Fallback header append failed:", e2.message || e2);
    }
  }
}

// =========================
// Append rows to sheet (exponential backoff)
// =========================
async function appendRows(rows) {
  if (!rows.length) {
    console.log("⚠️ No mapped rows to append.");
    return;
  }

  // Ensure header first
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

// =========================
// MAIN
// =========================
(async () => {
  if (!fs.existsSync(INPUT_FILE)) {
    console.error(`❌ Input file not found: ${INPUT_FILE}`);
    process.exit(1);
  }

  const rawData = JSON.parse(fs.readFileSync(INPUT_FILE, "utf8"));
  console.log(`📦 Loaded ${rawData.length} parsed rows from ${INPUT_FILE}`);

  const urlMapping = await getUrlMapping();
  console.log(`🌐 Fetched ${Object.keys(urlMapping).length} URL→County/State mappings`);

  const anomalies = [];
  const uniqueMap = new Map();
  let filteredOutCount = 0;

  for (const raw of rawData) {
    const baseKey = normalizeBaseUrl(raw.sourceUrl || "");
    const key = `${baseKey}|${(raw.caseNumber || '').trim()}|${(raw.parcelId || '').trim()}`;
    if (uniqueMap.has(key)) continue;

    const mapped = mapRow(raw, urlMapping, anomalies);
    if (mapped) uniqueMap.set(key, mapped);
    else filteredOutCount++;
  }

  console.log(`ℹ️ Filtered out ${filteredOutCount} rows (non-finalized or invalid).`);

  const mappedRows = [...uniqueMap.values()];

  if (mappedRows.length) {
    console.log("🧪 Sample mapped row preview:", mappedRows[0]);
  } else {
    console.log("⚠️ No mapped rows produced after mapping step.");
  }

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(mappedRows, null, 2));
  console.log(`💾 Saved mapped rows → ${OUTPUT_FILE}`);

  if (anomalies.length) {
    fs.writeFileSync(ANOMALY_FILE, JSON.stringify(anomalies, null, 2));
    console.log(`⚠️ Saved ${anomalies.length} anomalies → ${ANOMALY_FILE}`);
  }

  try {
    await appendRows(mappedRows);
  } catch (err) {
    console.error('❌ Final append failed:', err.message || err);
    process.exit(1);
  }

  console.log("🏁 Done.");
})();
