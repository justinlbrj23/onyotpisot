// mappingScraper.cjs (Stage 3: map parsed auctions → Google Sheets + artifact output)
// Requires: npm install googleapis

const fs = require("fs");
const { google } = require("googleapis");

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = "./service-account.json";
const SPREADSHEET_ID = "1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA";
const SHEET_NAME_URLS = "web_tda";
const SHEET_NAME_RAW = "raw_main";
const INPUT_FILE = process.argv[2] || "parsed-auctions.json";
const OUTPUT_FILE = "mapped-output.json";
const ANOMALY_FILE = "mapping-anomalies.json";

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

// Normalize URL for mapping lookups: remove page param, trailing slashes, lowercase origin+path
function normalizeBaseUrl(u) {
  if (!u) return "";
  try {
    const url = new URL(u);
    // remove page param if present
    url.searchParams.delete("page");
    // remove any page= in query string variants
    url.search = url.search.replace(/(^&|&)?page=\d+/g, "");
    // remove trailing slash from pathname
    const p = url.pathname.replace(/\/+$/, "");
    return `${url.protocol}//${url.hostname}${p}`.toLowerCase();
  } catch (e) {
    // fallback: strip &page= and trailing slash
    return String(u).split("&page=")[0].replace(/\/+$/, "").trim().toLowerCase();
  }
}

// =========================
// County/State mapping
// =========================
async function getUrlMapping() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME_URLS}!A2:C`,
  });

  console.log('DEBUG getUrlMapping response keys:', Object.keys(res.data || {}));
  const rows = res.data.values || [];
  const mapping = {};

  rows.forEach(([county, state, url]) => {
    if (!url) return;
    const key = normalizeBaseUrl(url);
    mapping[key] = { county: county || "", state: state || "" };
    // also store origin-only fallback (host)
    try {
      const origin = new URL(url).origin.toLowerCase();
      if (!mapping[origin]) mapping[origin] = { county: county || "", state: state || "" };
    } catch (e) {
      // ignore
    }
  });

  return mapping;
}

// =========================
// Normalize Yes/No
// =========================
function yn(val) {
  if (val === true) return "Yes";
  if (val === false) return "No";
  if (typeof val === "string") {
    const v = val.trim().toLowerCase();
    if (v === "yes" || v === "y" || v === "true") return "Yes";
    if (v === "no" || v === "n" || v === "false") return "No";
  }
  return "";
}

// =========================
// Currency parser
// =========================
function parseCurrency(str) {
  if (str === null || str === undefined) return null;
  if (typeof str === "number") return str;
  const s = String(str).trim();
  if (!s) return null;
  const n = parseFloat(s.replace(/[^0-9.-]/g, ""));
  return isNaN(n) ? null : n;
}

// =========================
// Map parsed auction row → TSSF headers
// =========================
function mapRow(raw, urlMapping, anomalies) {
  // allow case-insensitive Sold and trim whitespace
  if (!raw || String(raw.auctionStatus || '').trim().toLowerCase() !== 'sold') return null;

  const mapped = {};
  HEADERS.forEach(h => (mapped[h] = ""));

  const baseKey = normalizeBaseUrl(raw.sourceUrl || "");
  let geo = urlMapping[baseKey] || { county: "", state: "" };

  // fallback: try origin-only mapping
  try {
    const origin = new URL(raw.sourceUrl || "").origin.toLowerCase();
    if (!geo.county && urlMapping[origin]) geo = urlMapping[origin];
  } catch (e) {
    // ignore
  }

  mapped["State"] = geo.state;
  mapped["County"] = geo.county;
  mapped["Property Address"] = raw.propertyAddress || "";
  mapped["Parcel / APN Number"] = raw.parcelId || "";
  mapped["Case Number"] = raw.caseNumber || "";
  mapped["Auction Date"] = raw.auctionDate || raw.date || "";
  mapped["Sale Finalized (Yes/No)"] = "Yes";

  const salePriceRaw = raw.salePrice || raw.amount || "";
  const openingBidRaw = raw.openingBid || "";
  const assessedRaw = raw.assessedValue || raw.assessed || "";

  mapped["Sale Price"] = salePriceRaw;
  mapped["Opening / Minimum Bid"] = openingBidRaw;

  // Prefer parser-provided surplus fields if present
  let estimatedSurplus = null;
  if (raw.surplusAssessVsSale !== undefined && raw.surplusAssessVsSale !== null) {
    const n = parseFloat(raw.surplusAssessVsSale);
    estimatedSurplus = isNaN(n) ? null : n;
  } else if (raw.surplusSaleVsOpen !== undefined && raw.surplusSaleVsOpen !== null) {
    const n = parseFloat(raw.surplusSaleVsOpen);
    estimatedSurplus = isNaN(n) ? null : n;
  } else {
    // fallback: compute from assessed - sale OR sale - open
    const assessed = parseCurrency(assessedRaw);
    const sale = parseCurrency(salePriceRaw);
    const open = parseCurrency(openingBidRaw);

    if (assessed !== null && sale !== null) {
      estimatedSurplus = assessed - sale;
    } else if (sale !== null && open !== null) {
      estimatedSurplus = sale - open;
    }
  }

  // anomalies: only push when we truly cannot determine sale or surplus
  const saleNum = parseCurrency(salePriceRaw);
  if (saleNum === null) {
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

  // Use parser's meetsMinimumSurplus if present (it is 'Yes'/'No' in parser), else compute
  let meetsMinimum = null;
  if (raw.meetsMinimumSurplus !== undefined && raw.meetsMinimumSurplus !== null) {
    meetsMinimum = String(raw.meetsMinimumSurplus).trim().toLowerCase() === "yes";
  } else if (estimatedSurplus !== null) {
    meetsMinimum = estimatedSurplus >= 25000;
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

  return mapped;
}

// =========================
// Append rows to sheet (exponential backoff)
// =========================
async function appendRows(rows) {
  if (!rows.length) {
    console.log("⚠️ No mapped rows to append.");
    return;
  }

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
  console.log('DEBUG sample row:', rawData[0] || 'no rows');

  const urlMapping = await getUrlMapping();
  console.log(`🌐 Fetched ${Object.keys(urlMapping).length} URL → County/State mappings`);

  const anomalies = [];
  const uniqueMap = new Map();
  let filteredOutCount = 0;

  rawData.forEach(raw => {
    const baseKey = normalizeBaseUrl(raw.sourceUrl || "");
    const key = `${baseKey}|${raw.caseNumber || ''}|${raw.parcelId || ''}`;
    if (!uniqueMap.has(key)) {
      const mapped = mapRow(raw, urlMapping, anomalies);
      console.log('DEBUG mapping attempt:', { key, auctionStatus: raw.auctionStatus, mappedPresent: !!mapped });
      if (mapped) uniqueMap.set(key, mapped);
      else filteredOutCount++;
    }
  });

  console.log(`ℹ️ Filtered out ${filteredOutCount} rows (non-Sold or invalid).`);

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