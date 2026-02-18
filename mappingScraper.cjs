// mappingScraper.cjs (Stage 3: map parsed auctions → Google Sheets + artifact output)
// Requires: npm install googleapis

const fs = require("fs");
const { google } = require("googleapis");

// =========================
// CONFIG (env-driven)
// =========================
const SERVICE_ACCOUNT_FILE = process.env.SERVICE_ACCOUNT_FILE || "./service-account.json";
const SPREADSHEET_ID = process.env.SPREADSHEET_ID || "1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA";
const SHEET_NAME_URLS = process.env.SHEET_NAME_URLS || "web_tda";
const SHEET_NAME_RAW = process.env.SHEET_NAME_RAW || "raw_main";
const INPUT_FILE = process.argv[2] || process.env.INPUT_FILE || "parsed-auctions.json";
const OUTPUT_FILE = process.env.OUTPUT_FILE || "mapped-output.json";
const ANOMALY_FILE = process.env.ANOMALY_FILE || "mapping-anomalies.json";

const MIN_SURPLUS = parseFloat(process.env.MIN_SURPLUS || "25000");
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "100", 10);
const RETRY_MAX = parseInt(process.env.RETRY_MAX || "5", 10);
const RETRY_BASE_MS = parseInt(process.env.RETRY_BASE_MS || "500", 10);
const ENABLE_SHEET_IDEMPOTENCY = (process.env.ENABLE_SHEET_IDEMPOTENCY || "false").toLowerCase() === "true";

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
// County/State mapping
// =========================
async function getUrlMapping() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME_URLS}!A2:C`,
  });

  const rows = res.data.values || [];
  const mapping = {};

  rows.forEach(([county, state, url]) => {
    if (url) mapping[url.trim()] = { county: county || "", state: state || "" };
  });

  return mapping;
}

// =========================
// Normalize Yes/No
// =========================
function yn(val) {
  if (val === true || val === "Yes") return "Yes";
  if (val === false || val === "No") return "No";
  if (typeof val === "string") {
    const v = val.trim().toLowerCase();
    if (v === "yes") return "Yes";
    if (v === "no") return "No";
  }
  return "";
}

// =========================
// Currency parser
// =========================
function parseCurrency(str) {
  if (!str) return null;
  const n = parseFloat(String(str).replace(/[^0-9.-]/g, ""));
  return isNaN(n) ? null : n;
}

// =========================
// Map parsed auction row → TSSF headers
// =========================
function mapRow(raw, urlMapping, anomalies) {
  if (!raw || raw.auctionStatus !== "Sold") return null;

  const mapped = {};
  HEADERS.forEach(h => (mapped[h] = ""));

  const baseUrl = (raw.sourceUrl || "").split("&page=")[0];
  const geo = urlMapping[baseUrl] || { county: "", state: "" };

  mapped["State"] = geo.state;
  mapped["County"] = geo.county;
  mapped["Property Address"] = raw.propertyAddress || "";
  mapped["Parcel / APN Number"] = raw.parcelId || "";
  mapped["Case Number"] = raw.caseNumber || "";
  mapped["Auction Date"] = raw.auctionDate || raw.date || "";
  mapped["Sale Finalized (Yes/No)"] = "Yes";

  const salePrice = raw.salePrice || raw.amount || "";
  const openingBid = raw.openingBid || "";

  mapped["Sale Price"] = salePrice;
  mapped["Opening / Minimum Bid"] = openingBid;

  const sale = parseCurrency(salePrice);
  const open = parseCurrency(openingBid);
  const estimatedSurplus = sale !== null && open !== null ? sale - open : null;

  if (sale === null) {
    anomalies.push({
      type: "MissingSalePrice",
      message: "Sold auction missing sale price",
      parcelId: raw.parcelId,
      caseNumber: raw.caseNumber,
      sourceUrl: raw.sourceUrl
    });
  }

  if (estimatedSurplus === null) {
    anomalies.push({
      type: "MissingSurplus",
      message: "Could not compute surplus",
      parcelId: raw.parcelId,
      caseNumber: raw.caseNumber,
      sourceUrl: raw.sourceUrl
    });
  }

  mapped["Estimated Surplus"] = estimatedSurplus !== null ? String(estimatedSurplus) : "";
  mapped["Final Estimated Surplus to Owner"] = estimatedSurplus !== null ? String(estimatedSurplus) : "";

  const meetsMinimum = estimatedSurplus !== null && estimatedSurplus >= MIN_SURPLUS;
  mapped["Meets Minimum Surplus? (Yes/No)"] = yn(meetsMinimum ? "Yes" : "No");
  mapped["Deal Viable? (Yes/No)"] = meetsMinimum ? "Yes" : "No";

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
// Retry wrapper
// =========================
async function withRetry(fn, retries = RETRY_MAX, baseMs = RETRY_BASE_MS) {
  for (let i = 0; i <= retries; i++) {
    try {
      return await fn();
    } catch (err) {
      const retriable = err && (err.code === 429 || (err.code >= 500 && err.code < 600));
      if (!retriable || i === retries) throw err;
      const wait = baseMs * Math.pow(2, i);
      console.warn(`Retry ${i + 1}/${retries} after ${wait}ms due to error: ${err.message || err}`);
      await new Promise(r => setTimeout(r, wait));
    }
  }
}

// =========================
// Fetch existing keys from sheet for idempotency (optional)
// Builds keys using Parcel / APN Number + Case Number + Auction Date if present
// =========================
async function fetchExistingKeys() {
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME_RAW}!A2:Z`,
    });
    const rows = res.data.values || [];
    // find header indices by matching HEADERS in the sheet header row if present
    // We assume the sheet uses the same HEADERS order; if not, we still attempt to map by header names
    // Build keys from columns: Parcel / APN Number, Case Number, Auction Date
    const keys = new Set();
    for (const r of rows) {
      const parcel = r[5] || ""; // HEADERS index 5 -> Parcel / APN Number
      const caseNum = r[6] || ""; // index 6 -> Case Number
      const date = r[7] || ""; // index 7 -> Auction Date
      const key = `${(parcel || "").toString().trim()}|${(caseNum || "").toString().trim()}|${(date || "").toString().trim()}`;
      if (key !== "||") keys.add(key);
    }
    console.log(`🔎 Fetched ${keys.size} existing keys from sheet for idempotency`);
    return keys;
  } catch (err) {
    console.warn("⚠️ Could not fetch existing sheet rows for idempotency:", err.message || err);
    return new Set();
  }
}

// =========================
// Append rows batched
// =========================
async function appendRowsBatched(rows) {
  if (!rows.length) {
    console.log("⚠️ No mapped rows to append.");
    return;
  }
  const batches = [];
  for (let i = 0; i < rows.length; i += BATCH_SIZE) batches.push(rows.slice(i, i + BATCH_SIZE));
  for (let i = 0; i < batches.length; i++) {
    const values = batches[i].map(row => HEADERS.map(h => row[h] || ""));
    await withRetry(() => sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: SHEET_NAME_RAW,
      valueInputOption: "RAW",
      insertDataOption: "INSERT_ROWS",
      requestBody: { values },
    }));
    console.log(`✅ Appended batch ${i + 1}/${batches.length} (${values.length} rows)`);
    // small throttle to avoid quota bursts
    await new Promise(r => setTimeout(r, 200));
  }
}

// =========================
// MAIN
// =========================
(async () => {
  try {
    if (!fs.existsSync(INPUT_FILE)) {
      console.error(`❌ Input file not found: ${INPUT_FILE}`);
      process.exit(1);
    }

    const rawData = JSON.parse(fs.readFileSync(INPUT_FILE, "utf8"));
    console.log(`📦 Loaded ${rawData.length} parsed rows from ${INPUT_FILE}`);

    const urlMapping = await getUrlMapping();
    console.log(`🌐 Fetched ${Object.keys(urlMapping).length} URL → County/State mappings`);

    const anomalies = [];
    const uniqueMap = new Map();

    // Optional: fetch existing keys from sheet to avoid appending duplicates
    const existingKeys = ENABLE_SHEET_IDEMPOTENCY ? await fetchExistingKeys() : new Set();

    rawData.forEach(raw => {
      // build dedupe key consistent with extractor: baseUrl|caseNumber|parcelId
      const baseUrl = (raw.sourceUrl || "").split("&page=")[0];
      const key = `${baseUrl}|${raw.caseNumber || ""}|${raw.parcelId || ""}`;

      // also build sheet-key for idempotency check (parcel|case|date)
      const sheetKey = `${(raw.parcelId || "").toString().trim()}|${(raw.caseNumber || "").toString().trim()}|${(raw.auctionDate || raw.date || "").toString().trim()}`;

      if (uniqueMap.has(key)) return; // local dedupe
      if (existingKeys.has(sheetKey)) {
        // skip rows already present in sheet
        console.log(`⏭ Skipping already-present row (sheet idempotency): ${sheetKey}`);
        return;
      }

      const mapped = mapRow(raw, urlMapping, anomalies);
      if (mapped) uniqueMap.set(key, mapped);
    });

    const mappedRows = [...uniqueMap.values()];

    if (mappedRows.length) {
      console.log("🧪 Sample mapped row preview:", mappedRows[0]);
    } else {
      console.log("ℹ️ No mapped rows to append after dedupe/idempotency checks.");
    }

    fs.writeFileSync(OUTPUT_FILE, JSON.stringify(mappedRows, null, 2));
    console.log(`💾 Saved mapped rows → ${OUTPUT_FILE}`);

    if (anomalies.length) {
      fs.writeFileSync(ANOMALY_FILE, JSON.stringify(anomalies, null, 2));
      console.log(`⚠️ Saved ${anomalies.length} anomalies → ${ANOMALY_FILE}`);
    }

    // Append to sheet in batches with retry/backoff
    if (mappedRows.length) {
      await appendRowsBatched(mappedRows);
    }

    console.log("🏁 Done.");
  } catch (err) {
    console.error("❌ Fatal error in mappingScraper:", err.message || err);
    process.exit(1);
  }
})();