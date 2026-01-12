// mappingScraper.cjs
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

// =========================
// GOOGLE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

// =========================
// HEADERS
// =========================
const HEADERS = [
  "State",
  "County",
  "Property Address",
  "City",
  "ZIP Code",
  "Parcel / APN Number",
  "Case Number",
  "Auction Date",
  "Sale Finalized (Yes/No)",
  "Sale Price",
  "Opening / Minimum Bid",
  "Estimated Surplus",
  "Meets Minimum Surplus? (Yes/No)",
  "Last Owner Name (as on Deed)",
  "Additional Owner(s)",
  "Ownership Type",
  "Deed Type",
  "Owner Deed Recording Date",
  "Owner Deed Instrument #",
  "Mortgage Lender Name",
  "Mortgage Amount",
  "Mortgage Recording Date",
  "Mortgage Satisfied? (Yes/No)",
  "Mortgage Release Recording #",
  "Mortgage Still Owed Amount",
  "Lien / Judgment Type",
  "Creditor Name",
  "Lien Amount",
  "Lien Recording Date",
  "Lien Expired? (Yes/No)",
  "Lien Satisfied? (Yes/No)",
  "Total Open Debt",
  "Final Estimated Surplus to Owner",
  "Deal Viable? (Yes/No)",
  "Ownership Deed Collected? (Yes/No)",
  "Foreclosure Deed Collected? (Yes/No)",
  "Proof of Sale Collected? (Yes/No)",
  "Debt Search Screenshot Collected? (Yes/No)",
  "Tax Assessor Page Collected? (Yes/No)",
  "File Complete? (Yes/No)",
  "File Submitted? (Yes/No)",
  "Submission Date",
  "Accepted / Rejected",
  "Kickback Reason",
  "Researcher Name",
];

// =========================
// HELPERS
// =========================
function yn(val) {
  if (val === true || val === "Yes") return "Yes";
  if (val === false || val === "No") return "No";
  return "";
}

function buildUniqueKey(row) {
  return [
    row["State"],
    row["County"],
    row["Parcel / APN Number"],
    row["Case Number"],
    row["Auction Date"],
  ]
    .map(v => (v || "").toString().trim().toLowerCase())
    .join("|");
}

// =========================
// LOAD URL → COUNTY/STATE
// =========================
async function getUrlMapping() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME_URLS}!A2:C`,
  });

  const mapping = {};
  (res.data.values || []).forEach(([county, state, url]) => {
    if (url) mapping[url.trim()] = { county, state };
  });

  return mapping;
}

// =========================
// LOAD EXISTING ROW KEYS
// =========================
async function getExistingKeys() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME_RAW}!A2:Z`,
  });

  const rows = res.data.values || [];
  const keys = new Set();

  rows.forEach(row => {
    const obj = {};
    HEADERS.forEach((h, i) => (obj[h] = row[i] || ""));
    keys.add(buildUniqueKey(obj));
  });

  return keys;
}

// =========================
// MAP RAW → SHEET ROW
// =========================
function mapRow(raw, urlMapping) {
  const row = {};
  HEADERS.forEach(h => (row[h] = ""));

  const geo = urlMapping[raw.sourceUrl || ""] || {};

  row["State"] = geo.state || "";
  row["County"] = geo.county || "";
  row["Property Address"] = raw.propertyAddress || "";
  row["Parcel / APN Number"] = raw.parcelId || "";
  row["Case Number"] = raw.caseNumber || "";
  row["Auction Date"] = raw.auctionDate || "";

  row["Sale Finalized (Yes/No)"] = "Yes";
  row["Sale Price"] = raw.salePrice || "";
  row["Opening / Minimum Bid"] = raw.openingBid || "";

  if (raw.surplus != null) {
    row["Estimated Surplus"] = String(raw.surplus);
    row["Final Estimated Surplus to Owner"] = String(raw.surplus);
  }

  row["Meets Minimum Surplus? (Yes/No)"] = yn(raw.meetsMinimumSurplus);
  row["Deal Viable? (Yes/No)"] =
    yn(raw.meetsMinimumSurplus) === "Yes" ? "Yes" : "No";

  row["Ownership Deed Collected? (Yes/No)"] = "No";
  row["Foreclosure Deed Collected? (Yes/No)"] = "No";
  row["Proof of Sale Collected? (Yes/No)"] = "No";
  row["Debt Search Screenshot Collected? (Yes/No)"] = "No";
  row["Tax Assessor Page Collected? (Yes/No)"] = "No";
  row["File Complete? (Yes/No)"] = "No";
  row["File Submitted? (Yes/No)"] = "No";

  return row;
}

// =========================
// APPEND NON-DUPLICATES
// =========================
async function appendRows(rows) {
  if (!rows.length) {
    console.log("⚠️ No new rows to append.");
    return;
  }

  const values = rows.map(r => HEADERS.map(h => r[h] || ""));
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: SHEET_NAME_RAW,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values },
  });

  console.log(`✅ Appended ${rows.length} new rows.`);
}

// =========================
// MAIN
// =========================
(async () => {
  if (!fs.existsSync(INPUT_FILE)) {
    console.error(`❌ Missing input file: ${INPUT_FILE}`);
    process.exit(1);
  }

  const rawData = JSON.parse(fs.readFileSync(INPUT_FILE, "utf8"));
  console.log(`📦 Loaded ${rawData.length} parsed rows`);

  const urlMapping = await getUrlMapping();
  const existingKeys = await getExistingKeys();

  console.log(`🔐 Loaded ${existingKeys.size} existing row keys`);

  const mapped = rawData.map(r => mapRow(r, urlMapping));

  const newRows = [];
  let skipped = 0;

  for (const row of mapped) {
    const key = buildUniqueKey(row);
    if (existingKeys.has(key)) {
      skipped++;
    } else {
      existingKeys.add(key);
      newRows.push(row);
    }
  }

  console.log(`⏭️ Skipped ${skipped} duplicate rows`);
  await appendRows(newRows);
  console.log("🏁 Done.");
})();