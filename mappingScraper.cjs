// mappingScraper.cjs
// Requires: npm install googleapis

const fs = require("fs");
const { google } = require("googleapis");

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = "./service-account.json";
const SPREADSHEET_ID = "1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA";
const SHEET_NAME_URLS = "web_tda"; // where URLs, counties, states are
const SHEET_NAME_RAW = "raw_main"; // where we append mapped rows
const INPUT_FILE = process.argv[2] || "raw-scrape.json"; // JSON artifact

// =========================
// GOOGLE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

// =========================
// HEADERS (TSSF-COMPLIANT)
// =========================
const HEADERS = [
  "State","County","Property Address","City","ZIP Code","Parcel / APN Number","Case Number",
  "Auction Date","Sale Finalized (Yes/No)","Sale Price","Opening / Minimum Bid","Estimated Surplus","Meets Minimum Surplus? (Yes/No)",
  "Last Owner Name (as on Deed)","Additional Owner(s)","Ownership Type","Deed Type","Owner Deed Recording Date","Owner Deed Instrument #",
  "Mortgage Lender Name","Mortgage Amount","Mortgage Recording Date","Mortgage Satisfied? (Yes/No)","Mortgage Release Recording #","Mortgage Still Owed Amount",
  "Lien / Judgment Type","Creditor Name","Lien Amount","Lien Recording Date","Lien Expired? (Yes/No)","Lien Satisfied? (Yes/No)",
  "Total Open Debt","Final Estimated Surplus to Owner","Deal Viable? (Yes/No)",
  "Ownership Deed Collected? (Yes/No)","Foreclosure Deed Collected? (Yes/No)","Proof of Sale Collected? (Yes/No)","Debt Search Screenshot Collected? (Yes/No)","Tax Assessor Page Collected? (Yes/No)","File Complete? (Yes/No)",
  "File Submitted? (Yes/No)","Submission Date","Accepted / Rejected","Kickback Reason","Researcher Name","Notes"
];

// =========================
// FUNCTION: County/State mapping from sheet
// =========================
async function getUrlMapping() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME_URLS}!A2:C`, // County | State | URL
  });

  const rows = res.data.values || [];
  const mapping = {};

  rows.forEach(([county, state, url]) => {
    if (url) mapping[url.trim()] = { county: county || "", state: state || "" };
  });

  return mapping;
}

// =========================
// FUNCTION: Normalize Yes/No
// =========================
function yn(val) {
  if (val === true || val === "Yes") return "Yes";
  if (val === false || val === "No") return "No";
  return "";
}

// =========================
// FUNCTION: Map raw scrape row → TSSF headers
// =========================
function mapRow(raw, urlMapping) {
  const mapped = {};
  HEADERS.forEach(h => (mapped[h] = "")); // initialize all headers

  // Basic mappings
  mapped["Parcel / APN Number"] = raw.apn || "";
  mapped["Auction Date"] = raw.saleDate || raw.auctionDate || "";
  mapped["Opening / Minimum Bid"] = raw.openingBid || "";
  mapped["Sale Price"] = raw.winningBid || "";
  mapped["Case Number"] = raw.caseNumber || raw.id || "";
  mapped["Notes"] = raw.notes || "";

  // Surplus fields
  if (raw.surplus !== undefined && raw.surplus !== null) {
    mapped["Estimated Surplus"] = String(raw.surplus);
  }
  mapped["Meets Minimum Surplus? (Yes/No)"] = yn(raw.meetsMinimumSurplus);

  // State/County
  const url = raw.sourceUrl || "";
  const geo = urlMapping[url] || { county: "", state: "" };
  mapped["State"] = geo.state;
  mapped["County"] = geo.county;

  // Property Address
  if (raw.propertyAddress) {
    mapped["Property Address"] = raw.propertyAddress;
  }

  return mapped;
}

// =========================
// FUNCTION: Validate mapped row
// =========================
function validateRow(row, index) {
  const issues = [];

  // Critical fields
  if (!row["Parcel / APN Number"]) issues.push("Missing APN");
  if (!row["Auction Date"]) issues.push("Missing Auction Date");
  if (!row["Case Number"]) issues.push("Missing Case Number");

  // Surplus consistency
  if (row["Estimated Surplus"] && !row["Meets Minimum Surplus? (Yes/No)"]) {
    issues.push("Surplus present but MeetsMin flag missing");
  }

  // Address sanity
  if (!row["Property Address"]) issues.push("Missing Property Address");

  if (issues.length > 0) {
    console.warn(`⚠️ Row ${index + 1} validation issues: ${issues.join("; ")}`);
  }
  return issues.length === 0;
}

// =========================
// FUNCTION: Append rows to sheet
// =========================
async function appendRows(rows) {
  const values = rows.map(row => HEADERS.map(h => row[h] || ""));
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: SHEET_NAME_RAW,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values },
  });
  console.log(`✅ Appended ${values.length} mapped rows.`);
}

// =========================
// MAIN EXECUTION
// =========================
(async () => {
  if (!fs.existsSync(INPUT_FILE)) {
    console.error(`❌ Input file not found: ${INPUT_FILE}`);
    process.exit(1);
  }

  const rawData = JSON.parse(fs.readFileSync(INPUT_FILE, "utf8"));
  console.log(`📦 Loaded ${rawData.length} raw rows from ${INPUT_FILE}`);

  const urlMapping = await getUrlMapping();
  console.log(`🌐 Fetched ${Object.keys(urlMapping).length} URL → County/State mappings`);

  const mappedRows = rawData.map((raw, i) => {
    const mapped = mapRow(raw, urlMapping);
    validateRow(mapped, i);
    return mapped;
  });

  console.log("🧪 Sample mapped row preview:", mappedRows[0]);

  await appendRows(mappedRows);
  console.log("🏁 Done.");
})();