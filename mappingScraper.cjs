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
const INPUT_FILE = process.argv[2] || "parsed-auctions.json"; // JSON artifact from webInspector

// =========================
// GOOGLE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

// =========================
// HEADERS (TSSF-COMPLIANT, aligned with organizer_headers_tssf)
// =========================
const HEADERS = [
  // FILE IDENTIFICATION
  "State",
  "County",
  "Property Address",
  "City",
  "ZIP Code",
  "Parcel / APN Number",
  "Case Number",

  // SALE DETAILS
  "Auction Date",
  "Sale Finalized (Yes/No)",
  "Sale Price",
  "Opening / Minimum Bid",
  "Estimated Surplus",
  "Meets Minimum Surplus? (Yes/No)",

  // OWNERSHIP
  "Last Owner Name (as on Deed)",
  "Additional Owner(s)",
  "Ownership Type",
  "Deed Type",
  "Owner Deed Recording Date",
  "Owner Deed Instrument #",

  // MORTGAGES
  "Mortgage Lender Name",
  "Mortgage Amount",
  "Mortgage Recording Date",
  "Mortgage Satisfied? (Yes/No)",
  "Mortgage Release Recording #",
  "Mortgage Still Owed Amount",

  // LIENS / JUDGMENTS
  "Lien / Judgment Type",
  "Creditor Name",
  "Lien Amount",
  "Lien Recording Date",
  "Lien Expired? (Yes/No)",
  "Lien Satisfied? (Yes/No)",

  // FINAL CALCULATION
  "Total Open Debt",
  "Final Estimated Surplus to Owner",
  "Deal Viable? (Yes/No)",

  // DOCUMENTATION
  "Ownership Deed Collected? (Yes/No)",
  "Foreclosure Deed Collected? (Yes/No)",
  "Proof of Sale Collected? (Yes/No)",
  "Debt Search Screenshot Collected? (Yes/No)",
  "Tax Assessor Page Collected? (Yes/No)",
  "File Complete? (Yes/No)",

  // SUBMISSION STATUS
  "File Submitted? (Yes/No)",
  "Submission Date",
  "Accepted / Rejected",
  "Kickback Reason",
  "Researcher Name",
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
  if (typeof val === "string" && val.toLowerCase() === "yes") return "Yes";
  if (typeof val === "string" && val.toLowerCase() === "no") return "No";
  return "";
}

// =========================
// FUNCTION: Map parsed auction row → TSSF headers
// =========================
function mapRow(raw, urlMapping) {
  const mapped = {};
  HEADERS.forEach(h => (mapped[h] = "")); // initialize all headers

  const url = raw.sourceUrl || "";
  const geo = urlMapping[url] || { county: "", state: "" };

  // Geo
  mapped["State"] = geo.state;
  mapped["County"] = geo.county;

  // Basic auction fields
  mapped["Property Address"] = raw.propertyAddress || "";
  mapped["Parcel / APN Number"] = raw.parcelId || "";
  mapped["Case Number"] = raw.caseNumber || "";
  mapped["Auction Date"] = raw.auctionDate || "";

  // Sale details (Sold only in this pipeline)
  mapped["Sale Finalized (Yes/No)"] = "Yes";
  mapped["Sale Price"] = raw.salePrice || "";
  mapped["Opening / Minimum Bid"] = raw.openingBid || "";

  if (raw.surplus !== undefined && raw.surplus !== null) {
    mapped["Estimated Surplus"] = String(raw.surplus);
    mapped["Final Estimated Surplus to Owner"] = String(raw.surplus);
  }

  mapped["Meets Minimum Surplus? (Yes/No)"] = yn(raw.meetsMinimumSurplus);

  // Deal viability: Yes only if meets minimum surplus
  mapped["Deal Viable? (Yes/No)"] =
    yn(raw.meetsMinimumSurplus) === "Yes" ? "Yes" : "No";

  // Everything else stays blank or "No" by default
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
// FUNCTION: Append rows to sheet
// =========================
async function appendRows(rows) {
  if (!rows.length) {
    console.log("⚠️ No mapped rows to append.");
    return;
  }

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
  console.log(`📦 Loaded ${rawData.length} parsed rows from ${INPUT_FILE}`);

  const urlMapping = await getUrlMapping();
  console.log(`🌐 Fetched ${Object.keys(urlMapping).length} URL → County/State mappings`);

  const mappedRows = rawData.map(raw => mapRow(raw, urlMapping));
  console.log("🧪 Sample mapped row preview:", mappedRows[0]);

  await appendRows(mappedRows);
  console.log("🏁 Done.");
})();