// mappingScraper.cjs
// Requires:
// npm install googleapis

const fs = require("fs");
const { google } = require("googleapis");

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = "./service-account.json";
const SPREADSHEET_ID = "1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA";
const SHEET_NAME = "raw_main";
const INPUT_FILE = process.argv[2] || "raw-scrape.json"; // JSON artifact from inspectWebpage.cjs

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
  "File Submitted? (Yes/No)","Submission Date","Accepted / Rejected","Kickback Reason","Researcher Name"
];

// =========================
// FUNCTION: Map raw scrape row → TSSF headers
// =========================
function mapRow(raw) {
  const mapped = {};
  HEADERS.forEach(h => (mapped[h] = "")); // initialize all headers

  // Basic mappings from table fields
  mapped["Parcel / APN Number"] = raw.apn;
  mapped["Auction Date"] = raw.saleDate;
  mapped["Opening / Minimum Bid"] = raw.openingBid;
  mapped["Sale Price"] = raw.winningBid;
  mapped["Case Number"] = raw.id;
  mapped["Notes"] = raw.notes || "";

  // Use surplus fields directly from raw JSON
  if (raw.surplus !== undefined && raw.surplus !== null) {
    mapped["Estimated Surplus"] = raw.surplus.toString();
  }
  if (raw.meetsMinimumSurplus) {
    mapped["Meets Minimum Surplus? (Yes/No)"] = raw.meetsMinimumSurplus;
  }

  // Static context (since site is Sacramento County, CA)
  mapped["State"] = "California";
  mapped["County"] = "Sacramento";

  return mapped;
}

// =========================
// FUNCTION: Append rows to sheet
// =========================
async function appendRows(rows) {
  const values = rows.map(row => HEADERS.map(h => row[h] || ""));
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: SHEET_NAME,
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

  const mappedRows = rawData.map(mapRow);
  console.log("🧪 Sample mapped row:", mappedRows[0]);

  await appendRows(mappedRows);
  console.log("🏁 Done.");
})();