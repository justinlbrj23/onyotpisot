// mappingScraper.cjs
// Webpage → Google Sheets mapping scraper (SAFE + PIPELINE-ALIGNED + VALIDATION)

// -------------------------
// IMPORTS
// -------------------------
const { google } = require("googleapis");
const path = require("path");
const fs = require("fs");
const cheerio = require("cheerio"); // for HTML parsing
const fetch = require("node-fetch"); // lightweight HTTP client

// -------------------------
// CONFIG
// -------------------------
const SPREADSHEET_ID = "1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA";
const SHEET_NAME = "raw_main";
const SERVICE_ACCOUNT_FILE = path.resolve(__dirname, "./service-account.json");

// -------------------------
// AUTH
// -------------------------
if (!fs.existsSync(SERVICE_ACCOUNT_FILE)) {
  throw new Error(`❌ Service account file not found at ${SERVICE_ACCOUNT_FILE}`);
}

const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});

const sheets = google.sheets({ version: "v4", auth });

// -------------------------
// HEADERS (AUTHORITATIVE – MUST MATCH sheetHeaders.cjs)
// -------------------------
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

// -------------------------
// SCRAPER LOGIC
// -------------------------
async function scrapePage(url) {
  console.log(`🌐 Fetching: ${url}`);
  const res = await fetch(url);
  if (!res.ok) throw new Error(`❌ Failed to fetch ${url}: ${res.statusText}`);
  const html = await res.text();
  const $ = cheerio.load(html);

  // Example mapping (adjust selectors per site)
  const row = {
    State: $("span.state").text().trim(),
    County: $("span.county").text().trim(),
    PropertyAddress: $("div.address").text().trim(),
    City: $("span.city").text().trim(),
    ZIPCode: $("span.zip").text().trim(),
    ParcelAPNNumber: $("span.apn").text().trim(),
    CaseNumber: $("span.case").text().trim(),
    AuctionDate: $("span.auction-date").text().trim(),
    SaleFinalized: $("span.sale-finalized").text().trim(),
    SalePrice: $("span.sale-price").text().trim(),
    OpeningMinimumBid: $("span.min-bid").text().trim(),
    EstimatedSurplus: $("span.surplus").text().trim(),
    MeetsMinimumSurplus: $("span.meets-surplus").text().trim(),
    // … continue mapping all fields
  };

  return row;
}

// -------------------------
// VALIDATION
// -------------------------
function validateRow(row) {
  const keys = Object.keys(row);
  const mismatches = [];

  HEADERS.forEach(h => {
    // Normalize header key to object property naming
    const normalized = h
      .replace(/[()]/g, "")
      .replace(/\s+/g, "")
      .replace(/\//g, "")
      .replace(/\?/g, "")
      .replace(/-/g, "")
      .replace(/#/g, "")
      .replace(/:/g, "")
      .replace(/YesNo/g, "YesNo"); // keep Yes/No consistent

    if (!keys.some(k => k.toLowerCase().includes(normalized.toLowerCase()))) {
      mismatches.push(h);
    }
  });

  if (mismatches.length > 0) {
    console.log("⚠️ Validation: Missing mappings for headers:");
    mismatches.forEach(m => console.log(`   - ${m}`));
  } else {
    console.log("✅ Validation: All headers mapped.");
  }
}

// -------------------------
// WRITE TO SHEET
// -------------------------
async function appendRow(row) {
  const values = HEADERS.map(h => row[h] || "");
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: SHEET_NAME,
    valueInputOption: "RAW",
    requestBody: {
      values: [values],
    },
  });
  console.log("✅ Row appended successfully.");
}

// -------------------------
// MAIN
// -------------------------
async function run() {
  try {
    const url = process.argv[2];
    if (!url) throw new Error("❌ No URL provided. Usage: node mappingScraper.cjs <url>");

    const row = await scrapePage(url);
    validateRow(row);
    await appendRow(row);

    console.log(
      `📊 Open sheet: https://docs.google.com/spreadsheets/d/${SPREADSHEET_ID}`
    );
  } catch (err) {
    console.error("❌ Scraper failed:");
    console.error(err.stack || err.message || err);
    process.exit(1);
  }
}

run();