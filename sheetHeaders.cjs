// sheetHeaders.cjs
// Google Sheets header organizer (SAFE + PIPELINE-ALIGNED + DIAGNOSTIC)

// -------------------------
// IMPORTS
// -------------------------
const { google } = require("googleapis");
const path = require("path");
const fs = require("fs");

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
// HEADERS (AUTHORITATIVE – MUST MATCH mappingScraper.cjs)
// -------------------------
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

// -------------------------
// MAIN
// -------------------------
async function organizeHeaders() {
  try {
    // Fetch spreadsheet metadata
    const meta = await sheets.spreadsheets.get({
      spreadsheetId: SPREADSHEET_ID,
    });

    const sheet = meta.data.sheets.find(
      s => s.properties.title === SHEET_NAME
    );

    if (!sheet) {
      throw new Error(`❌ Sheet "${SHEET_NAME}" not found in spreadsheet.`);
    }

    const sheetId = sheet.properties.sheetId;

    // 1️⃣ Write headers to row 1 ONLY
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!A1`,
      valueInputOption: "RAW",
      requestBody: {
        values: [HEADERS],
      },
    });

    // 2️⃣ Freeze header row
    // 3️⃣ Auto-resize columns for readability
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: {
        requests: [
          {
            updateSheetProperties: {
              properties: {
                sheetId,
                gridProperties: {
                  frozenRowCount: 1,
                },
              },
              fields: "gridProperties.frozenRowCount",
            },
          },
          {
            autoResizeDimensions: {
              dimensions: {
                sheetId,
                dimension: "COLUMNS",
                startIndex: 0,
                endIndex: HEADERS.length,
              },
            },
          },
        ],
      },
    });

    console.log("✅ Headers organized, frozen, and auto-resized successfully.");
    console.log(
      `📊 Open sheet: https://docs.google.com/spreadsheets/d/${SPREADSHEET_ID}`
    );
  } catch (err) {
    console.error("❌ Header organization failed:");
    console.error(err.stack || err.message || err);
    process.exit(1);
  }
}

// -------------------------
// EXECUTE
// -------------------------
organizeHeaders();