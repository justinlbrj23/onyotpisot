const { google } = require("googleapis");

// -------------------------
// CONFIG
// -------------------------
const SPREADSHEET_ID = "1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA";
const SHEET_NAME = "raw_main"; // change if needed
const SERVICE_ACCOUNT_FILE = "./service-account.json";

// -------------------------
// AUTH
// -------------------------
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});

const sheets = google.sheets({ version: "v4", auth });

// -------------------------
// HEADERS (FINAL, TSSF-COMPLIANT)
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
  // Write headers to row 1
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!A1`,
    valueInputOption: "RAW",
    resource: {
      values: [HEADERS],
    },
  });

  // Freeze header row
  const meta = await sheets.spreadsheets.get({
    spreadsheetId: SPREADSHEET_ID,
  });

  const sheetId = meta.data.sheets.find(
    s => s.properties.title === SHEET_NAME
  )?.properties.sheetId;

  if (sheetId === undefined) {
    throw new Error(`Sheet "${SHEET_NAME}" not found`);
  }

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    resource: {
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
      ],
    },
  });

  console.log("Headers organized successfully.");
  console.log(`Open sheet: https://docs.google.com/spreadsheets/d/${SPREADSHEET_ID}`);
}

organizeHeaders().catch(console.error);
