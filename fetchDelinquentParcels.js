import fs from "fs";
import { google } from "googleapis";
import xlsx from "xlsx";

// --------------------------------------
// GOOGLE AUTH
// --------------------------------------
const auth = new google.auth.GoogleAuth({
  keyFile: "./service-account.json",
  scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"]
});
const sheets = google.sheets({ version: "v4", auth });

// --------------------------------------
// HELPERS
// --------------------------------------
function cleanValues(values) {
  return values
    .flat(Infinity)                // flatten nested arrays
    .map(v => (v || "").toString().trim())
    .filter(v => v.length > 0);    // remove blanks
}

function uniqueCount(values) {
  return new Set(values).size;
}

// --------------------------------------
// EXCEL HANDLER
// --------------------------------------
function countExcel(filePath) {
  if (!fs.existsSync(filePath)) {
    console.warn(`⚠️ Skipping missing file: ${filePath}`);
    return 0;
  }

  const wb = xlsx.readFile(filePath);
  let allValues = [];
  wb.SheetNames.forEach(name => {
    const ws = wb.Sheets[name];
    const data = xlsx.utils.sheet_to_json(ws, { header: 1 });
    allValues.push(...cleanValues(data));
  });
  const count = uniqueCount(allValues);
  console.log(`📊 Excel ${filePath}: ${count}`);
  return count;
}

// --------------------------------------
// GOOGLE SHEETS HANDLER
// --------------------------------------
async function countGoogleSheet(sheetId) {
  try {
    const meta = await sheets.spreadsheets.get({ spreadsheetId: sheetId });
    let allValues = [];
    for (const sheet of meta.data.sheets) {
      const name = sheet.properties.title;
      const res = await sheets.spreadsheets.values.get({
        spreadsheetId: sheetId,
        range: name
      });
      const values = res.data.values || [];
      allValues.push(...cleanValues(values));
    }
    const count = uniqueCount(allValues);
    console.log(`📊 Google Sheet ${sheetId}: ${count}`);
    return count;
  } catch (err) {
    console.error(`❌ Error reading Google Sheet ${sheetId}:`, err.message);
    return 0;
  }
}

// --------------------------------------
// MAIN
// --------------------------------------
async function run() {
  // ✅ Only include files you actually have
  const excelFiles = ["./file1.xlsx"]; 
  const googleSheetIds = ["YOUR_GOOGLE_SHEET_ID"]; // replace with real IDs

  let grandTotal = 0;

  // Excel files
  for (const file of excelFiles) {
    grandTotal += countExcel(file);
  }

  // Google Sheets
  for (const id of googleSheetIds) {
    grandTotal += await countGoogleSheet(id);
  }

  console.log(`🎯 Grand Total Unique Count: ${grandTotal}`);
}

run().catch(err => console.error("❌ Fatal Error:", err));