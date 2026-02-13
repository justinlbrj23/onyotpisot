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
  const wb = xlsx.readFile(filePath);
  let allValues = [];
  wb.SheetNames.forEach(name => {
    const ws = wb.Sheets[name];
    const data = xlsx.utils.sheet_to_json(ws, { header: 1 });
    allValues.push(...cleanValues(data));
  });
  return uniqueCount(allValues);
}

// --------------------------------------
// GOOGLE SHEETS HANDLER
// --------------------------------------
async function countGoogleSheet(sheetId) {
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
  return uniqueCount(allValues);
}

// --------------------------------------
// MAIN
// --------------------------------------
async function run() {
  const excelFiles = ["./file1.xlsx", "./file2.xlsx"];
  const googleSheetIds = ["SHEET_ID_1", "SHEET_ID_2"];

  let grandTotal = 0;

  for (const file of excelFiles) {
    const count = countExcel(file);
    console.log(`📊 Excel ${file}: ${count}`);
    grandTotal += count;
  }

  for (const id of googleSheetIds) {
    const count = await countGoogleSheet(id);
    console.log(`📊 Google Sheet ${id}: ${count}`);
    grandTotal += count;
  }

  console.log(`🎯 Grand Total Unique Count: ${grandTotal}`);
}

run().catch(err => console.error("❌ Error:", err));