/**
 * titleSearch.cjs
 * Dallas Title Search Automation (loop version)
 * - Reads ALL rows from Google Sheets
 * - Loops through each row
 * - Builds title search URL per row
 * - Opens page via Puppeteer
 * - Waits 60 seconds
 * - Saves full‑page screenshot
 */

const { google } = require("googleapis");
const fs = require("fs");
const path = require("path");
const puppeteer = require("puppeteer");

// ----------------------------
// CONFIG
// ----------------------------
const SHEET_ID = "1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA";
const RANGE = "raw_main!R2:N"; // Pull all columns needed in one go

// ----------------------------
// Google Sheets Auth
// ----------------------------
async function getSheets() {
  const auth = new google.auth.GoogleAuth({
    keyFile: "service-account.json",
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
  });

  return google.sheets({ version: "v4", auth });
}

// ----------------------------
// Convert date mm/dd/yyyy → yyyymmdd
// ----------------------------
function formatDate(input) {
  if (!input) return "";
  const parts = input.split("/"); // prevents timezone problems
  if (parts.length !== 3) return "";

  const [mm, dd, yyyy] = parts;
  return `${yyyy}${mm.padStart(2, "0")}${dd.padStart(2, "0")}`;
}

// ----------------------------
// Build URL
// ----------------------------
function buildDallasURL(recDate, aucDate, ownerName) {
  return (
    "https://dallas.tx.publicsearch.us/results?department=RP" +
    "&keywordSearch=false" +
    `&recordedDateRange=${recDate}%2C${aucDate}` +
    "&searchOcrText=false&searchType=quickSearch" +
    `&searchValue=${encodeURIComponent(ownerName)}`
  );
}

// ----------------------------
// Puppeteer Navigation + Screenshot
// ----------------------------
async function captureScreenshot(url, index) {
  const browser = await puppeteer.launch({
    headless: "new",
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  const page = await browser.newPage();
  await page.goto(url, {
    waitUntil: "networkidle2",
    timeout: 60000,
  });

  // WAIT 60 seconds before screenshot
  await page.waitForTimeout(60000);

  const outDir = "artifacts";
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir);

  const filename = `title_search_row_${index + 2}.png`; // row number aligns with sheet
  const filePath = path.join(outDir, filename);

  await page.screenshot({ path: filePath, fullPage: true });

  await browser.close();
  return filePath;
}

// ----------------------------
// MAIN LOOP
// ----------------------------
async function main() {
  console.log("Reading Google Sheet...");
  const sheets = await getSheets();

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: RANGE,
  });

  const rows = res.data.values || [];
  if (rows.length === 0) {
    console.log("No data found.");
    return;
  }

  console.log(`Found ${rows.length} rows. Processing...`);

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const recDateRaw = row[0];
    const aucDateRaw = row[1];
    const ownerNameRaw = row[2];

    // Stop if all key values are blank
    if (!recDateRaw && !aucDateRaw && !ownerNameRaw) continue;

    const recDate = formatDate(recDateRaw);
    const aucDate = formatDate(aucDateRaw);
    const ownerName = (ownerNameRaw || "").trim();

    const url = buildDallasURL(recDate, aucDate, ownerName);

    console.log(
      `Row ${i + 2}: Rec=${recDateRaw}, Auc=${aucDateRaw}, Owner=${ownerName}`
    );
    console.log(`→ URL: ${url}`);

    const screenshot = await captureScreenshot(url, i);
    console.log(`Saved screenshot: ${screenshot}`);
  }

  console.log("All rows processed.");
}

// Run
main().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});