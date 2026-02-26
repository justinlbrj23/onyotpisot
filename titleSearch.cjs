/**
 * titleSearch.cjs
 * Dallas Title Search Automation (loop version)
 * - Reads ALL rows from Google Sheets
 * - Loops through each row
 * - Builds title search URL per row
 * - Opens page via Puppeteer
 * - Waits 60 seconds (safe method)
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

/**
 * Pull all columns from H → R:
 * H = auc_date
 * N = owner_name
 * R = rec_date
 *
 * Range "H2:R" ensures correct column order.
 */
const RANGE = "raw_main!H2:R";

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
// Convert date mm/dd/yyyy → yyyymmdd (no timezone issues)
// ----------------------------
function formatDate(input) {
  if (!input) return "";

  const parts = input.split("/");
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

  // WAIT 60 seconds before screenshot (Puppeteer-safe)
  await new Promise((res) => setTimeout(res, 60000));

  const outDir = "artifacts";
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir);

  const filename = `title_search_row_${index + 2}.png`;
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

  const result = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: RANGE,
  });

  const rows = result.data.values || [];
  if (rows.length === 0) {
    console.log("No data found.");
    return;
  }

  console.log(`Found ${rows.length} rows. Processing...`);

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];

    // COLUMN MAPPING (based on RANGE = H2:R)
    const aucDateRaw = row[0];     // H column
    const ownerNameRaw = row[6];   // N column
    const recDateRaw = row[10];    // R column

    // Skip row if all 3 key fields are empty
    if (!recDateRaw && !aucDateRaw && !ownerNameRaw) continue;

    const recDate = formatDate(recDateRaw);
    const aucDate = formatDate(aucDateRaw);
    const ownerName = (ownerNameRaw || "").trim();

    const url = buildDallasURL(recDate, aucDate, ownerName);

    console.log(
      `Row ${i + 2}: Rec=${recDateRaw}, Auc=${aucDateRaw}, Owner=${ownerName}`
    );
    console.log(`→ URL: ${url}`);

    const screenshotPath = await captureScreenshot(url, i);
    console.log(`Saved screenshot: ${screenshotPath}`);
  }

  console.log("All rows processed.");
}

// Run
main().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});