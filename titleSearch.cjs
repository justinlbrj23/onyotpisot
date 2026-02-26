/**
 * titleSearch.cjs
 * Dallas Title Search Automation
 * - Reads values from Google Sheets
 * - Builds title search URL
 * - Opens page using Puppeteer
 * - Captures full‑page screenshot
 */

const { google } = require("googleapis");
const fetch = require("node-fetch");
const fs = require("fs");
const path = require("path");
const puppeteer = require("puppeteer");

// ----------------------------
// CONFIG
// ----------------------------
const SHEET_ID = "1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA";
const RANGE_REC = "raw_main!R2:R";
const RANGE_AUC = "raw_main!H2:H";
const RANGE_OWNER = "raw_main!N2:N";

// ----------------------------
// 1. Create Google Sheets Client
// ----------------------------
async function getSheets() {
  const auth = new google.auth.GoogleAuth({
    keyFile: "service-account.json",
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
  });

  return google.sheets({ version: "v4", auth });
}

// ----------------------------
// 2. Read values from Google Sheet
// ----------------------------
async function getSheetValues(sheets) {
  async function get(range) {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range,
    });
    return res.data.values ? res.data.values[0][0] : "";
  }

  return {
    recDateRaw: await get(RANGE_REC),
    aucDateRaw: await get(RANGE_AUC),
    ownerNameRaw: await get(RANGE_OWNER),
  };
}

// ----------------------------
// 3. Convert "MM/DD/YYYY" → "YYYYMMDD"
// ----------------------------
function formatDate(input) {
  if (!input) return "";
  const d = new Date(input);
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}${mm}${dd}`;
}

// ----------------------------
// 4. Build Dallas Title Search URL
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
// 5. Navigate and Screenshot
// ----------------------------
async function captureScreenshot(url) {
  const browser = await puppeteer.launch({
    headless: "new",
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  const page = await browser.newPage();
  await page.goto(url, {
    waitUntil: "networkidle2",
    timeout: 60000,
  });

  const outDir = "artifacts";
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir);

  const outPath = path.join(outDir, "title_search_fullpage.png");

  await page.screenshot({
    path: outPath,
    fullPage: true,
  });

  await browser.close();
  return outPath;
}

// ----------------------------
// MAIN EXECUTION
// ----------------------------
async function main() {
  console.log("Loading Google Sheets…");
  const sheets = await getSheets();
  const { recDateRaw, aucDateRaw, ownerNameRaw } = await getSheetValues(sheets);

  console.log("Raw sheet values:", { recDateRaw, aucDateRaw, ownerNameRaw });

  const recDate = formatDate(recDateRaw);
  const aucDate = formatDate(aucDateRaw);
  const ownerName = ownerNameRaw.trim();

  const url = buildDallasURL(recDate, aucDate, ownerName);
  console.log("Generated URL:");
  console.log(url);

  console.log("Navigating and capturing screenshot…");
  const file = await captureScreenshot(url);

  console.log("Screenshot saved:", file);
}

main().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});