// mappingScraper.cjs
const puppeteer = require("puppeteer");
const cheerio = require("cheerio");
const { google } = require("googleapis");

const SERVICE_ACCOUNT_FILE = "./service-account.json";
const SPREADSHEET_ID = "1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA";
const SHEET_NAME = "raw_main";
const TARGET_URL = "https://sacramento.mytaxsale.com/reports/total_sales";

const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

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

// -------------------------
// RULES: keyword/regex mapping
// -------------------------
const mappingRules = {
  "Parcel / APN Number": [/parcel/i, /APN/i],
  "ZIP Code": [/^\d{5}$/],
  "Sale Price": [/sale price/i, /\$\d+/],
  "Auction Date": [/auction date/i, /\d{2}\/\d{2}\/\d{4}/],
  "County": [/county/i],
  "State": [/california/i, /CA\b/],
  "Property Address": [/address/i],
  "Case Number": [/case/i],
  // … expand rules for other headers
};

// -------------------------
// Scrape page
// -------------------------
async function inspectPage(url) {
  const browser = await puppeteer.launch({ headless: "new", args: ["--no-sandbox"] });
  const page = await browser.newPage();
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 120000 });
  const html = await page.content();
  await browser.close();
  return cheerio.load(html);
}

// -------------------------
// Match text to header
// -------------------------
function matchHeader(text) {
  for (const [header, patterns] of Object.entries(mappingRules)) {
    for (const p of patterns) {
      if (p.test(text)) return header;
    }
  }
  return null;
}

// -------------------------
// Build row object
// -------------------------
function buildRow($) {
  const row = {};
  HEADERS.forEach(h => row[h] = ""); // initialize empty row

  $("*").each((_, el) => {
    const text = $(el).text().replace(/\s+/g, " ").trim();
    if (!text) return;
    const header = matchHeader(text);
    if (header) {
      row[header] = text;
      console.log(`Mapped "${text}" → ${header}`);
    }
  });

  return row;
}

// -------------------------
// Append to sheet
// -------------------------
async function appendRow(row) {
  const values = HEADERS.map(h => row[h] || "");
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: SHEET_NAME,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values: [values] },
  });
  console.log("✅ Row appended.");
}

// -------------------------
// MAIN
// -------------------------
(async () => {
  const $ = await inspectPage(TARGET_URL);
  const row = buildRow($);
  await appendRow(row);
})();