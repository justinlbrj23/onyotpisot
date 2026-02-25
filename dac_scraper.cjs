// dac_scraper.cjs
// Dallas CAD Parcel Scraper (Artifact Mode, PDFKit)
// Requires: npm install puppeteer cheerio googleapis pdfkit

const puppeteer = require("puppeteer");
const fs = require("fs");
const cheerio = require("cheerio");
const PDFDocument = require("pdfkit");
const path = require("path");
const { google } = require("googleapis");

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = "./service-account.json";
const SPREADSHEET_ID = "1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA";
const SHEET_NAME = "raw_main";
const PARCEL_RANGE = "F2:F";
const YEAR_RANGE = "H2:H";
const OWNER_OUTPUT_COL = "N";

const TARGET_URL_1 = "https://www.dallascad.org/AcctDetailRes.aspx?ID=";
const TARGET_URL_2 = "https://www.dallascad.org/AcctHistory.aspx?ID=";

// Ensure artifacts directory exists
if (!fs.existsSync("./artifacts")) {
  fs.mkdirSync("./artifacts");
}

// =========================
// GOOGLE AUTH (Sheets Only)
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});

const sheets = google.sheets({ version: "v4", auth });

// =========================
// Load PARCEL IDs + Years
// =========================
async function loadParcelData() {
  const parcelsRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!${PARCEL_RANGE}`,
  });
  const yearsRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!${YEAR_RANGE}`,
  });

  const parcels = (parcelsRes.data.values || []).flat().map(v => (v || "").trim());
  const years = (yearsRes.data.values || []).flat().map(v => (v || "").trim());

  const items = [];
  for (let i = 0; i < parcels.length; i++) {
    if (parcels[i]) {
      items.push({
        parcelId: parcels[i],
        auctionYear: years[i] || "",
        rowNum: i + 2
      });
    }
  }
  return items;
}

// =========================
// Owner Extraction Logic
// =========================
function extractOwnerName(html, auctionYear) {
  const $ = cheerio.load(html);
  let owner = "";

  let yearPattern = auctionYear;
  const m = auctionYear.match(/\d{4}/);
  if (m) yearPattern = m[0];

  const tableCell = $("table td").filter((i, el) => {
    return $(el).text().includes(yearPattern);
  }).first();

  if (tableCell.length) {
    const fullText = tableCell.text().trim();
    const lines = fullText.split("\n").map(l => l.trim()).filter(Boolean);

    // Owner name is typically on second line
    if (lines.length > 1) {
      owner = lines[1].replace(/\s+/g, " ").trim();
    }
  }

  return owner;
}

// =========================
// Build PDF for each parcel
// =========================
function createPDF(parcelId, detailScreenshot, historyScreenshot) {
  const pdfPath = path.join("artifacts", `parcel_${parcelId}.pdf`);
  const doc = new PDFDocument({ autoFirstPage: false });
  const stream = fs.createWriteStream(pdfPath);
  doc.pipe(stream);

  const img1 = doc.openImage(detailScreenshot);
  doc.addPage({ size: [img1.width, img1.height] });
  doc.image(img1, 0, 0);

  const img2 = doc.openImage(historyScreenshot);
  doc.addPage({ size: [img2.width, img2.height] });
  doc.image(img2, 0, 0);

  doc.end();
  return pdfPath;
}

// =========================
// MAIN PROCESS
// =========================
(async () => {
  console.log("📥 Loading parcel data...");
  const parcelData = await loadParcelData();
  console.log(`📄 Found ${parcelData.length} parcels`);

  const browser = await puppeteer.launch({
  headless: "new",   // or true
  args: [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-blink-features=AutomationControlled",
    "--ignore-certificate-errors",
    "--disable-gpu",
    "--single-process",
    "--no-zygote"
  ]
});

  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
      "AppleWebKit/537.36 (KHTML, like Gecko) " +
      "Chrome/121.0.0.0 Safari/537.36"
  );

  await page.setExtraHTTPHeaders({
    Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9"
  });

  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => false });
  });

  // Process each parcel
  for (const { parcelId, auctionYear, rowNum } of parcelData) {
    console.log("\n==============================");
    console.log(`📌 Parcel: ${parcelId} | Auction Year: ${auctionYear}`);

    // DETAIL PAGE
    const url1 = TARGET_URL_1 + parcelId;
    console.log("➡️ Navigating:", url1);
    await page.goto(url1, { waitUntil: "domcontentloaded" });
    await new Promise(r => setTimeout(r, 3000));
    const detailFile = `detail_${parcelId}.jpg`;
    await page.screenshot({ path: detailFile, fullPage: true });

    // HISTORY PAGE
    const url2 = TARGET_URL_2 + parcelId;
    console.log("➡️ Navigating:", url2);
    await page.goto(url2, { waitUntil: "domcontentloaded" });
    await new Promise(r => setTimeout(r, 3000));
    const html2 = await page.content();
    const historyFile = `history_${parcelId}.jpg`;
    await page.screenshot({ path: historyFile, fullPage: true });

    // Extract Owner
    const ownerName = extractOwnerName(html2, auctionYear);
    console.log(`👤 Owner Extracted: ${ownerName}`);

    // Update Sheet
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!${OWNER_OUTPUT_COL}${rowNum}`,
      valueInputOption: "RAW",
      requestBody: { values: [[ownerName]] }
    });
    console.log("📌 Sheet updated");

    // PDF
    const pdfPath = createPDF(parcelId, detailFile, historyFile);
    console.log("📄 PDF Generated:", pdfPath);

    // Clean temp images
    fs.unlinkSync(detailFile);
    fs.unlinkSync(historyFile);
  }

  await browser.close();
  console.log("\n🏁 DONE — All parcels processed. PDFs ready for GitHub artifacts.");
})();