// dac_scraper.cjs
// Dallas CAD Parcel Scraper (Artifact Mode, PDFKit)
// Requires: npm install puppeteer cheerio googleapis pdfkit he

const puppeteer = require("puppeteer");
const fs = require("fs");
const PDFDocument = require("pdfkit");
const path = require("path");
const { google } = require("googleapis");
const cheerio = require('cheerio');
const he = require('he');

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
// Owner + Deed Date Extraction (Paired)
// =========================
function extractOwnerAndDeedDateFromHistoryPage(html, auctionYear) {
  const $ = cheerio.load(html);
  const auctionYearNum = auctionYear.match(/\d{4}/) ? auctionYear.match(/\d{4}/)[0] : auctionYear;
  const ownerTable = $('#pnlOwnHist table').first();
  if (!ownerTable.length) return { owner: '', deedDate: '' };

  const rows = ownerTable.find('tr').slice(1);

  let owner = '', deedDate = '';
  let rowFound = null;

  // Try to find the row matching the auction year
  rows.each(function () {
    const yearCell = $(this).find('th').first();
    const ownerCell = $(this).find('td').eq(0);
    const legalDescCell = $(this).find('td').eq(1);
    if (yearCell.length && ownerCell.length && legalDescCell.length) {
      const yearText = yearCell.text().trim();
      if (yearText.includes(auctionYearNum)) {
        rowFound = { ownerCell, legalDescCell };
        return false; // break loop
      }
    }
  });

  // Fallback: use first data row if no match
  if (!rowFound && rows.length > 0) {
    const ownerCell = $(rows[0]).find('td').eq(0);
    const legalDescCell = $(rows[0]).find('td').eq(1);
    rowFound = { ownerCell, legalDescCell };
  }

  if (rowFound && rowFound.ownerCell && rowFound.legalDescCell) {
    // Owner name
    const ownerHtml = rowFound.ownerCell.html() || '';
    const ownerNameRaw = ownerHtml.split(/<br\s*\/?>/i)[0].replace(/[\n\r]/g, '').trim();
    owner = he.decode(ownerNameRaw);

    // Deed Transfer Date (search in legalDescCell)
    const legalHtml = rowFound.legalDescCell.html() || '';
    // Try to match "Deed Transfer Date:" and get the next tag/text
    const deedDateMatch = legalHtml.match(/Deed Transfer Date:<\/span>\s*([0-9\/]+)/i);
    if (deedDateMatch) {
      deedDate = deedDateMatch[1].trim();
    } else {
      // fallback: try to match just the date pattern
      const dateOnlyMatch = legalHtml.match(/(\d{1,2}\/\d{1,2}\/\d{4})/);
      deedDate = dateOnlyMatch ? dateOnlyMatch[1] : '';
    }
  }

  return { owner, deedDate };
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
    const historyHtml = await page.content();
    const historyFile = `history_${parcelId}.jpg`;
    await page.screenshot({ path: historyFile, fullPage: true });

    // Extract Owner Name and Deed Transfer Date from the same row
    const { owner, deedDate } = extractOwnerAndDeedDateFromHistoryPage(historyHtml, auctionYear);
    console.log(`👤 Owner Extracted: ${owner}`);
    console.log(`📅 Deed Transfer Date: ${deedDate}`);

    // Update Owner in Sheet (column N)
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!${OWNER_OUTPUT_COL}${rowNum}`,
      valueInputOption: "RAW",
      requestBody: { values: [[owner]] }
    });

    // Update Deed Transfer Date in Sheet (column R)
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!R${rowNum}`,
      valueInputOption: "RAW",
      requestBody: { values: [[deedDate]] }
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