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

const INDEX_RANGE = "C2:C";   // <-- NEW (use column C for filenames)
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
// Load PARCEL IDs + Years + INDEX (column C)
// =========================
async function loadParcelData() {
  const indexRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!${INDEX_RANGE}`,
  });

  const parcelsRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!${PARCEL_RANGE}`,
  });

  const yearsRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!${YEAR_RANGE}`,
  });

  const indexValues = (indexRes.data.values || []).flat().map(v => (v || "").trim());
  const parcels = (parcelsRes.data.values || []).flat().map(v => (v || "").trim());
  const years = (yearsRes.data.values || []).flat().map(v => (v || "").trim());

  const items = [];
  for (let i = 0; i < parcels.length; i++) {
    if (parcels[i]) {
      items.push({
        indexValue: indexValues[i] || "",    // <--- NEW
        parcelId: parcels[i],
        auctionYear: years[i] || "",
        rowNum: i + 2
      });
    }
  }
  return items;
}

// =========================
// Owner + Deed Date + Instrument Extraction
// =========================
function extractOwnerDeedDateInstrumentFromHistoryPage(html, auctionYear) {
  const $ = cheerio.load(html);
  const auctionYearNum = auctionYear.match(/\d{4}/) ? auctionYear.match(/\d{4}/)[0] : auctionYear;
  const ownerTable = $('#pnlOwnHist table').first();
  if (!ownerTable.length) return { owner: '', deedDate: '', instrument: '' };

  const rows = ownerTable.find('tr').slice(1);

  let owner = '', deedDate = '', instrument = '';
  let rowFound = null;

  rows.each(function () {
    const yearCell = $(this).find('th').first();
    const ownerCell = $(this).find('td').eq(0);
    const legalDescCell = $(this).find('td').eq(1);
    if (yearCell.length && ownerCell.length && legalDescCell.length) {
      const yearText = yearCell.text().trim();
      if (yearText.includes(auctionYearNum)) {
        rowFound = { ownerCell, legalDescCell };
        return false;
      }
    }
  });

  if (!rowFound && rows.length > 0) {
    rowFound = {
      ownerCell: $(rows[0]).find('td').eq(0),
      legalDescCell: $(rows[0]).find('td').eq(1)
    };
  }

  if (rowFound) {
    const ownerHtml = rowFound.ownerCell.html() || '';
    owner = ownerHtml
      .split(/<br\s*\/?>/i)
      .map(line => he.decode(line).trim())
      .filter(line =>
        /^[A-Z\s\.\&]+$/.test(line) &&
        !/\d/.test(line) &&
        !/DALLAS|TEXAS|\d{5}/i.test(line)
      )
      .join(" & ");

    const legalHtml = rowFound.legalDescCell.html() || "";
    const deedMatch = legalHtml.match(/(\d{1,2}\/\d{1,2}\/\d{4})/);
    deedDate = deedMatch ? deedMatch[1] : "";

    const instrMatch = legalHtml.match(/([A-Z]{2,}\d{9,})/);
    instrument = instrMatch ? instrMatch[1] : "";
  }

  return { owner, deedDate, instrument };
}

// =========================
// Build PDF using indexValue
// =========================
function createPDF(indexValue, detailScreenshot, historyScreenshot) {
  const safe = String(indexValue).replace(/[^\w\-]+/g, "_");
  const pdfPath = path.join("artifacts", `parcel_${safe}.pdf`);

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
    headless: "new",
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
  for (const { parcelId, auctionYear, indexValue, rowNum } of parcelData) {

    const safe = String(indexValue).replace(/[^\w\-]+/g, "_");

    console.log("\n==============================");
    console.log(`📌 Parcel: ${parcelId} | Index: ${indexValue}`);

    // DETAIL PAGE
    const url1 = TARGET_URL_1 + parcelId;
    console.log("➡️ Navigating:", url1);
    await page.goto(url1, { waitUntil: "domcontentloaded" });
    await new Promise(r => setTimeout(r, 3000));

    const detailFile = `detail_${safe}.jpg`;
    await page.screenshot({ path: detailFile, fullPage: true });

    // HISTORY PAGE
    const url2 = TARGET_URL_2 + parcelId;
    console.log("➡️ Navigating:", url2);
    await page.goto(url2, { waitUntil: "domcontentloaded" });
    await new Promise(r => setTimeout(r, 3000));

    const historyHtml = await page.content();
    const historyFile = `history_${safe}.jpg`;
    await page.screenshot({ path: historyFile, fullPage: true });

    const { owner, deedDate, instrument } =
      extractOwnerDeedDateInstrumentFromHistoryPage(historyHtml, auctionYear);

    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!${OWNER_OUTPUT_COL}${rowNum}`,
      valueInputOption: "RAW",
      requestBody: { values: [[owner]] }
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!R${rowNum}`,
      valueInputOption: "RAW",
      requestBody: { values: [[deedDate]] }
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${SHEET_NAME}!S${rowNum}`,
      valueInputOption: "RAW",
      requestBody: { values: [[instrument]] }
    });

    console.log("📌 Sheet updated");

    const pdfPath = createPDF(indexValue, detailFile, historyFile);
    console.log("📄 PDF Generated:", pdfPath);

    fs.unlinkSync(detailFile);
    fs.unlinkSync(historyFile);
  }

  await browser.close();
  console.log("\n🏁 DONE — All parcels processed. PDFs ready for GitHub artifacts.");
})();