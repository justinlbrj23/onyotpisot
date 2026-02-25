// dac_scraper.cjs
// Dallas CAD Parcel Scraper with PDF output + GitHub Artifacts
// Requires:
// npm install puppeteer cheerio googleapis pdfkit

const puppeteer = require("puppeteer");
const fs = require("fs");
const cheerio = require("cheerio");
const PDFDocument = require("pdfkit");
const { google } = require("googleapis");
const path = require("path");

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
// GOOGLE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});

const sheets = google.sheets({ version: "v4", auth });

// =========================
// Load Parcel IDs & Auction Years
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

  const parcels = (parcelsRes.data.values || []).flat().map((v) => (v || "").trim());
  const years = (yearsRes.data.values || []).flat().map((v) => (v || "").trim());

  const parcelData = [];
  for (let i = 0; i < parcels.length; i++) {
    if (parcels[i]) {
      parcelData.push({
        parcelId: parcels[i],
        auctionYear: years[i] || "",
        rowNum: i + 2,
      });
    }
  }
  return parcelData;
}

// =========================
// Extract Owner Name
// =========================
function extractOwnerNameForYear(html, auctionYear) {
  const $ = cheerio.load(html);
  let ownerName = "";

  let yearPattern = auctionYear;
  const yearMatch = auctionYear.match(/\d{4}/);
  if (yearMatch) yearPattern = yearMatch[0];

  const bodyText = $("body").text();

  const regex = new RegExp(`${yearPattern}\\s*([A-Z\\s]+)\\s*\\d{1,4}`, "m");
  const match = bodyText.match(regex);
  if (match) {
    ownerName = match[1].trim();
  } else {
    const yearIdx = bodyText.indexOf(yearPattern);
    if (yearIdx !== -1) {
      const afterYear = bodyText
        .slice(yearIdx + yearPattern.length)
        .split("\n")
        .map((l) => l.trim());
      for (const line of afterYear) {
        if (/^[A-Z\s]+$/.test(line) && line.length > 2) {
          ownerName = line;
          break;
        }
      }
    }
  }
  return ownerName;
}

// =========================
// Create PDF using PDFKIT
// =========================
function createParcelPDF(parcelId, screenshot1, screenshot2) {
  const pdfPath = path.join("artifacts", `parcel_${parcelId}.pdf`);
  const doc = new PDFDocument({ autoFirstPage: false });

  const stream = fs.createWriteStream(pdfPath);
  doc.pipe(stream);

  // Add DETAIL screenshot
  const img1 = doc.openImage(screenshot1);
  doc.addPage({ size: [img1.width, img1.height] });
  doc.image(img1, 0, 0);

  // Add HISTORY screenshot
  const img2 = doc.openImage(screenshot2);
  doc.addPage({ size: [img2.width, img2.height] });
  doc.image(img2, 0, 0);

  doc.end();
  return pdfPath;
}

// =========================
// MAIN
// =========================
(async () => {
  console.log("📥 Loading PARCEL IDs and Auction Years...");
  const parcelData = await loadParcelData();
  console.log(`🧾 Loaded ${parcelData.length} Parcel IDs`);

  const browser = await puppeteer.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-blink-features=AutomationControlled",
      "--ignore-certificate-errors",
      "--disable-gpu",
      "--single-process",
      "--no-zygote",
    ],
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
    "Accept-Language": "en-US,en;q=0.9",
  });

  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => false });
  });

  // MAIN LOOP
  for (const { parcelId, auctionYear, rowNum } of parcelData) {
    console.log("\n==============================");
    console.log(`📌 Processing Parcel: ${parcelId}`);

    // URL 1 - DETAIL
    const url1 = TARGET_URL_1 + parcelId;
    console.log(`🌐 Navigating to: ${url1}`);
    await page.goto(url1, { waitUntil: "domcontentloaded" });
    await new Promise((r) => setTimeout(r, 3000));
    const html1 = await page.content();
    const detailFile = `detail_${parcelId}.jpg`;
    await page.screenshot({ path: detailFile, fullPage: true });

    // URL 2 - HISTORY
    const url2 = TARGET_URL_2 + parcelId;
    console.log(`🌐 Navigating to: ${url2}`);
    await page.goto(url2, { waitUntil: "domcontentloaded" });
    await new Promise((r) => setTimeout(r, 3000));
    const html2 = await page.content();
    const historyFile = `history_${parcelId}.jpg`;
    await page.screenshot({ path: historyFile, fullPage: true });

    // Extract owner
    const ownerName = extractOwnerNameForYear(html2, auctionYear);
    console.log(`👤 Owner for ${auctionYear}: ${ownerName}`);

    // PDF Output
    const pdfPath = createParcelPDF(parcelId, detailFile, historyFile);
    console.log(`📄 Created PDF: ${pdfPath}`);

    // Clean temp images
    fs.unlinkSync(detailFile);
    fs.unlinkSync(historyFile);

    // Update Google Sheet
    const writeRange = `${SHEET_NAME}!${OWNER_OUTPUT_COL}${rowNum}`;
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: writeRange,
      valueInputOption: "RAW",
      requestBody: { values: [[ownerName]] },
    });

    console.log(`📝 Updated sheet cell ${writeRange}`);
  }

  await browser.close();
  console.log("\n🏁 DONE — All parcels processed successfully!");
})();