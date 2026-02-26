// dac_scraper.cjs
// Dallas CAD Parcel Scraper (Artifact Mode, PDFKit)
// Requires: npm install puppeteer cheerio googleapis pdfkit

const puppeteer = require("puppeteer");
const fs = require("fs");
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

function extractOwnerNameFromHistoryPage(pageText, auctionYear) {
  // Normalize auction year (e.g., "02/03/2026" → "2026")
  const auctionYearNum = auctionYear.match(/\d{4}/) ? auctionYear.match(/\d{4}/)[0] : auctionYear;

  // Split into blocks by year (each block starts with a year)
  const blocks = pageText.split(/(?=\d{4}\n)/);

  for (const block of blocks) {
    const lines = block.split('\n').map(l => l.trim()).filter(Boolean);

    // Find Deed Transfer Date line and extract year
    const deedLine = lines.find(line => line.includes('Deed Transfer Date:'));
    if (deedLine) {
      const deedYearMatch = deedLine.match(/\d{4}/);
      if (deedYearMatch && deedYearMatch[0] === auctionYearNum) {
        // Owner is the line immediately after the year line (lines[0] = year, lines[1] = owner)
        if (lines.length > 1) {
          return lines[1];
        }
      }
    }
  }

  // Fallback: If no matching year found, extract owner from first plausible row
  return extractOwnerNameFallback(pageText);
}

// Improved fallback extraction function
function extractOwnerNameFallback(pageText) {
  const lines = pageText.split('\n').map(l => l.trim()).filter(Boolean);

  // List of phrases to skip (add more as needed)
  const skipPhrases = [
    "NAVIGATION LINKS",
    "City School County College Hospital Special District",
    "Owner Legal Desc Market Value Taxable Value Exemptions",
    "Owner / Legal Description",
    "Year",
    "Legal Description",
    "Deed Transfer Date",
    "Market Value",
    "Taxable Value",
    "Exemptions",
    "Annual Report",
    "About DCAD",
    "Search Appraisals",
    "Find Property on Map",
    "Online BPP Rendition",
    "Online TaxRep Website",
    "Forms",
    "Data Products",
    "Open Records",
    "GIS Data Products",
    "Exemptions",
    "Property Valuation Process",
    "Protest Process",
    "uFILE Online Protest System",
    "Informal Review Process",
    "ARB",
    "Taxpayer Liaison Officer",
    "Paying Taxes",
    "Local Tax Offices",
    "Taxing Unit Rates",
    "Entity Truth-in-Taxation",
    "Notice Of Estimated Taxes",
    "Elections DCAD BOD",
    "Low Income Housing Cap Rate",
    "F.A.Q.",
    "Calendar",
    "Certified Value Summaries",
    "Certified Comparisons",
    "Certification Reports",
    "Preliminary Comparisons",
    "Average SFR Values",
    "Median SFR Values",
    "Reappraisal Plan",
    "Mass Appraisal Report",
    "Water & Electricity Usage",
    "Administration",
    "Human Resources",
    "Links",
    "Contact Us"
  ];

  // Heuristic: likely owner lines contain a name and address (e.g., "CASTILLO GUADALUPE 422 AVENUE E DALLAS, TEXAS")
  const ownerRegex = /[A-Z]{2,}.*\d{1,}.*DALLAS/i;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (
      !skipPhrases.some(phrase => line.includes(phrase)) &&
      ownerRegex.test(line)
    ) {
      return line;
    }
  }

  // If nothing found, fallback to second line (row 2)
  return lines[1] || '';
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

    // Extract Owner (from plain text)
    const historyText = await page.evaluate(() => document.body.innerText);
    const ownerName = extractOwnerNameFromHistoryPage(historyText, auctionYear);
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