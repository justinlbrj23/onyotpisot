// mappingScraper.cjs (Stage 3: map parsed auctions → Google Sheets + artifact output)
// Requires: npm install googleapis

const fs = require("fs");
const { google } = require("googleapis");

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = "./service-account.json";
const SPREADSHEET_ID = "1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA";
const SHEET_NAME_URLS = "web_tda";
const SHEET_NAME_RAW = "raw_main";
const INPUT_FILE = process.argv[2] || "parsed-auctions.json";
const OUTPUT_FILE = "mapped-output.json";
const ANOMALY_FILE = "mapping-anomalies.json";

// =========================
// GOOGLE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

// =========================
// HEADERS (TSSF-compliant)
// =========================
const HEADERS = [
  "State","County","Property Address","City","ZIP Code","Parcel / APN Number","Case Number",
  "Auction Date","Sale Finalized (Yes/No)","Sale Price","Opening / Minimum Bid","Estimated Surplus","Meets Minimum Surplus? (Yes/No)",
  "Last Owner Name (as on Deed)","Additional Owner(s)","Ownership Type","Deed Type","Owner Deed Recording Date","Owner Deed Instrument #",
  "Mortgage Lender Name","Mortgage Amount","Mortgage Recording Date","Mortgage Satisfied? (Yes/No)","Mortgage Release Recording #","Mortgage Still Owed Amount",
  "Lien / Judgment Type","Creditor Name","Lien Amount","Lien Recording Date","Lien Expired? (Yes/No)","Lien Satisfied? (Yes/No)",
  "Total Open Debt","Final Estimated Surplus to Owner","Deal Viable? (Yes/No)",
  "Ownership Deed Collected? (Yes/No)","Foreclosure Deed Collected? (Yes/No)","Proof of Sale Collected? (Yes/No)","Debt Search Screenshot Collected? (Yes/No)","Tax Assessor Page Collected? (Yes/No)","File Complete? (Yes/No)",
  "File Submitted? (Yes/No)","Submission Date","Accepted / Rejected","Kickback Reason","Researcher Name",
];

// =========================
// County/State mapping
// =========================
async function getUrlMapping() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME_URLS}!A2:C`,
  });

  const rows = res.data.values || [];
  const mapping = {};

  rows.forEach(([county, state, url]) => {
    if (url) mapping[url.trim()] = { county: county || "", state: state || "" };
  });

  return mapping;
}

// =========================
// Normalize Yes/No
// =========================
function yn(val) {
  if (val === true || val === "Yes") return "Yes";
  if (val === false || val === "No") return "No";
  if (typeof val === "string") {
    const v = val.trim().toLowerCase();
    if (v === "yes") return "Yes";
    if (v === "no") return "No";
  }
  return "";
}

// =========================
// Currency parser
// =========================
function parseCurrency(str) {
  if (!str) return null;
  const n = parseFloat(str.replace(/[^0-9.-]/g, ""));
  return isNaN(n) ? null : n;
}

// =========================
// Map parsed auction row → TSSF headers
// =========================
function mapRow(raw, urlMapping, anomalies) {
  if (raw.auctionStatus !== "Sold") return null;

  const mapped = {};
  HEADERS.forEach(h => (mapped[h] = ""));

  const baseUrl = (raw.sourceUrl || "").split("&page=")[0];
  const geo = urlMapping[baseUrl] || { county: "", state: "" };

  mapped["State"] = geo.state;
  mapped["County"] = geo.county;
  mapped["Property Address"] = raw.propertyAddress || "";
  mapped["Parcel / APN Number"] = raw.parcelId || "";
  mapped["Case Number"] = raw.caseNumber || "";
  mapped["Auction Date"] = raw.auctionDate || raw.date || "";
  mapped["Sale Finalized (Yes/No)"] = "Yes";

  const salePrice = raw.salePrice || raw.amount || "";
  const openingBid = raw.openingBid || "";

  mapped["Sale Price"] = salePrice;
  mapped["Opening / Minimum Bid"] = openingBid;

  const sale = parseCurrency(salePrice);
  const open = parseCurrency(openingBid);
  const estimatedSurplus = sale !== null && open !== null ? sale - open : null;

  if (sale === null) {
    anomalies.push({
      type: "MissingSalePrice",
      message: "Sold auction missing sale price",
      parcelId: raw.parcelId,
      caseNumber: raw.caseNumber,
      sourceUrl: raw.sourceUrl
    });
  }

  if (estimatedSurplus === null) {
    anomalies.push({
      type: "MissingSurplus",
      message: "Could not compute surplus",
      parcelId: raw.parcelId,
      caseNumber: raw.caseNumber,
      sourceUrl: raw.sourceUrl
    });
  }

  mapped["Estimated Surplus"] = estimatedSurplus !== null ? String(estimatedSurplus) : "";
  mapped["Final Estimated Surplus to Owner"] = estimatedSurplus !== null ? String(estimatedSurplus) : "";

  const meetsMinimum = estimatedSurplus !== null && estimatedSurplus >= 25000;
  mapped["Meets Minimum Surplus? (Yes/No)"] = yn(meetsMinimum ? "Yes" : "No");
  mapped["Deal Viable? (Yes/No)"] = meetsMinimum ? "Yes" : "No";

  mapped["Ownership Deed Collected? (Yes/No)"] = "No";
  mapped["Foreclosure Deed Collected? (Yes/No)"] = "No";
  mapped["Proof of Sale Collected? (Yes/No)"] = "No";
  mapped["Debt Search Screenshot Collected? (Yes/No)"] = "No";
  mapped["Tax Assessor Page Collected? (Yes/No)"] = "No";
  mapped["File Complete? (Yes/No)"] = "No";
  mapped["File Submitted? (Yes/No)"] = "No";

  return mapped;
}

// =========================
// Append rows to sheet
// =========================
async function appendRows(rows) {
  if (!rows.length) {
    console.log("⚠️ No mapped rows to append.");
    return;
  }

  const values = rows.map(row => HEADERS.map(h => row[h] || ""));
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: SHEET_NAME_RAW,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values },
  });
  console.log(`✅ Appended ${values.length} mapped rows.`);
}

// =========================
// MAIN
// =========================
(async () => {
  if (!fs.existsSync(INPUT_FILE)) {
    console.error(`❌ Input file not found: ${INPUT_FILE}`);
    process.exit(1);
  }

  const rawData = JSON.parse(fs.readFileSync(INPUT_FILE, "utf8"));
  console.log(`📦 Loaded ${rawData.length} parsed rows from ${INPUT_FILE}`);

  const urlMapping = await getUrlMapping();
  console.log(`🌐 Fetched ${Object.keys(urlMapping).length} URL → County/State mappings`);

  const anomalies = [];
  const uniqueMap = new Map();

  rawData.forEach(raw => {
    const key = `${(raw.sourceUrl || "").split("&page=")[0]}|${raw.caseNumber}|${raw.parcelId}`;
    if (!uniqueMap.has(key)) {
      const mapped = mapRow(raw, urlMapping, anomalies);
      if (mapped) uniqueMap.set(key, mapped);
    }
  });

  const mappedRows = [...uniqueMap.values()];

  if (mappedRows.length) {
    console.log("🧪 Sample mapped row preview:", mappedRows[0]);
  }

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(mappedRows, null, 2));
  console.log(`💾 Saved mapped rows → ${OUTPUT_FILE}`);

  if (anomalies.length) {
    fs.writeFileSync(ANOMALY_FILE, JSON.stringify(anomalies, null, 2));
    console.log(`⚠️ Saved ${anomalies.length} anomalies → ${ANOMALY_FILE}`);
  }

  await appendRows(mappedRows);
  console.log("🏁 Done.");
})();