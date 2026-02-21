// mappingScraper.cjs
// Stage 3: Map parsed auctions → Google Sheets + artifact output
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

const MIN_SURPLUS = 25000;

// =========================
// GOOGLE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});
const sheets = google.sheets({ version: "v4", auth });

// =========================
// HEADERS
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
// HELPERS
// =========================

function normalizeBaseUrl(u) {
  if (!u) return "";
  try {
    const url = new URL(u);
    url.searchParams.delete("page");
    const params = url.searchParams.toString();
    const cleanQuery = params ? `?${params}` : "";
    const cleanPath = url.pathname.replace(/\/+$/, "");
    return `${url.protocol}//${url.hostname}${cleanPath}${cleanQuery}`.toLowerCase();
  } catch {
    return String(u)
      .replace(/([?&])page=\d+/g, "")
      .replace(/\/+$/, "")
      .trim()
      .toLowerCase();
  }
}

function parseCurrency(str) {
  if (!str) return null;
  const n = parseFloat(String(str).replace(/[^0-9.-]/g, ""));
  return isNaN(n) ? null : n;
}

// =========================
// URL → County/State Mapping
// =========================
async function getUrlMapping() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME_URLS}!A2:C`,
  });

  const rows = res.data.values || [];
  const mapping = {};

  rows.forEach(([county, state, url]) => {
    if (!url) return;
    const key = normalizeBaseUrl(url);
    mapping[key] = { county: county || "", state: state || "" };
  });

  return mapping;
}

// =========================
// Map Row
// =========================
function mapRow(raw, urlMapping, anomalies) {

  if (
    !raw ||
    !raw.auctionStatus ||
    String(raw.auctionStatus).trim().toLowerCase() !== "sold"
  ) {
    return null;
  }

  const mapped = {};
  HEADERS.forEach(h => (mapped[h] = ""));

  const baseKey = normalizeBaseUrl(raw.sourceUrl || "");
  const geo = urlMapping[baseKey] || { county: "", state: "" };

  mapped["State"] = geo.state;
  mapped["County"] = geo.county;
  mapped["Property Address"] = raw.propertyAddress || "";
  mapped["Parcel / APN Number"] = raw.parcelId || "";
  mapped["Case Number"] = raw.caseNumber || "";
  mapped["Auction Date"] = raw.auctionDate || "";
  mapped["Sale Finalized (Yes/No)"] = "Yes";
  mapped["Sale Price"] = raw.salePrice || "";
  mapped["Opening / Minimum Bid"] = raw.openingBid || "";

  let surplus = null;

  if (raw.surplusAssessVsSale !== null && raw.surplusAssessVsSale !== undefined) {
    surplus = Number(raw.surplusAssessVsSale);
  } else if (raw.surplusSaleVsOpen !== null && raw.surplusSaleVsOpen !== undefined) {
    surplus = Number(raw.surplusSaleVsOpen);
  } else {
    const assess = parseCurrency(raw.assessedValue);
    const sale = parseCurrency(raw.salePrice);
    if (assess !== null && sale !== null) {
      surplus = assess - sale;
    }
  }

  if (surplus === null) {
    anomalies.push({
      type: "MissingSurplus",
      caseNumber: raw.caseNumber,
      parcelId: raw.parcelId,
      sourceUrl: raw.sourceUrl,
    });
  }

  mapped["Estimated Surplus"] = surplus !== null ? String(surplus) : "";
  mapped["Final Estimated Surplus to Owner"] = surplus !== null ? String(surplus) : "";

  const meets = surplus !== null ? surplus >= MIN_SURPLUS : false;

  mapped["Meets Minimum Surplus? (Yes/No)"] = meets ? "Yes" : "No";
  mapped["Deal Viable? (Yes/No)"] = meets ? "Yes" : "No";

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
// Append to Sheets
// =========================
async function appendRows(rows) {
  if (!rows.length) {
    console.log("⚠️ No rows to append.");
    return;
  }

  const values = rows.map(row => HEADERS.map(h => row[h] || ""));

  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME_RAW}!A1`,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values },
  });

  console.log(`✅ Appended ${values.length} rows.`);
}

// =========================
// MAIN
// =========================
(async () => {

  if (!fs.existsSync(INPUT_FILE)) {
    console.error("❌ Input file missing.");
    process.exit(1);
  }

  let rawData = JSON.parse(fs.readFileSync(INPUT_FILE, "utf8"));

  if (!Array.isArray(rawData)) {
    console.error("❌ parsed-auctions.json is not an array.");
    process.exit(1);
  }

  console.log(`📦 Loaded ${rawData.length} rows.`);

  const urlMapping = await getUrlMapping();
  const anomalies = [];

  const uniqueMap = new Map();

  rawData.forEach(raw => {
    const key = `${normalizeBaseUrl(raw.sourceUrl)}|${raw.caseNumber}|${raw.parcelId}`;
    if (!uniqueMap.has(key)) {
      const mapped = mapRow(raw, urlMapping, anomalies);
      if (mapped) uniqueMap.set(key, mapped);
    }
  });

  const mappedRows = [...uniqueMap.values()];

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(mappedRows, null, 2));

  if (anomalies.length) {
    fs.writeFileSync(ANOMALY_FILE, JSON.stringify(anomalies, null, 2));
  }

  await appendRows(mappedRows);

  console.log("🏁 Mapping complete.");
})();