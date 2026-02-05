/**
 * Milwaukee County Parcels Scraper
 * Clears sheet, then logs results directly into Google Sheets
 * Overwrites instead of appending to avoid 10M cell limit
 * Limits parsed data to 10k rows
 */

import fetch from "node-fetch";
import { google } from "googleapis";

// =========================
// CONFIG
// =========================
const SHEET_ID = "192sAixH2UDvOcb5PL9kSnzLRJUom-0ZiSuTH9cYAi1A";
const SHEET_NAME = "Sheet1";

const HEADERS = [
  "TAXKEY",
  "OWNER",
  "ADDRESS",
  "CITY",
  "DELQ",
  "NET_TAX",
  "LAST_SYNC"
];

// Use MPROP layer (id 1 or whichever has data)
const ENDPOINT =
  "https://milwaukeemaps.milwaukee.gov/arcgis/rest/services/property/parcels_mprop/MapServer/1/query";

const PAGE_SIZE = 2000;
const MAX_ROWS = 10000; // cap at 10k rows

// =========================
// GOOGLE SHEETS AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: "./service-account.json",
  scopes: ["https://www.googleapis.com/auth/spreadsheets"]
});

const sheets = google.sheets({ version: "v4", auth });

// =========================
// ARC GIS FETCH FUNCTIONS
// =========================
async function fetchPage(offset) {
  const params = new URLSearchParams({
    where: "1=1", // no filter, pull everything
    outFields: "*", // request all available fields
    returnGeometry: "false",
    f: "json",
    resultOffset: offset,
    resultRecordCount: PAGE_SIZE
  });

  const res = await fetch(`${ENDPOINT}?${params}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

// =========================
// SHEET HELPERS
// =========================
async function clearSheet() {
  console.log("🧹 Clearing entire sheet contents...");
  await sheets.spreadsheets.values.clear({
    spreadsheetId: SHEET_ID,
    range: SHEET_NAME
  });
}

async function writeHeaders() {
  console.log("🧾 Writing sheet headers...");
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A1`,
    valueInputOption: "RAW",
    requestBody: { values: [HEADERS] }
  });
}

async function overwriteRows(rows) {
  console.log("✍️ Writing rows (overwrite mode)...");
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A2`,
    valueInputOption: "RAW",
    requestBody: { values: rows }
  });
}

// =========================
// MAIN
// =========================
async function run() {
  let offset = 0;
  let hasMore = true;
  let parcels = [];

  console.log("🔎 Fetching parcels from ArcGIS...");

  while (hasMore && parcels.length < MAX_ROWS) {
    const data = await fetchPage(offset);
    if (!data.features?.length) {
      console.log("⚠️ No features returned at offset", offset);
      break;
    }

    console.log(`➡️ Page fetched: ${data.features.length} records`);
    parcels.push(...data.features.map(f => f.attributes));
    offset += PAGE_SIZE;
    hasMore = data.exceededTransferLimit === true;
  }

  // Cap at MAX_ROWS
  if (parcels.length > MAX_ROWS) {
    parcels = parcels.slice(0, MAX_ROWS);
  }

  console.log(`📦 Total parcels fetched: ${parcels.length}`);

  await clearSheet();
  await writeHeaders();

  if (parcels.length > 0) {
    console.log("🔍 Sample record keys:", Object.keys(parcels[0]));
  }

  const rows = parcels.map(p => [
    p.TAXKEY?.toString() || "",
    p.OWNER_NAME || p.OWNER_NAME2 || p.OWNER_NAME_1 || "",
    p.ADDRESS || p.PROP_ADDR || `${p.PROP_HOUSE_NR || ""} ${p.PROP_STREET || ""}`.trim(),
    p.CITY || p.MUNI || p.OWNER_CITY_STATE || "",
    p.TAX_DELQ || p.TAXDELQ_AMT || "",
    p.NET_TAX || "",
    new Date().toISOString()
  ]);

  if (!rows.length) {
    console.log("✅ No rows to write");
    return;
  }

  await overwriteRows(rows);
  console.log(`✅ Wrote ${rows.length} rows to Google Sheets (overwrite mode)`);
}

// =========================
// RUN
// =========================
run().catch(err => {
  console.error("❌ Error:", err);
  process.exit(1);
});