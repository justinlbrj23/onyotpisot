/**
 * Milwaukee County Parcels Scraper
 * Clears sheet, then logs results directly into Google Sheets
 * Dynamically fetches available fields and uses them as sheet headers
 * First tests with 10 records, then fetches up to 10k rows
 */

import fetch from "node-fetch";
import { google } from "googleapis";

// =========================
// CONFIG
// =========================
const SHEET_ID = "192sAixH2UDvOcb5PL9kSnzLRJUom-0ZiSuTH9cYAi1A";
const SHEET_NAME = "Sheet1";

// Use MPROP_full layer (id 2 is usually populated)
const ENDPOINT =
  "https://milwaukeemaps.milwaukee.gov/arcgis/rest/services/property/parcels_mprop/MapServer/2/query";

const METADATA_URL =
  "https://milwaukeemaps.milwaukee.gov/arcgis/rest/services/property/parcels_mprop/MapServer/2?f=pjson";

const TEST_SIZE = 10;
const PAGE_SIZE = 500; // smaller page size to reduce timeout risk
const MAX_ROWS = 5000; // cap at 10k rows

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
async function getAvailableFields() {
  const res = await fetch(METADATA_URL);
  if (!res.ok) throw new Error(`Metadata HTTP ${res.status}`);
  const meta = await res.json();

  const fields = meta.fields.map(f => f.name);
  console.log("📑 Available fields:", fields);
  return fields;
}

async function fetchPage(offset, outFields, size) {
  const params = new URLSearchParams({
    where: "1=1",
    outFields: outFields.join(","),
    returnGeometry: "false",
    f: "json",
    resultOffset: offset,
    resultRecordCount: size
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

async function writeHeaders(headers) {
  console.log("🧾 Writing sheet headers...");
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A1`,
    valueInputOption: "RAW",
    requestBody: { values: [headers] }
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
  const fields = await getAvailableFields();

  console.log("🔎 Testing ArcGIS with 10 records...");
  const testData = await fetchPage(0, fields, TEST_SIZE);
  if (!testData.features?.length) {
    console.log("⚠️ Test query returned no features. Layer may be empty or restricted.");
    return;
  }

  console.log("🔍 Sample record keys:", Object.keys(testData.features[0].attributes));
  console.log("🔍 Sample record values:", testData.features[0].attributes);

  // Proceed to full fetch
  let offset = 0;
  let hasMore = true;
  let parcels = [];

  console.log("🔎 Fetching parcels from ArcGIS (bulk)...");

  while (hasMore && parcels.length < MAX_ROWS) {
    const data = await fetchPage(offset, fields, PAGE_SIZE);
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
  await writeHeaders(fields);

  const rows = parcels.map(p =>
    fields.map(field => p[field] ?? "")
  );

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