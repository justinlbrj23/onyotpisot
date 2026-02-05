/**
 * Milwaukee County Parcels Scraper
 * Clears sheet, then logs results directly into Google Sheets
 * Dynamically fetches available fields and uses them as sheet headers
 * Limits parsed data to 10k rows
 */

import fetch from "node-fetch";
import { google } from "googleapis";

// =========================
// CONFIG
// =========================
const SHEET_ID = "192sAixH2UDvOcb5PL9kSnzLRJUom-0ZiSuTH9cYAi1A";
const SHEET_NAME = "Sheet1";

// Use MPROP layer (id 1 or whichever has data)
const ENDPOINT =
  "https://milwaukeemaps.milwaukee.gov/arcgis/rest/services/property/parcels_mprop/MapServer/1/query";

const METADATA_URL =
  "https://milwaukeemaps.milwaukee.gov/arcgis/rest/services/property/parcels_mprop/MapServer/1?f=pjson";

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
async function getAvailableFields() {
  const res = await fetch(METADATA_URL);
  if (!res.ok) throw new Error(`Metadata HTTP ${res.status}`);
  const meta = await res.json();

  const fields = meta.fields.map(f => f.name);
  console.log("📑 Available fields:", fields);
  return fields;
}

async function fetchPage(offset, outFields) {
  const params = new URLSearchParams({
    where: "1=1", // no filter, pull everything
    outFields: outFields.join(","),
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

  let offset = 0;
  let hasMore = true;
  let parcels = [];

  console.log("🔎 Fetching parcels from ArcGIS...");

  while (hasMore && parcels.length < MAX_ROWS) {
    const data = await fetchPage(offset, fields);
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

  if (parcels.length > 0) {
    console.log("🔍 Sample record keys:", Object.keys(parcels[0]));
  }

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