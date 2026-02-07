/**
 * Milwaukee County Parcels Property Information Scraper
 * Clears sheet, then logs results directly into Google Sheets
 * Dynamically fetches available fields and uses them as sheet headers
 * Limits parsed data to 10k rows
 * Retries failed fetches up to 3 times with backoff
 * Supports ArcGIS token authentication
 */

import fetch from "node-fetch";
import { google } from "googleapis";

const SHEET_ID = "192sAixH2UDvOcb5PL9kSnzLRJUom-0ZiSuTH9cYAi1A";
const SHEET_NAME = "Sheet1";

// ✅ Correct service root
const SERVICE_ROOT =
  "https://services2.arcgis.com/s1wgJQKbKJihhhaT/arcgis/rest/services/Milwaukee_County_Parcels_Property_Information_view/FeatureServer";

// 🔄 Change this if the layer isn’t 0
const LAYER_ID = 0;

const ENDPOINT = `${SERVICE_ROOT}/${LAYER_ID}/query`;
const METADATA_URL = `${SERVICE_ROOT}/${LAYER_ID}?f=pjson`;

const TEST_SIZE = 10;
const PAGE_SIZE = 500;
const MAX_ROWS = 10000;
const MAX_RETRIES = 3;

// ArcGIS token (if dataset is private)
const ARCGIS_TOKEN = process.env.ARCGIS_TOKEN || "";

const auth = new google.auth.GoogleAuth({
  keyFile: "./service-account.json",
  scopes: ["https://www.googleapis.com/auth/spreadsheets"]
});
const sheets = google.sheets({ version: "v4", auth });

// =========================
// Retry wrapper for fetch
// =========================
async function fetchWithRetry(url, options = {}, retries = MAX_RETRIES) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, options);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (err) {
      console.warn(`⚠️ Fetch attempt ${attempt} failed: ${err.message}`);
      if (attempt === retries) throw err;
      await new Promise(r => setTimeout(r, 1000 * attempt)); // backoff
    }
  }
}

// =========================
// ARC GIS FETCH FUNCTIONS
// =========================
async function getAvailableFields() {
  const url = ARCGIS_TOKEN ? `${METADATA_URL}&token=${ARCGIS_TOKEN}` : METADATA_URL;
  const meta = await fetchWithRetry(url);
  if (!meta.fields) {
    console.error("⚠️ No fields array found in metadata. Raw metadata:", meta);
    return [];
  }
  const fields = meta.fields.map(f => f.name);
  console.log("📑 Available fields:", fields);
  return fields;
}

async function fetchPage(offset, outFields, size) {
  const params = new URLSearchParams({
    where: "1=1",
    outFields: outFields.length ? outFields.join(",") : "*",
    returnGeometry: "false",
    f: "json",
    resultOffset: offset,
    resultRecordCount: size
  });
  if (ARCGIS_TOKEN) params.append("token", ARCGIS_TOKEN);
  return await fetchWithRetry(`${ENDPOINT}?${params}`);
}

// =========================
// SHEET HELPERS
// =========================
async function clearSheet() {
  await sheets.spreadsheets.values.clear({
    spreadsheetId: SHEET_ID,
    range: SHEET_NAME
  });
}

async function writeHeaders(headers) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A1`,
    valueInputOption: "RAW",
    requestBody: { values: [headers] }
  });
}

async function overwriteRows(rows) {
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
    parcels.push(...data.features.map(f => f.attributes));
    offset += PAGE_SIZE;
    hasMore = data.exceededTransferLimit === true;
  }

  if (parcels.length > MAX_ROWS) {
    parcels = parcels.slice(0, MAX_ROWS);
  }

  console.log(`📦 Total parcels fetched: ${parcels.length}`);

  await clearSheet();
  await writeHeaders(fields.length ? fields : Object.keys(testData.features[0].attributes));

  const rows = parcels.map(p =>
    (fields.length ? fields : Object.keys(p)).map(field => p[field] ?? "")
  );

  if (!rows.length) {
    console.log("✅ No rows to write");
    return;
  }

  await overwriteRows(rows);
  console.log(`✅ Wrote ${rows.length} rows to Google Sheets (overwrite mode)`);
}

run().catch(err => {
  console.error("❌ Error:", err);
  process.exit(1);
});