/**
 * Milwaukee County Parcels Property Information Scraper
 * Clears sheet, then logs ALL available fields directly into Google Sheets
 * Dynamically fetches available fields and uses them as sheet headers
 * Limits parsed data to 5k rows
 * Retries failed fetches up to 3 times with backoff
 * ✅ Writes in batches to avoid exceeding Google Sheets cell limits
 * 🚫 Excludes business owners (LLC, INC, etc.)
 * 🚫 Excludes parcels not marked RESIDENTIAL under DESCRIPTION
 */

import fetch from "node-fetch";
import { google } from "googleapis";

const SHEET_ID = "1QWUiJ2ddikZdwx9NdEfResI1FZPGkpVY-nQ4G6I95Ug";
const SHEET_NAME = "Sheet1";

const SERVICE_ROOT =
  "https://lio.milwaukeecountywi.gov/arcgis/rest/services/Sheriff/SheriffSales/FeatureServer";

const LAYER_ID = 0;

const ENDPOINT = `${SERVICE_ROOT}/${LAYER_ID}/query`;
const METADATA_URL = `${SERVICE_ROOT}/${LAYER_ID}?f=pjson`;

const TEST_SIZE = 10;
const PAGE_SIZE = 500;
const MAX_ROWS = 5000;
const MAX_RETRIES = 3;
const BATCH_SIZE = 500;

const ARCGIS_TOKEN = process.env.ARCGIS_TOKEN || "";

const auth = new google.auth.GoogleAuth({
  keyFile: "./service-account.json",
  scopes: ["https://www.googleapis.com/auth/spreadsheets"]
});
const sheets = google.sheets({ version: "v4", auth });

// Retry wrapper
async function fetchWithRetry(url, options = {}, retries = MAX_RETRIES) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, options);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (err) {
      console.warn(`⚠️ Fetch attempt ${attempt} failed: ${err.message}`);
      if (attempt === retries) throw err;
      await new Promise(r => setTimeout(r, 1000 * attempt));
    }
  }
}

// Get fields
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

// Fetch page
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

// Sheet helpers
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

async function appendRowsBatch(rows) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A2`,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values: rows }
  });
}

// Filter helpers
function isBusinessOwner(name) {
  if (!name) return false;
  const upper = name.toUpperCase();
  return (
    upper.includes("LLC") ||
    upper.includes("INC") ||
    upper.includes("CORP") ||
    upper.includes("COMPANY") ||
    upper.includes("CO.") ||
    upper.includes("LTD")
  );
}

function isResidential(description) {
  if (!description) return false;
  return description.toUpperCase().startsWith("RESIDENTIAL");
}

// Main
async function run() {
  const fields = await getAvailableFields();

  console.log("🔎 Testing ArcGIS with 10 records...");
  const testData = await fetchPage(0, fields, TEST_SIZE);
  if (!testData.features?.length) {
    console.log("⚠️ Test query returned no features.");
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

  // 🚫 Apply filters
  const beforeFilterCount = parcels.length;
  parcels = parcels.filter(
    p => !isBusinessOwner(p.OWNERNAME1) && isResidential(p.DESCRIPTION)
  );

  console.log(`📦 Parcels before filter: ${beforeFilterCount}`);
  console.log(`📦 Parcels after filter: ${parcels.length}`);
  console.log(`📊 Total cells to write: ${parcels.length * fields.length}`);

  await clearSheet();
  await writeHeaders(fields);

  const rows = parcels.map(p => fields.map(field => p[field] ?? ""));

  if (!rows.length) {
    console.log("✅ No rows to write");
    return;
  }

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    await appendRowsBatch(batch);
    console.log(`✅ Appended batch ${i / BATCH_SIZE + 1} (${batch.length} rows)`);
  }

  console.log(`✅ Wrote ${rows.length} rows to Google Sheets in batches`);
}

run().catch(err => {
  console.error("❌ Error:", err);
  process.exit(1);
});