/**
 * Universal ArcGIS Item Scraper
 * --------------------------------------
 * ✔ Accepts ANY ArcGIS item data URL
 * ✔ Auto‑discovers FeatureServer + Layer ID OR handles FeatureCollection
 * ✔ Dynamically fetches available fields
 * ✔ Writes all attributes to Google Sheets
 * ✔ Batches writes to avoid cell limits
 * ✔ Retries failed fetches with backoff
 */

import fetch from "node-fetch";
import { google } from "googleapis";

// --------------------------------------
// CONFIG
// --------------------------------------
const SHEET_ID = "1woBGXySPNxQavq_3FMsNK4WnqhJ4w_Oh5Ui_05rzzL0";
const SHEET_NAME = "Sheet1";

const ITEM_DATA_URL =
  "https://www.arcgis.com/sharing/rest/content/items/240d1eab8fa44e5e8baf244fb1a15365/data";

let SERVICE_ROOT = "";
let LAYER_ID = 0;
let ENDPOINT = "";
let METADATA_URL = "";

const TEST_SIZE = 10;
const PAGE_SIZE = 500;
const MAX_ROWS = 5000;
const MAX_RETRIES = 3;
const BATCH_SIZE = 500;

const ARCGIS_TOKEN = process.env.ARCGIS_TOKEN || "";

// --------------------------------------
// GOOGLE AUTH
// --------------------------------------
const auth = new google.auth.GoogleAuth({
  keyFile: "./service-account.json",
  scopes: ["https://www.googleapis.com/auth/spreadsheets"]
});
const sheets = google.sheets({ version: "v4", auth });

// --------------------------------------
// RETRY WRAPPER
// --------------------------------------
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

// --------------------------------------
// DISCOVER FEATURESERVER OR FEATURECOLLECTION
// --------------------------------------
async function resolveArcGISItem() {
  const data = await fetchWithRetry(`${ITEM_DATA_URL}?f=json`);

  // Case 1: Feature Collection (embedded data, no external URL)
  if (data.featureCollection?.layers?.length) {
    console.log("📦 Item is a Feature Collection (embedded data).");
    return { type: "featureCollection", layers: data.featureCollection.layers };
  }

  // Case 2: WebMap referencing external services
  if (data.operationalLayers?.length && data.operationalLayers[0].url) {
    SERVICE_ROOT = data.operationalLayers[0].url.replace(/\/\d+$/, "");
    LAYER_ID = parseInt(data.operationalLayers[0].url.split("/").pop(), 10);
    ENDPOINT = `${SERVICE_ROOT}/${LAYER_ID}/query`;
    METADATA_URL = `${SERVICE_ROOT}/${LAYER_ID}?f=pjson`;
    console.log("🔗 Resolved FeatureServer:", SERVICE_ROOT);
    console.log("🔢 Layer ID:", LAYER_ID);
    return { type: "featureServer" };
  }

  throw new Error("❌ Could not locate FeatureServer or FeatureCollection in item data.");
}

// --------------------------------------
// GET AVAILABLE FIELDS (FeatureServer only)
// --------------------------------------
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

// --------------------------------------
// FETCH PAGE (FeatureServer only)
// --------------------------------------
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

// --------------------------------------
// SHEET HELPERS
// --------------------------------------
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

// --------------------------------------
// MAIN
// --------------------------------------
async function run() {
  console.log("🔎 Resolving ArcGIS item...");
  const info = await resolveArcGISItem();

  let fields = [];
  let rowsRaw = [];

  if (info.type === "featureServer") {
    fields = await getAvailableFields();

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

    console.log("🔎 Fetching features from ArcGIS (bulk)...");

    while (hasMore && rowsRaw.length < MAX_ROWS) {
      const data = await fetchPage(offset, fields, PAGE_SIZE);

      if (!data.features?.length) {
        console.log("⚠️ No features returned at offset", offset);
        break;
      }

      rowsRaw.push(...data.features.map(f => f.attributes));

      offset += PAGE_SIZE;
      hasMore = data.exceededTransferLimit === true;
    }

    if (rowsRaw.length > MAX_ROWS) {
      rowsRaw = rowsRaw.slice(0, MAX_ROWS);
    }
  }

  else if (info.type === "featureCollection") {
    const layer = info.layers[0];
    if (!layer.featureSet?.features?.length) {
      console.log("⚠️ No features found in FeatureCollection.");
      return;
    }
    fields = Object.keys(layer.featureSet.features[0].attributes);
    rowsRaw = layer.featureSet.features.map(f => f.attributes);
    if (rowsRaw.length > MAX_ROWS) {
      rowsRaw = rowsRaw.slice(0, MAX_ROWS);
    }
  }

  console.log(`📦 Total records: ${rowsRaw.length}`);
  console.log(`📊 Total cells to write: ${rowsRaw.length * fields.length}`);

  await clearSheet();
  await writeHeaders(fields);

  const rows = rowsRaw.map(p => fields.map(field => p[field] ?? ""));

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

// --------------------------------------
run().catch(err => {
  console.error("❌ Error:", err);
  process.exit(1);
});