/**
 * Milwaukee County Parcels Property Information Scraper
 * Splits fields across multiple sheets to avoid 10M cell limit
 * Automatically creates new sheet tabs if they don't exist
 */

import fetch from "node-fetch";
import { google } from "googleapis";

const SHEET_ID = "192sAixH2UDvOcb5PL9kSnzLRJUom-0ZiSuTH9cYAi1A";
const SERVICE_ROOT =
  "https://services2.arcgis.com/s1wgJQKbKJihhhaT/arcgis/rest/services/Milwaukee_County_Parcels_Property_Information_view/FeatureServer";
const LAYER_ID = 58;

const ENDPOINT = `${SERVICE_ROOT}/${LAYER_ID}/query`;
const METADATA_URL = `${SERVICE_ROOT}/${LAYER_ID}?f=pjson`;

const PAGE_SIZE = 500;
const MAX_ROWS = 5000;
const BATCH_SIZE = 500;
const CHUNK_SIZE = 20; // number of fields per sheet tab

const ARCGIS_TOKEN = process.env.ARCGIS_TOKEN || "";

const auth = new google.auth.GoogleAuth({
  keyFile: "./service-account.json",
  scopes: ["https://www.googleapis.com/auth/spreadsheets"]
});
const sheets = google.sheets({ version: "v4", auth });

// Retry wrapper
async function fetchWithRetry(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return await res.json();
}

// Get fields
async function getAvailableFields() {
  const url = ARCGIS_TOKEN ? `${METADATA_URL}&token=${ARCGIS_TOKEN}` : METADATA_URL;
  const meta = await fetchWithRetry(url);
  return meta.fields.map(f => f.name);
}

// Fetch page
async function fetchPage(offset, outFields, size) {
  const params = new URLSearchParams({
    where: "1=1",
    outFields: outFields.join(","),
    returnGeometry: "false",
    f: "json",
    resultOffset: offset,
    resultRecordCount: size
  });
  if (ARCGIS_TOKEN) params.append("token", ARCGIS_TOKEN);
  return await fetchWithRetry(`${ENDPOINT}?${params}`);
}

// Ensure sheet tab exists
async function ensureSheetExists(sheetName) {
  const spreadsheet = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  const exists = spreadsheet.data.sheets.some(s => s.properties.title === sheetName);
  if (!exists) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: {
        requests: [
          {
            addSheet: {
              properties: { title: sheetName }
            }
          }
        ]
      }
    });
    console.log(`✅ Created new sheet tab: ${sheetName}`);
  }
}

// Write data to sheet
async function writeToSheet(sheetName, headers, rows) {
  await ensureSheetExists(sheetName);

  await sheets.spreadsheets.values.clear({
    spreadsheetId: SHEET_ID,
    range: sheetName
  });

  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${sheetName}!A1`,
    valueInputOption: "RAW",
    requestBody: { values: [headers] }
  });

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: `${sheetName}!A2`,
      valueInputOption: "RAW",
      insertDataOption: "INSERT_ROWS",
      requestBody: { values: batch }
    });
    console.log(`✅ Appended ${batch.length} rows to ${sheetName}`);
  }
}

// Main
async function run() {
  const fields = await getAvailableFields();
  console.log("📑 Available fields:", fields);

  let offset = 0;
  let parcels = [];
  while (parcels.length < MAX_ROWS) {
    const data = await fetchPage(offset, fields, PAGE_SIZE);
    if (!data.features?.length) break;
    parcels.push(...data.features.map(f => f.attributes));
    offset += PAGE_SIZE;
    if (!data.exceededTransferLimit) break;
  }
  parcels = parcels.slice(0, MAX_ROWS);

  console.log(`📦 Total parcels fetched: ${parcels.length}`);

  // Split fields into chunks of CHUNK_SIZE
  for (let i = 0; i < fields.length; i += CHUNK_SIZE) {
    const fieldChunk = fields.slice(i, i + CHUNK_SIZE);
    const rows = parcels.map(p => fieldChunk.map(f => p[f] ?? ""));
    const sheetName = `Fields_${i / CHUNK_SIZE + 1}`;
    await writeToSheet(sheetName, fieldChunk, rows);
  }

  console.log("✅ Data written across multiple sheets");
}

run().catch(err => {
  console.error("❌ Error:", err);
  process.exit(1);
});