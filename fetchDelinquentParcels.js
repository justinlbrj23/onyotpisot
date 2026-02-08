/**
 * Milwaukee County Parcels Property Information Scraper
 *
 * ✔ Fetches parcel data from ArcGIS
 * ✔ Creates a NEW sheet per run
 * ✔ Writes headers + rows in batches
 * ✔ Deletes all previous Milwaukee_* sheets after success
 * ✔ Avoids Google Sheets 10M cell limit permanently
 * ✔ Retries ArcGIS requests with backoff
 */

import fetch from "node-fetch";
import { google } from "googleapis";

// =========================
// CONFIG
// =========================
const SHEET_ID = "192sAixH2UDvOcb5PL9kSnzLRJUom-0ZiSuTH9cYAi1A";
const SHEET_NAME = `Milwaukee_${new Date().toISOString().replace(/[:.]/g, "-")}`;

const SERVICE_ROOT =
  "https://services2.arcgis.com/s1wgJQKbKJihhhaT/arcgis/rest/services/Milwaukee_County_Parcels_Property_Information_view/FeatureServer";

const LAYER_ID = 58;
const ENDPOINT = `${SERVICE_ROOT}/${LAYER_ID}/query`;
const METADATA_URL = `${SERVICE_ROOT}/${LAYER_ID}?f=pjson`;

const PAGE_SIZE = 500;
const MAX_ROWS = 5000;
const MAX_RETRIES = 3;
const BATCH_SIZE = 500;

const ARCGIS_TOKEN = process.env.ARCGIS_TOKEN || "";

// =========================
// GOOGLE SHEETS AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: "./service-account.json",
  scopes: ["https://www.googleapis.com/auth/spreadsheets"]
});

const sheets = google.sheets({ version: "v4", auth });

// =========================
// FIELDS (minimized)
// =========================
const FIELDS_TO_KEEP = [
  "TAXKEY",
  "OWNERNAME1",
  "OWNERADDR",
  "MUNINAME",
  "ADDRESS",
  "ACRES",
  "DESCRIPTION"
];

// =========================
// FETCH WITH RETRY
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
      await new Promise(r => setTimeout(r, 1000 * attempt));
    }
  }
}

// =========================
// ARCGIS HELPERS
// =========================
async function getAvailableFields() {
  const url = ARCGIS_TOKEN ? `${METADATA_URL}&token=${ARCGIS_TOKEN}` : METADATA_URL;
  const meta = await fetchWithRetry(url);
  return meta.fields?.map(f => f.name) || [];
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
  if (ARCGIS_TOKEN) params.append("token", ARCGIS_TOKEN);
  return fetchWithRetry(`${ENDPOINT}?${params}`);
}

// =========================
// SHEET HELPERS
// =========================
async function getSheets() {
  const res = await sheets.spreadsheets.get({ spreadsheetId: SHEET_ID });
  return res.data.sheets || [];
}

async function createSheet(sheetName, columnCount) {
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      requests: [
        {
          addSheet: {
            properties: {
              title: sheetName,
              gridProperties: {
                rowCount: 1000,
                columnCount
              }
            }
          }
        }
      ]
    }
  });
  console.log(`🆕 Created sheet: ${sheetName}`);
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

async function deleteOldMilwaukeeSheets(currentSheetName) {
  const allSheets = await getSheets();

  const oldSheets = allSheets.filter(
    s =>
      s.properties.title.startsWith("Milwaukee_") &&
      s.properties.title !== currentSheetName
  );

  if (!oldSheets.length) return;

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      requests: oldSheets.map(s => ({
        deleteSheet: { sheetId: s.properties.sheetId }
      }))
    }
  });

  console.log(`🗑 Deleted ${oldSheets.length} old Milwaukee sheets`);
}

// =========================
// MAIN
// =========================
async function run() {
  const allFields = await getAvailableFields();
  const selectedFields = allFields.filter(f => FIELDS_TO_KEEP.includes(f));

  console.log("📑 Using fields:", selectedFields);

  let offset = 0;
  let parcels = [];
  let hasMore = true;

  console.log("🔎 Fetching parcels from ArcGIS...");

  while (hasMore && parcels.length < MAX_ROWS) {
    const data = await fetchPage(offset, selectedFields, PAGE_SIZE);
    if (!data.features?.length) break;

    parcels.push(...data.features.map(f => f.attributes));
    offset += PAGE_SIZE;
    hasMore = data.exceededTransferLimit === true;
  }

  parcels = parcels.slice(0, MAX_ROWS);
  console.log(`📦 Total parcels fetched: ${parcels.length}`);

  // ✅ CREATE NEW SHEET
  await createSheet(SHEET_NAME, selectedFields.length);

  // ✅ WRITE HEADERS
  await writeHeaders(selectedFields);

  const rows = parcels.map(p =>
    selectedFields.map(field => p[field] ?? "")
  );

  // ✅ WRITE ROWS IN BATCHES
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    await appendRowsBatch(batch);
    console.log(`✅ Appended batch ${i / BATCH_SIZE + 1}`);
  }

  // ✅ DELETE OLD SHEETS
  await deleteOldMilwaukeeSheets(SHEET_NAME);

  console.log("🎉 Milwaukee parcel import complete");
}

run().catch(err => {
  console.error("❌ Fatal error:", err);
  process.exit(1);
});