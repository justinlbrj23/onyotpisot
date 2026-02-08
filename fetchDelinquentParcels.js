/**
 * Milwaukee County Parcels Property Information Scraper
 *
 * 🔒 CELL-LIMIT SAFE STRATEGY
 * - Create TEMP sheet
 * - Write data once
 * - Delete OLD sheet (frees cells)
 * - Rename TEMP → MAIN
 *
 * This prevents the Google Sheets 10M cell limit permanently.
 */

import fetch from "node-fetch";
import { google } from "googleapis";

/* =========================
   CONFIG
========================= */

const SPREADSHEET_ID = "192sAixH2UDvOcb5PL9kSnzLRJUom-0ZiSuTH9cYAi1A";
const MAIN_SHEET_NAME = "Milwaukee_Parcels";
const TEMP_SHEET_NAME = `TEMP_${Date.now()}`;

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

/* =========================
   GOOGLE AUTH
========================= */

const auth = new google.auth.GoogleAuth({
  keyFile: "./service-account.json",
  scopes: ["https://www.googleapis.com/auth/spreadsheets"]
});

const sheets = google.sheets({ version: "v4", auth });

/* =========================
   FIELDS (LIMIT CELLS)
========================= */

const FIELDS_TO_KEEP = [
  "TAXKEY",
  "OWNERNAME1",
  "OWNERADDR",
  "MUNINAME",
  "ADDRESS",
  "ACRES",
  "DESCRIPTION"
];

/* =========================
   FETCH WITH RETRY
========================= */

async function fetchWithRetry(url, retries = MAX_RETRIES) {
  for (let i = 1; i <= retries; i++) {
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (err) {
      console.warn(`⚠️ Fetch attempt ${i} failed`);
      if (i === retries) throw err;
      await new Promise(r => setTimeout(r, i * 1000));
    }
  }
}

/* =========================
   ARC GIS
========================= */

async function getAvailableFields() {
  const url = ARCGIS_TOKEN ? `${METADATA_URL}&token=${ARCGIS_TOKEN}` : METADATA_URL;
  const meta = await fetchWithRetry(url);
  return meta.fields?.map(f => f.name) || [];
}

async function fetchPage(offset, fields) {
  const params = new URLSearchParams({
    where: "1=1",
    outFields: fields.join(","),
    returnGeometry: "false",
    f: "json",
    resultOffset: offset,
    resultRecordCount: PAGE_SIZE
  });

  if (ARCGIS_TOKEN) params.append("token", ARCGIS_TOKEN);
  return fetchWithRetry(`${ENDPOINT}?${params}`);
}

/* =========================
   SHEET HELPERS
========================= */

async function createTempSheet() {
  const res = await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: [{
        addSheet: {
          properties: { title: TEMP_SHEET_NAME }
        }
      }]
    }
  });

  return res.data.replies[0].addSheet.properties.sheetId;
}

async function writeBatch(startRow, rows) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `${TEMP_SHEET_NAME}!A${startRow}`,
    valueInputOption: "RAW",
    requestBody: { values: rows }
  });
}

async function deleteMainSheetIfExists() {
  const meta = await sheets.spreadsheets.get({
    spreadsheetId: SPREADSHEET_ID
  });

  const sheet = meta.data.sheets.find(
    s => s.properties.title === MAIN_SHEET_NAME
  );

  if (!sheet) return;

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: [{
        deleteSheet: {
          sheetId: sheet.properties.sheetId
        }
      }]
    }
  });
}

async function renameTempSheet(sheetId) {
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: [{
        updateSheetProperties: {
          properties: {
            sheetId,
            title: MAIN_SHEET_NAME
          },
          fields: "title"
        }
      }]
    }
  });
}

/* =========================
   MAIN
========================= */

async function run() {
  console.log("🔍 Fetching field metadata...");
  const fields = await getAvailableFields();
  const selectedFields = fields.filter(f => FIELDS_TO_KEEP.includes(f));

  console.log("📡 Fetching parcel data...");
  let offset = 0;
  let rows = [];
  let hasMore = true;

  while (hasMore && rows.length < MAX_ROWS) {
    const data = await fetchPage(offset, selectedFields);
    if (!data.features?.length) break;

    rows.push(
      ...data.features.map(f =>
        selectedFields.map(k => f.attributes[k] ?? "")
      )
    );

    offset += PAGE_SIZE;
    hasMore = data.exceededTransferLimit === true;
  }

  rows = rows.slice(0, MAX_ROWS);

  if (!rows.length) {
    console.log("⚠️ No data returned");
    return;
  }

  console.log(`📦 Rows fetched: ${rows.length}`);

  /* =========================
     WRITE FLOW
  ========================= */

  console.log("🆕 Creating temp sheet...");
  const tempSheetId = await createTempSheet();

  console.log("✍️ Writing data...");
  await writeBatch(1, [selectedFields]);

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    await writeBatch(i + 2, rows.slice(i, i + BATCH_SIZE));
  }

  console.log("🗑️ Deleting old sheet...");
  await deleteMainSheetIfExists();

  console.log("🔁 Renaming temp → main...");
  await renameTempSheet(tempSheetId);

  console.log("✅ DONE — cell limit permanently avoided");
}

run().catch(err => {
  console.error("❌ Fatal error:", err);
  process.exit(1);
});