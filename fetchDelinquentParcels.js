/**
 * 💣 NUCLEAR Google Sheets Fix
 * Deletes ALL sheets except the new one
 * Guaranteed to eliminate 10M cell limit errors
 */

import fetch from "node-fetch";
import { google } from "googleapis";

/* ================= CONFIG ================= */

const SPREADSHEET_ID = "192sAixH2UDvOcb5PL9kSnzLRJUom-0ZiSuTH9cYAi1A";
const FINAL_SHEET_NAME = "Milwaukee_Parcels";
const TEMP_SHEET_NAME = `TEMP_${Date.now()}`;

const SERVICE_ROOT =
  "https://services2.arcgis.com/s1wgJQKbKJihhhaT/arcgis/rest/services/Milwaukee_County_Parcels_Property_Information_view/FeatureServer";

const LAYER_ID = 58;
const ENDPOINT = `${SERVICE_ROOT}/${LAYER_ID}/query`;
const METADATA_URL = `${SERVICE_ROOT}/${LAYER_ID}?f=pjson`;

const PAGE_SIZE = 500;
const MAX_ROWS = 5000;
const BATCH_SIZE = 500;
const MAX_RETRIES = 3;

const ARCGIS_TOKEN = process.env.ARCGIS_TOKEN || "";

/* ================= GOOGLE AUTH ================= */

const auth = new google.auth.GoogleAuth({
  keyFile: "./service-account.json",
  scopes: ["https://www.googleapis.com/auth/spreadsheets"]
});

const sheets = google.sheets({ version: "v4", auth });

/* ================= FIELDS ================= */

const FIELDS_TO_KEEP = [
  "TAXKEY",
  "OWNERNAME1",
  "OWNERADDR",
  "MUNINAME",
  "ADDRESS",
  "ACRES",
  "DESCRIPTION"
];

/* ================= FETCH ================= */

async function fetchWithRetry(url, retries = MAX_RETRIES) {
  for (let i = 1; i <= retries; i++) {
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch {
      if (i === retries) throw err;
      await new Promise(r => setTimeout(r, i * 1000));
    }
  }
}

/* ================= ARC GIS ================= */

async function getFields() {
  const url = ARCGIS_TOKEN ? `${METADATA_URL}&token=${ARCGIS_TOKEN}` : METADATA_URL;
  const meta = await fetchWithRetry(url);
  return meta.fields.map(f => f.name).filter(f => FIELDS_TO_KEEP.includes(f));
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

/* ================= SHEETS ================= */

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

async function deleteAllOtherSheets(keepSheetId) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });

  const requests = meta.data.sheets
    .filter(s => s.properties.sheetId !== keepSheetId)
    .map(s => ({
      deleteSheet: { sheetId: s.properties.sheetId }
    }));

  if (!requests.length) return;

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: { requests }
  });
}

async function renameSheet(sheetId) {
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: [{
        updateSheetProperties: {
          properties: {
            sheetId,
            title: FINAL_SHEET_NAME
          },
          fields: "title"
        }
      }]
    }
  });
}

async function writeRows(startRow, rows) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `${TEMP_SHEET_NAME}!A${startRow}`,
    valueInputOption: "RAW",
    requestBody: { values: rows }
  });
}

/* ================= MAIN ================= */

async function run() {
  const fields = await getFields();

  let offset = 0;
  let rows = [];

  while (rows.length < MAX_ROWS) {
    const data = await fetchPage(offset, fields);
    if (!data.features?.length) break;

    rows.push(
      ...data.features.map(f =>
        fields.map(k => f.attributes[k] ?? "")
      )
    );

    offset += PAGE_SIZE;
    if (!data.exceededTransferLimit) break;
  }

  const tempSheetId = await createTempSheet();

  await writeRows(1, [fields]);

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    await writeRows(i + 2, rows.slice(i, i + BATCH_SIZE));
  }

  // 💣 THIS IS THE FIX
  await deleteAllOtherSheets(tempSheetId);
  await renameSheet(tempSheetId);

  console.log("✅ SUCCESS — spreadsheet fully reset");
}

run().catch(err => {
  console.error("❌ Fatal:", err);
  process.exit(1);
});