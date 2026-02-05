/**
 * Milwaukee County Tax-Delinquent Parcels Scraper
 * Clears sheet, then logs results directly into Google Sheets
 * Auto-detects TAX_DELQ type (numeric vs string)
 * Overwrites instead of appending to avoid 10M cell limit
 */

import fetch from "node-fetch";
import { google } from "googleapis";
import fs from "fs";

// =========================
// CONFIG
// =========================
const SHEET_ID = "192sAixH2UDvOcb5PL9kSnzLRJUom-0ZiSuTH9cYAi1A";
const SHEET_NAME = "Sheet1"; // Change if your tab has a different name

const HEADERS = [
  "TAXKEY",
  "OWNER",
  "ADDRESS",
  "CITY",
  "DELQ",
  "NET_TAX",
  "LAST_SYNC"
];

// Use the MPROP layer (layer 1) which has ownership and delinquency fields
const ENDPOINT =
  "https://milwaukeemaps.milwaukee.gov/arcgis/rest/services/property/parcels_mprop/MapServer/1/query";

const METADATA_URL =
  "https://milwaukeemaps.milwaukee.gov/arcgis/rest/services/property/parcels_mprop/MapServer/1?f=pjson";

const PAGE_SIZE = 2000;

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
async function detectDelqType() {
  // Fetch one sample record to inspect TAX_DELQ
  const params = new URLSearchParams({
    where: "1=1",
    outFields: "TAX_DELQ",
    returnGeometry: "false",
    f: "json",
    resultRecordCount: 1
  });

  const res = await fetch(`${ENDPOINT}?${params}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();

  const sample = data.features?.[0]?.attributes?.TAX_DELQ;
  console.log("🔍 Sample TAX_DELQ value:", sample);

  if (sample == null) return { field: null, type: "none" };
  if (typeof sample === "number") return { field: "TAX_DELQ", type: "numeric" };

  // If it's a string, check common values
  const val = sample.toString().trim().toUpperCase();
  if (["Y", "YES", "TRUE"].includes(val)) return { field: "TAX_DELQ", type: "stringFlag" };

  // If it's a string number
  if (!isNaN(Number(val))) return { field: "TAX_DELQ", type: "numericString" };

  return { field: "TAX_DELQ", type: "stringOther" };
}

async function fetchPage(offset, delqInfo) {
  let where = "1=1";
  if (delqInfo.field) {
    switch (delqInfo.type) {
      case "numeric":
        where = `${delqInfo.field} > 0`;
        break;
      case "numericString":
        where = `${delqInfo.field} <> '0' AND ${delqInfo.field} IS NOT NULL`;
        break;
      case "stringFlag":
        where = `${delqInfo.field} = 'Y' OR ${delqInfo.field} = 'YES' OR ${delqInfo.field} = 'TRUE'`;
        break;
      case "stringOther":
        // fallback: exclude blanks
        where = `${delqInfo.field} IS NOT NULL AND ${delqInfo.field} <> ''`;
        break;
    }
  }

  const outFields = [
    "TAXKEY",
    "OWNER_NAME_1",
    "ADDRESS",
    "OWNER_MAIL_ADDR",
    "OWNER_CITY_STATE",
    "OWNER_ZIP",
    "CITY",
    delqInfo.field || "",
    "NET_TAX"
  ].filter(Boolean).join(",");

  const params = new URLSearchParams({
    where,
    outFields,
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
  const delqInfo = await detectDelqType();

  let offset = 0;
  let hasMore = true;
  let parcels = [];

  console.log("🔎 Fetching parcels from ArcGIS...");

  while (hasMore) {
    const data = await fetchPage(offset, delqInfo);
    if (!data.features?.length) break;

    console.log(`➡️ Page fetched: ${data.features.length} records`);
    parcels.push(...data.features.map(f => f.attributes));
    offset += PAGE_SIZE;
    hasMore = data.exceededTransferLimit === true;
  }

  console.log(`📦 Total parcels fetched: ${parcels.length}`);

  // Clear sheet and re-write headers
  await clearSheet();
  await writeHeaders();

  if (parcels.length > 0) {
    console.log("🔍 Sample record keys:", Object.keys(parcels[0]));
  }

  const rows = [];

  for (const p of parcels) {
    const taxKey = p.TAXKEY?.toString();
    if (!taxKey) continue;

    const owner = p.OWNER_NAME_1 || "";
    const address = p.ADDRESS || "";
    const city = p.CITY || p.OWNER_CITY_STATE || "";
    const delqVal = delqInfo.field ? p[delqInfo.field] || "" : "";
    const netTax = p.NET_TAX || "";

    rows.push([
      taxKey,
      owner,
      address,
      city,
      delqVal,
      netTax,
      new Date().toISOString()
    ]);
  }

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