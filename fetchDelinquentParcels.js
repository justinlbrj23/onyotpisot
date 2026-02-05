/**
 * Milwaukee County Tax-Delinquent Parcels Scraper
 * Clears sheet, then logs results directly into Google Sheets
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

// IMPORTANT: Use the MPROP layer instead of layer 0
// Layer 1 typically contains the Master Property File attributes
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
async function getDelinquencyField() {
  const res = await fetch(METADATA_URL);
  if (!res.ok) throw new Error(`Metadata HTTP ${res.status}`);
  const meta = await res.json();

  const fields = meta.fields.map(f => f.name.toUpperCase());
  console.log("📑 Available fields:", fields);

  // Try common candidates
  const candidates = ["TAXDELQ_AMT", "TAX_DELQ", "DELINQUENT_TAX", "TAX_STATUS"];
  const found = candidates.find(c => fields.includes(c));

  if (found) {
    console.log(`✅ Using delinquency field: ${found}`);
    return found;
  }

  console.warn("⚠️ No known delinquency field found. Falling back to 1=1 query.");
  return null;
}

async function fetchPage(offset, delinquencyField) {
  const where = delinquencyField ? `${delinquencyField} > 0` : "1=1";

  // Request only needed fields to reduce payload
  const outFields = [
    "TAXKEY",
    "OWNER_NAME",
    "OWNER_NAME2",
    "PROP_ADDR",
    "PROP_HOUSE_NR",
    "PROP_STREET",
    "MUNI",
    "CITY",
    "NET_TAX",
    delinquencyField || ""
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
  const delinquencyField = await getDelinquencyField();

  let offset = 0;
  let hasMore = true;
  let parcels = [];

  console.log("🔎 Fetching parcels from ArcGIS...");

  while (hasMore) {
    const data = await fetchPage(offset, delinquencyField);
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

    // Flexible mapping: try multiple possible field names
    const owner =
      p.OWNER_NAME || p.OWNER_NAME2 || p.OWNER || "";
    const address =
      p.PROP_ADDR ||
      `${p.PROP_HOUSE_NR || ""} ${p.PROP_STREET || ""}`.trim() ||
      p.ADDRESS || "";
    const city =
      p.MUNI || p.CITY || "";

    rows.push([
      taxKey,
      owner,
      address,
      city,
      delinquencyField ? p[delinquencyField] || "" : "",
      p.NET_TAX || "",
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