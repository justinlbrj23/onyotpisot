/**
 * Fetch Milwaukee tax-delinquent parcels
 * Auto-create Google Sheet headers
 * Append non-duplicate rows
 */

const fetch = require("node-fetch");
const { google } = require("googleapis");

// =========================
// CONFIG
// =========================
const SHEET_ID = "192sAixH2UDvOcb5PL9kSnzLRJUom-0ZiSuTH9cYAi1A";
const SHEET_NAME = "Sheet1";

const HEADERS = [
  "TAXKEY",
  "OWNER",
  "ADDRESS",
  "CITY",
  "DELQ",
  "NET_TAX",
  "LAST_SYNC"
];

const ENDPOINT =
  "https://milwaukeemaps.milwaukee.gov/arcgis/rest/services/property/parcels_mprop/MapServer/0/query";

const PAGE_SIZE = 2000;

// =========================
// GOOGLE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: "./service-account.json",
  scopes: ["https://www.googleapis.com/auth/spreadsheets"]
});

const sheets = google.sheets({ version: "v4", auth });

// =========================
// ARC GIS FETCH
// =========================
async function fetchPage(offset) {
  const params = new URLSearchParams({
    where: "TAX_DELQ > 0",
    outFields: "*",
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
async function ensureHeaders() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A1:G1`
  });

  const existing = res.data.values?.[0];

  if (!existing || existing.length === 0) {
    console.log("🧾 Writing sheet headers...");
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!A1`,
      valueInputOption: "RAW",
      requestBody: {
        values: [HEADERS]
      }
    });
  } else {
    console.log("🧾 Headers already exist");
  }
}

async function getExistingKeys() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A2:A`
  });

  return new Set((res.data.values || []).flat());
}

async function appendRows(rows) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A2`,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values: rows }
  });
}

// =========================
// MAIN
// =========================
async function run() {
  let offset = 0;
  let parcels = [];
  let hasMore = true;

  console.log("🔎 Fetching delinquent parcels...");

  while (hasMore) {
    const data = await fetchPage(offset);
    if (!data.features?.length) break;

    parcels.push(...data.features.map(f => f.attributes));
    offset += PAGE_SIZE;
    hasMore = data.exceededTransferLimit === true;
  }

  console.log(`📦 Pulled ${parcels.length} records`);

  // ✅ Ensure headers FIRST
  await ensureHeaders();

  const existingKeys = await getExistingKeys();
  const rows = [];

  for (const p of parcels) {
    const taxKey = p.TAXKEY?.toString();
    if (!taxKey || existingKeys.has(taxKey)) continue;

    rows.push([
      taxKey,
      p.OWNER_NAME_1 || "",
      p.SITE_ADDR || "",
      p.MUNICIPALITY || "",
      p.TAX_DELQ || "",
      p.NET_TAX || "",
      new Date().toISOString()
    ]);
  }

  if (!rows.length) {
    console.log("✅ No new rows to append");
    return;
  }

  await appendRows(rows);
  console.log(`✅ Appended ${rows.length} new rows`);
}

run().catch(err => {
  console.error("❌ Error:", err);
  process.exit(1);
});