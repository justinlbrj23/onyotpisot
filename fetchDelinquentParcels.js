/**
 * Zillow Listing Scraper → Google Sheets
 * ----------------------------------------
 * ✔ Uses Zillow search API
 * ✔ Handles pagination
 * ✔ Extracts listing + agent + broker
 * ✔ Deduplicates by zpid
 * ✔ Prevents Google 10M overflow
 */

import fetch from "node-fetch";
import { google } from "googleapis";

// --------------------------------------
// CONFIG
// --------------------------------------
const SHEET_ID = "1woBGXySPNxQavq_3FMsNK4WnqhJ4w_Oh5Ui_05rzzL0";
const SHEET_NAME = "Sheet1";

const STATE = "Florida"; // Change per run
const MAX_PAGES = 20;
const BATCH_SIZE = 200;

const GOOGLE_CELL_LIMIT = 10000000;
const SAFETY_BUFFER = 5000;

// --------------------------------------
// GOOGLE AUTH
// --------------------------------------
const auth = new google.auth.GoogleAuth({
  keyFile: "./service-account.json",
  scopes: ["https://www.googleapis.com/auth/spreadsheets"]
});
const sheets = google.sheets({ version: "v4", auth });

// --------------------------------------
// HEADERS (Mimic Real Browser)
// --------------------------------------
const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
  "Accept": "application/json",
  "Content-Type": "application/json"
};

// --------------------------------------
// FETCH ZILLOW SEARCH PAGE
// --------------------------------------
async function fetchZillowPage(page = 1) {
  const searchQueryState = {
    pagination: { currentPage: page },
    mapBounds: {},
    regionSelection: [
      {
        regionName: STATE,
        regionType: 2
      }
    ],
    filterState: {},
    isListVisible: true
  };

  const url = `https://www.zillow.com/search/GetSearchPageState.htm?searchQueryState=${encodeURIComponent(
    JSON.stringify(searchQueryState)
  )}&wants={"cat1":["listResults"]}&requestId=${page}`;

  const res = await fetch(url, { headers: HEADERS });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return await res.json();
}

// --------------------------------------
// GOOGLE SHEET HELPERS
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
  console.log(`🔎 Scraping Zillow for ${STATE}`);

  const listingsMap = new Map();

  for (let page = 1; page <= MAX_PAGES; page++) {
    console.log(`📄 Page ${page}`);

    const data = await fetchZillowPage(page);

    const results =
      data?.cat1?.searchResults?.listResults || [];

    if (!results.length) break;

    for (const item of results) {
      if (!item.zpid) continue;

      listingsMap.set(item.zpid, {
        zpid: item.zpid,
        address: item.address,
        city: item.city,
        state: item.state,
        price: item.price,
        beds: item.beds,
        baths: item.baths,
        area: item.area,
        detailUrl: `https://www.zillow.com${item.detailUrl}`,
        brokerName: item.brokerName || "",
        agentName: item.agentName || ""
      });
    }
  }

  const listings = Array.from(listingsMap.values());

  if (!listings.length) {
    console.log("No listings found.");
    return;
  }

  const headers = Object.keys(listings[0]);

  // Prevent sheet overflow
  const maxRowsAllowed = Math.floor(
    (GOOGLE_CELL_LIMIT - SAFETY_BUFFER) / headers.length
  );

  const trimmed =
    listings.length > maxRowsAllowed
      ? listings.slice(0, maxRowsAllowed)
      : listings;

  console.log(`📦 Writing ${trimmed.length} listings`);

  await clearSheet();
  await writeHeaders(headers);

  const rows = trimmed.map(obj =>
    headers.map(h => obj[h] ?? "")
  );

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    await appendRowsBatch(batch);
    console.log(`✅ Batch ${i / BATCH_SIZE + 1}`);
  }

  console.log("🎉 Done");
}

run().catch(err => {
  console.error("❌ Error:", err);
  process.exit(1);
});