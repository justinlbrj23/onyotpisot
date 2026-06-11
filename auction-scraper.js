const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright");
const { google } = require("googleapis");

const URLS = [
  "https://www.auction.com/residential/KS/Johnson-county/active_lt/auction_date_order_st/y_nbs/foreclosures_at",
  "https://www.auction.com/residential/KS/Wyandotte-county/active_lt/auction_date_order_st/y_nbs/foreclosures_at",
  "https://www.auction.com/residential/KS/Clay-county/active_lt/auction_date_order_st/y_nbs/foreclosures_at",
  "https://www.auction.com/residential/KS/Jackson-county/active_lt/auction_date_order_st/y_nbs/foreclosures_at",
  "https://www.auction.com/residential/KS/Platte-county/active_lt/auction_date_order_st/y_nbs/foreclosures_at"
];

const SPREADSHEET_ID = "15L4mwR_4mdYfWolEVimtzCTPPNpFN0rsFo2U-J7d8jw";
const SHEET_NAME = "Main List";
const SERVICE_ACCOUNT_FILE = path.join(process.cwd(), "service-account.json");
const TIMEZONE = "America/Chicago";
const DATA_START_ROW = 2;

const SELECTORS = {
  pageH1: "h1",
  firstListRowReady: "div.asset-list-row:nth-of-type(1) > div",
  assetCards: "div.b__asset-root--VmozO",
  saleDate: "div[data-elm-id='date_value']"
};

function normalizeText(value) {
  return (value || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function nowInChicago() {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: TIMEZONE,
    hour12: false,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  });

  const parts = formatter.formatToParts(new Date());
  const map = Object.fromEntries(parts.map((p) => [p.type, p.value]));

  return {
    year: Number(map.year),
    month: Number(map.month),
    day: Number(map.day),
    hour: Number(map.hour),
    minute: Number(map.minute),
    second: Number(map.second)
  };
}

function shouldRunNow() {
  // Allow manual testing anytime
  if (process.env.GITHUB_EVENT_NAME === "workflow_dispatch") {
    return true;
  }

  // Only process when the local Chicago time is 07:00
  const t = nowInChicago();
  return t.hour === 7;
}

async function createSheetsClient() {
  if (!fs.existsSync(SERVICE_ACCOUNT_FILE)) {
    throw new Error(`Missing ${SERVICE_ACCOUNT_FILE}`);
  }

  const auth = new google.auth.GoogleAuth({
    keyFile: SERVICE_ACCOUNT_FILE,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });

  const client = await auth.getClient();

  return google.sheets({
    version: "v4",
    auth: client
  });
}

async function getExistingAddressesAndNextRow(sheets) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!E:E`
  });

  const rows = res.data.values || [];
  const existing = new Set();
  let lastUsedRow = 0;

  for (let i = 0; i < rows.length; i++) {
    const rowNum = i + 1;
    const val = (rows[i] && rows[i][0]) ? rows[i][0] : "";
    const norm = normalizeText(val);

    if (norm) {
      lastUsedRow = rowNum;
      if (rowNum >= DATA_START_ROW) {
        existing.add(norm);
      }
    }
  }

  const nextRow = Math.max(lastUsedRow + 1, DATA_START_ROW);
  return { existing, nextRow };
}

async function appendRowsToSheet(sheets, rowsToAppend, startRow) {
  if (!rowsToAppend.length) {
    console.log("No new rows to append.");
    return;
  }

  const endRow = startRow + rowsToAppend.length - 1;
  const range = `${SHEET_NAME}!E${startRow}:F${endRow}`;

  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range,
    valueInputOption: "RAW",
    requestBody: {
      values: rowsToAppend
    }
  });

  console.log(`Appended ${rowsToAppend.length} row(s) to ${range}`);
}

async function safeGoto(page, url) {
  await page.goto(url, {
    waitUntil: "domcontentloaded",
    timeout: 120000
  });

  try {
    await page.waitForLoadState("networkidle", { timeout: 15000 });
  } catch {
    // Ignore if the site keeps background requests open
  }
}

async function getTextOrEmpty(locator) {
  try {
    const txt = await locator.first().innerText({ timeout: 15000 });
    return (txt || "").trim();
  } catch {
    return "";
  }
}

async function scrapeCountyPage(page, url) {
  console.log(`\n[COUNTY] Visiting: ${url}`);
  await safeGoto(page, url);

  await page.waitForSelector(SELECTORS.pageH1, { timeout: 30000 });
  const h1Text = await getTextOrEmpty(page.locator(SELECTORS.pageH1));
  console.log(`[COUNTY] h1 = ${h1Text}`);

  if (normalizeText(h1Text).includes("near")) {
    console.log(`[COUNTY] "near" detected in h1. Skipping this county page.`);
    return [];
  }

  await page.waitForSelector(SELECTORS.firstListRowReady, { timeout: 30000 });

  const links = await page.$$eval(SELECTORS.assetCards, (cards) => {
    const result = [];

    for (const card of cards) {
      const a = card.querySelector("a[href]");
      if (a && a.href) {
        result.push(a.href);
      }
    }

    return Array.from(new Set(result));
  });

  console.log(`[COUNTY] Found ${links.length} property link(s).`);
  return links;
}

async function scrapePropertyDetail(page, detailUrl) {
  console.log(`[DETAIL] Visiting: ${detailUrl}`);
  await safeGoto(page, detailUrl);

  await page.waitForSelector(SELECTORS.pageH1, { timeout: 30000 });

  const address = await getTextOrEmpty(page.locator(SELECTORS.pageH1));
  const saleDate = await getTextOrEmpty(page.locator(SELECTORS.saleDate));

  console.log(`[DETAIL] Address: ${address}`);
  console.log(`[DETAIL] Sale Date: ${saleDate}`);

  return { address, saleDate, detailUrl };
}

async function main() {
  if (!shouldRunNow()) {
    const t = nowInChicago();
    console.log(
      `Skipping run. Local ${TIMEZONE} time is ${String(t.hour).padStart(2, "0")}:${String(t.minute).padStart(2, "0")}, not 07:00.`
    );
    return;
  }

  const sheets = await createSheetsClient();
  const { existing, nextRow } = await getExistingAddressesAndNextRow(sheets);

  console.log(`Existing addresses in column E: ${existing.size}`);
  console.log(`Next target row: ${nextRow}`);

  const browser = await chromium.launch({ headless: true });

  const context = await browser.newContext({
    viewport: { width: 1440, height: 2200 },
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
  });

  const page = await context.newPage();

  const rowsToAppend = [];
  const seenThisRun = new Set();

  try {
    for (const countyUrl of URLS) {
      let propertyLinks = [];

      try {
        propertyLinks = await scrapeCountyPage(page, countyUrl);
      } catch (err) {
        console.error(`[COUNTY] Failed on ${countyUrl}: ${err.message}`);
        continue;
      }

      for (const detailUrl of propertyLinks) {
        try {
          const { address, saleDate } = await scrapePropertyDetail(page, detailUrl);
          const normalizedAddress = normalizeText(address);

          if (!normalizedAddress) {
            console.log(`[DETAIL] Empty address. Skipping.`);
            continue;
          }

          if (existing.has(normalizedAddress)) {
            console.log(`[SHEET] Already exists in column E. Skipping: ${address}`);
            continue;
          }

          if (seenThisRun.has(normalizedAddress)) {
            console.log(`[SHEET] Duplicate in current run. Skipping: ${address}`);
            continue;
          }

          rowsToAppend.push([address, saleDate]);
          seenThisRun.add(normalizedAddress);

          console.log(`[SHEET] Queued => ${address} | ${saleDate}`);
        } catch (err) {
          console.error(`[DETAIL] Failed on ${detailUrl}: ${err.message}`);
        }
      }
    }
  } finally {
    await browser.close();
  }

  await appendRowsToSheet(sheets, rowsToAppend, nextRow);
  console.log("Done.");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
``
