// county-aware-scraper.js
const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright");
const { google } = require("googleapis");

const URLS = [
  "https://www.auction.com/residential/KS/Johnson-county/active_lt/auction_date_order_st/y_nbs/foreclosures_at",
  "https://www.auction.com/residential/KS/Wyandotte-county/active_lt/auction_date_order_st/y_nbs/foreclosures_at",
  "https://www.auction.com/residential/MO/Clay-county/active_lt/auction_date_order_st/y_nbs/foreclosures_at",
  "https://www.auction.com/residential/MO/Jackson-county/active_lt/auction_date_order_st/y_nbs/foreclosures_at",
  "https://www.auction.com/residential/MO/Platte-county/active_lt/auction_date_order_st/y_nbs/foreclosures_at"
];

const SPREADSHEET_ID = "15L4mwR_4mdYfWolEVimtzCTPPNpFN0rsFo2U-J7d8jw";
const SHEET_NAME = "Main List";
const SERVICE_ACCOUNT_FILE = path.join(process.cwd(), "service-account.json");
const TIMEZONE = "America/Chicago";
const DATA_START_ROW = 2;

const SELECTORS = {
  pageH1: "h1",
  assetCards: "div.b__asset-root--VmozO",
  listRow: "div.asset-list-row",
  saleDate: "div[data-elm-id='date_value']",
  loadMoreButton: "button:has-text('Load More'), button:has-text('Load more')"
};

function normalizeText(value) {
  return (value || "").replace(/\s+/g, " ").trim().toLowerCase();
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
  if (process.env.FORCE_RUN === "true") return true;
  if (process.env.GITHUB_EVENT_NAME === "workflow_dispatch") return true;
  const t = nowInChicago();
  return t.hour === 7;
}

async function createSheetsClient() {
  let keyFile = SERVICE_ACCOUNT_FILE;
  if (!fs.existsSync(keyFile)) {
    if (process.env.GOOGLE_APPLICATION_CREDENTIALS && fs.existsSync(process.env.GOOGLE_APPLICATION_CREDENTIALS)) {
      keyFile = process.env.GOOGLE_APPLICATION_CREDENTIALS;
    } else {
      throw new Error(`Missing service account credentials. Put a service-account.json in the project root or set GOOGLE_APPLICATION_CREDENTIALS.`);
    }
  }
  const auth = new google.auth.GoogleAuth({
    keyFile,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });
  const client = await auth.getClient();
  return google.sheets({ version: "v4", auth: client });
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
      if (rowNum >= DATA_START_ROW) existing.add(norm);
    }
  }
  const nextRow = Math.max(lastUsedRow + 1, DATA_START_ROW);
  return { existing, nextRow };
}

async function appendRowsToSheet(sheets, rowsToAppend) {
  if (!rowsToAppend.length) {
    console.log("No new rows to append.");
    return;
  }
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!E:F`,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values: rowsToAppend }
  });
  console.log(`Appended ${rowsToAppend.length} row(s) to sheet ${SHEET_NAME}`);
}

async function safeGoto(page, url) {
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 120000 });
  try {
    await page.waitForLoadState("networkidle", { timeout: 15000 });
  } catch {
    // ignore if network stays busy
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

async function autoScroll(page, maxScrolls = 18, delayMs = 600) {
  let lastHeight = await page.evaluate(() => document.body.scrollHeight);
  for (let i = 0; i < maxScrolls; i++) {
    await page.evaluate(() => window.scrollBy(0, window.innerHeight));
    await page.waitForTimeout(delayMs);
    const newHeight = await page.evaluate(() => document.body.scrollHeight);
    if (newHeight === lastHeight) {
      await page.waitForTimeout(500);
      const finalHeight = await page.evaluate(() => document.body.scrollHeight);
      if (finalHeight === lastHeight) break;
    }
    lastHeight = newHeight;
  }
}

async function tryClickLoadMore(page) {
  try {
    const btn = await page.$(SELECTORS.loadMoreButton);
    if (btn) {
      console.log("[COUNTY] Found Load More button. Clicking repeatedly.");
      for (let i = 0; i < 6; i++) {
        try {
          await btn.click({ timeout: 5000 });
          await page.waitForTimeout(1200);
        } catch {
          break;
        }
      }
      return true;
    }
  } catch (err) {
    console.warn("[COUNTY] Error clicking Load More:", err.message);
  }
  return false;
}

/**
 * Extract county slug from a county URL.
 * Example: ".../MO/Jackson-county/..." -> "jackson-county"
 * Also returns a short name without "-county": "jackson"
 */
function extractCountySlug(countyUrl) {
  try {
    const m = countyUrl.match(/\/([A-Za-z0-9-]+-county)(?:\/|$)/i);
    if (m && m[1]) {
      const slug = m[1].toLowerCase();
      const short = slug.replace(/-county$/, "");
      return { slug, short };
    }
  } catch {
    // fall through
  }
  return { slug: null, short: null };
}

/**
 * Robust county page scraper:
 * - scrolls to load lazy items
 * - clicks "Load more" if present
 * - tries multiple extraction strategies
 * - filters links to those matching the county slug or short name
 * - saves debug HTML if no links found
 */
async function scrapeCountyPage(page, url) {
  console.log(`\n[COUNTY] Visiting: ${url}`);
  await safeGoto(page, url);

  try {
    await page.waitForSelector(SELECTORS.pageH1, { timeout: 20000 });
  } catch {
    // continue even if H1 not found
  }
  const h1Text = await getTextOrEmpty(page.locator(SELECTORS.pageH1));
  console.log(`[COUNTY] h1 = ${h1Text}`);
  if (normalizeText(h1Text).includes("near")) {
    console.log(`[COUNTY] "near" detected in h1. Skipping this county page.`);
    return [];
  }

  // load more + scroll to trigger lazy loading
  await tryClickLoadMore(page);
  await autoScroll(page, 18, 600);

  // extract county slug/short for filtering
  const { slug: countySlug, short: countyShort } = extractCountySlug(url);
  const countySlugLower = countySlug ? countySlug.toLowerCase() : null;
  const countyShortLower = countyShort ? countyShort.toLowerCase() : null;

  // helper to filter links by county
  const filterByCounty = (href) => {
    if (!href) return false;
    const h = href.toLowerCase();
    if (countySlugLower && h.includes(countySlugLower)) return true;
    if (countyShortLower && h.includes(countyShortLower)) return true;
    // sometimes county appears as query param or different token; allow state+city patterns too
    return false;
  };

  // Strategy A: known asset card selector
  try {
    const linksA = await page.$$eval(SELECTORS.assetCards, (cards) => {
      const out = [];
      for (const c of cards) {
        const a = c.querySelector("a[href]");
        if (a && a.href) out.push(a.href);
      }
      return Array.from(new Set(out));
    });
    const filteredA = linksA.filter(filterByCounty);
    if (filteredA.length) {
      console.log(`[COUNTY] Strategy A found ${filteredA.length} county-matching link(s).`);
      return filteredA;
    } else {
      console.log(`[COUNTY] Strategy A found 0 county-matching links (total found: ${linksA.length}).`);
    }
  } catch (err) {
    console.warn(`[COUNTY] Strategy A error: ${err.message}`);
  }

  // Strategy B: anchors inside list rows
  try {
    const linksB = await page.$$eval(SELECTORS.listRow, (rows) => {
      const out = [];
      for (const r of rows) {
        const a = r.querySelector("a[href]");
        if (a && a.href) out.push(a.href);
      }
      return Array.from(new Set(out));
    });
    const filteredB = linksB.filter(filterByCounty);
    if (filteredB.length) {
      console.log(`[COUNTY] Strategy B found ${filteredB.length} county-matching link(s).`);
      return filteredB;
    } else {
      console.log(`[COUNTY] Strategy B found 0 county-matching links (total found: ${linksB.length}).`);
    }
  } catch (err) {
    console.warn(`[COUNTY] Strategy B error: ${err.message}`);
  }

  // Strategy C: any anchor that looks like a property detail
  try {
    const linksC = await page.$$eval("a[href]", (anchors) => {
      const out = [];
      for (const a of anchors) {
        const href = (a.href || "").toLowerCase();
        if (!href) continue;
        if (href.includes("/property/") || href.includes("/asset/") || href.includes("/residential/")) {
          out.push(a.href);
        }
      }
      return Array.from(new Set(out));
    });
    const filteredC = linksC.filter(filterByCounty);
    if (filteredC.length) {
      console.log(`[COUNTY] Strategy C found ${filteredC.length} county-matching link(s).`);
      return filteredC;
    } else {
      console.log(`[COUNTY] Strategy C found 0 county-matching links (total found: ${linksC.length}).`);
    }
  } catch (err) {
    console.warn(`[COUNTY] Strategy C error: ${err.message}`);
  }

  // If nothing found, save a debug HTML snapshot for inspection
  try {
    const html = await page.content();
    const debugPath = path.join(process.cwd(), "debug-jackson.html");
    fs.writeFileSync(debugPath, html, "utf8");
    console.warn(`[COUNTY] No county-matching property links found. Saved debug HTML to ${debugPath}`);
  } catch (err) {
    console.warn("[COUNTY] Failed to save debug HTML:", err.message);
  }

  return [];
}

async function scrapePropertyDetail(page, detailUrl) {
  console.log(`[DETAIL] Visiting: ${detailUrl}`);
  await safeGoto(page, detailUrl);
  try {
    await page.waitForSelector(SELECTORS.pageH1, { timeout: 20000 });
  } catch {
    // continue
  }
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
      `Skipping run. Local ${TIMEZONE} time is ${String(t.hour).padStart(2, "0")}:${String(t.minute).padStart(2, "0")}. Set FORCE_RUN=true to override.`
    );
    return;
  }

  const sheets = await createSheetsClient();
  const { existing } = await getExistingAddressesAndNextRow(sheets);
  console.log(`Existing addresses in column E: ${existing.size}`);

  const browser = await chromium.launch({
    headless: process.env.FORCE_RUN === "true" ? false : true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
    slowMo: process.env.FORCE_RUN === "true" ? 80 : 0
  });

  let context;
  let page;
  const rowsToAppend = [];
  const seenThisRun = new Set();

  try {
    context = await browser.newContext({
      viewport: { width: 1440, height: 2200 },
      userAgent:
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    });
    page = await context.newPage();

    for (const countyUrl of URLS) {
      console.log(`\n[MAIN] Processing county URL: ${countyUrl}`);

      let propertyLinks = [];
      try {
        propertyLinks = await scrapeCountyPage(page, countyUrl);
      } catch (err) {
        console.error(`[COUNTY] Failed on ${countyUrl}: ${err.message}`);
        continue;
      }

      console.log(`[MAIN] ${propertyLinks.length} property link(s) returned for ${countyUrl}`);

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
    try {
      if (page) await page.close();
      if (context) await context.close();
      await browser.close();
    } catch (closeErr) {
      console.warn("Error closing browser resources:", closeErr.message);
    }
  }

  await appendRowsToSheet(sheets, rowsToAppend);
  console.log("Done.");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});