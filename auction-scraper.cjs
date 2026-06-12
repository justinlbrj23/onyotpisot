// county-verify-on-detail-scraper.js (updated)
// Modifications:
// 1) Address stored in sheet trimmed up through ZIP code.
// 2) Dates formatted as M/D/YYYY and written to column L.
// 3) Detail page URL written to column I.
// 4) Append writes columns E through L.

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

// Trim address to include up through the 5-digit (or 5+4) ZIP code if present.
// If no zip found, return the original address trimmed.
function trimAddressToZip(address) {
  if (!address) return "";
  const txt = address.trim();
  // match up to and including 5-digit or 5+4 zip (e.g., 12345 or 12345-6789)
  const m = txt.match(/^(.*?\b\d{5}(?:-\d{4})?)/);
  if (m && m[1]) return m[1].trim();
  // fallback: try to stop at state abbreviation (e.g., ", MO" or ", KS")
  const m2 = txt.match(/^(.*?\b[A-Z]{2}\b)/);
  return (m2 && m2[1]) ? m2[1].trim() : txt;
}

// Parse a saleDate string and return M/D/YYYY (no leading zeros).
// Accepts strings like "Thursday, Jun 18, 2026 – Add to calendar" or "Jun 18, 2026".
function parseSaleDateToMDY(raw) {
  if (!raw) return "";
  // Remove common trailing text and dashes
  let s = raw.replace(/\u2013|\u2014|–|—/g, " ").replace(/–.*$/,"").replace(/—.*$/,"").trim();
  // Remove "Add to calendar" or similar trailing phrases
  s = s.replace(/\badd to calendar\b/i, "").replace(/\b–.*$/,"").trim();
  // Try to find MonthName Day, Year
  const m = s.match(/([A-Za-z]+)\s+0?(\d{1,2}),?\s*(\d{4})/);
  if (m) {
    const monthName = m[1].toLowerCase();
    const day = Number(m[2]);
    const year = Number(m[3]);
    const months = {
      jan:1, january:1, feb:2, february:2, mar:3, march:3, apr:4, april:4,
      may:5, jun:6, june:6, jul:7, july:7, aug:8, august:8, sep:9, sept:9, september:9,
      oct:10, october:10, nov:11, november:11, dec:12, december:12
    };
    const key = monthName.slice(0,3);
    const mnum = months[key] || months[monthName] || NaN;
    if (!isNaN(mnum)) return `${mnum}/${day}/${year}`;
  }
  // Try numeric formats or ISO parse
  const iso = Date.parse(s);
  if (!isNaN(iso)) {
    const d = new Date(iso);
    return `${d.getMonth()+1}/${d.getDate()}/${d.getFullYear()}`;
  }
  // Try to extract numeric date patterns like 06/18/2026 or 6/18/2026
  const m2 = s.match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/);
  if (m2) {
    const mm = Number(m2[1]);
    const dd = Number(m2[2]);
    let yyyy = Number(m2[3]);
    if (yyyy < 100) yyyy += 2000;
    return `${mm}/${dd}/${yyyy}`;
  }
  return "";
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
    // Trim existing sheet addresses to zip for consistent dedupe
    const trimmed = trimAddressToZip(val);
    const norm = normalizeText(trimmed);
    if (norm) {
      lastUsedRow = rowNum;
      if (rowNum >= DATA_START_ROW) existing.add(norm);
    }
  }
  const nextRow = Math.max(lastUsedRow + 1, DATA_START_ROW);
  return { existing, nextRow };
}

// Append rows into columns E through L (8 columns)
async function appendRowsToSheet(sheets, rowsToAppend) {
  if (!rowsToAppend.length) {
    console.log("No new rows to append.");
    return;
  }

  // Ensure each row has exactly 8 columns (E..L)
  const normalizedRows = rowsToAppend.map((r) => {
    const copy = Array.from(r);
    while (copy.length < 8) copy.push("");
    return copy.slice(0, 8);
  });

  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!E:L`,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values: normalizedRows }
  });

  console.log(`Appended ${normalizedRows.length} row(s) to ${SHEET_NAME}!E:L`);
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
 * Scrape county page and return ALL candidate property links (no county filtering here).
 * Filtering is done later on the detail page.
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
    if (linksA && linksA.length) {
      console.log(`[COUNTY] Strategy A found ${linksA.length} link(s).`);
      return linksA;
    } else {
      console.log(`[COUNTY] Strategy A found 0 links.`);
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
    if (linksB && linksB.length) {
      console.log(`[COUNTY] Strategy B found ${linksB.length} link(s).`);
      return linksB;
    } else {
      console.log(`[COUNTY] Strategy B found 0 links.`);
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
    if (linksC && linksC.length) {
      console.log(`[COUNTY] Strategy C found ${linksC.length} link(s).`);
      return linksC;
    } else {
      console.log(`[COUNTY] Strategy C found 0 links.`);
    }
  } catch (err) {
    console.warn(`[COUNTY] Strategy C error: ${err.message}`);
  }

  // Save debug HTML for inspection
  try {
    const html = await page.content();
    const debugPath = path.join(process.cwd(), "debug-county.html");
    fs.writeFileSync(debugPath, html, "utf8");
    console.warn(`[COUNTY] No property links found. Saved debug HTML to ${debugPath}`);
  } catch (err) {
    console.warn("[COUNTY] Failed to save debug HTML:", err.message);
  }

  return [];
}

/**
 * Scrape detail page and return address, saleDate, and the page content for verification.
 */
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
  // also capture some page text for verification (small snippet)
  let snippet = "";
  try {
    snippet = await page.locator("body").innerText({ timeout: 5000 });
    snippet = snippet ? snippet.slice(0, 2000).toLowerCase() : "";
  } catch {
    snippet = "";
  }
  console.log(`[DETAIL] Address: ${address}`);
  console.log(`[DETAIL] Sale Date: ${saleDate}`);
  return { address, saleDate, detailUrl, snippet };
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

      // extract county slug/short for verification on detail page
      const { slug: countySlug, short: countyShort } = extractCountySlug(countyUrl);
      const countySlugLower = countySlug ? countySlug.toLowerCase() : null;
      const countyShortLower = countyShort ? countyShort.toLowerCase() : null;

      let propertyLinks = [];
      try {
        propertyLinks = await scrapeCountyPage(page, countyUrl);
      } catch (err) {
        console.error(`[COUNTY] Failed on ${countyUrl}: ${err.message}`);
        continue;
      }

      console.log(`[MAIN] ${propertyLinks.length} candidate property link(s) returned for ${countyUrl}`);

      for (const detailUrl of propertyLinks) {
        try {
          const { address, saleDate, snippet } = await scrapePropertyDetail(page, detailUrl);

          // Trim address to zipcode for storage and dedupe
          const trimmedAddress = trimAddressToZip(address);
          const normalizedAddress = normalizeText(trimmedAddress);

          // Verify the detail page belongs to the county by checking:
          // 1) address or H1 contains county short or slug
          // 2) or the page snippet contains the county short or slug
          // 3) or the detailUrl contains the county short or slug (fallback)
          let belongsToCounty = false;
          if (countySlugLower && (normalizedAddress.includes(countySlugLower) || snippet.includes(countySlugLower) || detailUrl.toLowerCase().includes(countySlugLower))) {
            belongsToCounty = true;
          }
          if (!belongsToCounty && countyShortLower && (normalizedAddress.includes(countyShortLower) || snippet.includes(countyShortLower) || detailUrl.toLowerCase().includes(countyShortLower))) {
            belongsToCounty = true;
          }

          if (!belongsToCounty) {
            console.log(`[VERIFY] Detail page does not appear to belong to ${countySlug || countyShort || "this county"}. Skipping: ${detailUrl}`);
            continue;
          }

          if (!normalizedAddress) {
            console.log(`[DETAIL] Empty address after trimming. Skipping.`);
            continue;
          }

          if (existing.has(normalizedAddress)) {
            console.log(`[SHEET] Already exists in column E. Skipping: ${trimmedAddress}`);
            continue;
          }

          if (seenThisRun.has(normalizedAddress)) {
            console.log(`[SHEET] Duplicate in current run. Skipping: ${trimmedAddress}`);
            continue;
          }

          // Format date to M/D/YYYY and prepare row with placeholders for columns F,G,H,J,K
          const formattedDate = parseSaleDateToMDY(saleDate);

          // Build row for columns E through L (E,F,G,H,I,J,K,L)
          // Populate E (address), I (detailUrl), and L (date). Other columns left empty.
          const row = [
            trimmedAddress, // E
            "",             // F
            "",             // G
            "",             // H
            detailUrl,      // I
            "",             // J
            "",             // K
            formattedDate   // L
          ];

          rowsToAppend.push(row);
          seenThisRun.add(normalizedAddress);

          console.log(`[SHEET] Queued => ${trimmedAddress} | ${formattedDate} | ${detailUrl}`);
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