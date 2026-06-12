// county-verify-on-detail-scraper.js (final update)
// - Only accepts properties in KS or MO (state whitelist).
// - Verifies county as before, but rejects pages outside KS/MO.
// - Writes trimmed address (through ZIP) to column E, detail URL to I, date (M/D/YYYY) to L.
// - Writes exactly to columns E:L using explicit-range update with verification and retries.

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

// Accept only these state abbreviations for target properties
const STATE_WHITELIST = new Set(["ks", "mo"]);

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
// Normalize newlines and commas so state extraction works reliably.
function trimAddressToZip(address) {
  if (!address) return "";
  // Normalize whitespace and newlines into single spaces
  let txt = address.replace(/\r?\n/g, " ").replace(/\s+/g, " ").trim();
  // Try to match up through ZIP (5 or 5-4)
  const m = txt.match(/^(.*?\b[A-Za-z]{2}\b[\s,]*\d{5}(?:-\d{4})?)/);
  if (m && m[1]) return m[1].trim();
  // Fallback: match up through ZIP without requiring state
  const m2 = txt.match(/^(.*?\b\d{5}(?:-\d{4})?)/);
  if (m2 && m2[1]) return m2[1].trim();
  // Final fallback: return normalized single-line address
  return txt;
}

// Parse a saleDate string and return M/D/YYYY (no leading zeros).
function parseSaleDateToMDY(raw) {
  if (!raw) return "";
  let s = raw.replace(/\u2013|\u2014|–|—/g, " ").replace(/–.*$/,"").replace(/—.*$/,"").trim();
  s = s.replace(/\badd to calendar\b/i, "").replace(/\b–.*$/,"").trim();
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
  const iso = Date.parse(s);
  if (!isNaN(iso)) {
    const d = new Date(iso);
    return `${d.getMonth()+1}/${d.getDate()}/${d.getFullYear()}`;
  }
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

// Build a row mapped to columns E..L (8 columns)
function buildRowForEL({ address, detailUrl, formattedDate }) {
  return [
    address || "",     // E
    "",                // F
    "",                // G
    "",                // H
    detailUrl || "",   // I
    "",                // J
    "",                // K
    formattedDate || ""// L
  ];
}

// Ensure each row has exactly width columns
function padRows(rows, width = 8) {
  return rows.map((r) => {
    const copy = Array.from(r);
    while (copy.length < width) copy.push("");
    return copy.slice(0, width);
  });
}

// Read back and verify written values match expected payload
async function verifyWrite(sheets, startRow, endRow, expectedRows) {
  const range = `${SHEET_NAME}!E${startRow}:L${endRow}`;
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range });
  const actual = res.data.values || [];
  for (let i = 0; i < expectedRows.length; i++) {
    const exp = expectedRows[i];
    const act = actual[i] || [];
    for (let c = 0; c < exp.length; c++) {
      if ((act[c] || "") !== (exp[c] || "")) {
        throw new Error(`Verification failed at row ${startRow + i} col ${String.fromCharCode(69 + c)}: expected "${exp[c]}", got "${act[c] || ''}"`);
      }
    }
  }
  return true;
}

// Write exact range E{start}:L{end} with retries and verification
async function writeExactRangeWithRetries(sheets, startRow, rows, maxRetries = 3) {
  const normalized = padRows(rows, 8);
  const endRow = startRow + normalized.length - 1;
  const range = `${SHEET_NAME}!E${startRow}:L${endRow}`;

  let attempt = 0;
  let lastErr = null;

  while (attempt < maxRetries) {
    attempt++;
    try {
      console.log(`[SHEETS] Writing ${normalized.length} row(s) to ${range} (attempt ${attempt})`);
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range,
        valueInputOption: "RAW",
        requestBody: { values: normalized }
      });

      // Read back and verify
      await verifyWrite(sheets, startRow, endRow, normalized);
      console.log(`[SHEETS] Verification succeeded for ${range}`);
      return { range, startRow, endRow };
    } catch (err) {
      lastErr = err;
      console.warn(`[SHEETS] Write attempt ${attempt} failed: ${err.message}`);
      const backoffMs = 200 * Math.pow(3, attempt - 1);
      await new Promise((res) => setTimeout(res, backoffMs));
    }
  }

  throw new Error(`Failed to write and verify range ${range} after ${maxRetries} attempts. Last error: ${lastErr && lastErr.message}`);
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

async function scrapeCountyPage(page, url) {
  console.log(`\n[COUNTY] Visiting: ${url}`);
  await safeGoto(page, url);

  try { await page.waitForSelector(SELECTORS.pageH1, { timeout: 20000 }); } catch {}
  const h1Text = await getTextOrEmpty(page.locator(SELECTORS.pageH1));
  console.log(`[COUNTY] h1 = ${h1Text}`);

  // If H1 contains "near" skip (preserve existing behavior)
  if (normalizeText(h1Text).includes("near")) {
    console.log(`[COUNTY] "near" detected in h1. Skipping this county page.`);
    return [];
  }

  await tryClickLoadMore(page);

  // Stabilize visible card count by scrolling and clicking load more
  const maxAttempts = 12;
  let lastCount = 0;
  for (let i = 0; i < maxAttempts; i++) {
    await page.evaluate(() => window.scrollBy(0, window.innerHeight));
    await page.waitForTimeout(800);
    await tryClickLoadMore(page);
    const count = await page.evaluate(() => {
      const byAsset = document.querySelectorAll('div.b__asset-root--VmozO').length;
      const byRow = document.querySelectorAll('div.asset-list-row').length;
      return Math.max(byAsset, byRow);
    });
    console.log(`[COUNTY] visible card count attempt ${i+1}: ${count}`);
    if (count === lastCount && count > 0) break;
    lastCount = count;
  }

  // Extract candidate links (anchors, data-* and onclick fallbacks)
  const rawLinks = await page.evaluate(() => {
    const out = new Set();
    const cardSelectors = ['div.b__asset-root--VmozO', 'div.asset-list-row', '.asset-card', '.asset'];
    for (const sel of cardSelectors) {
      document.querySelectorAll(sel).forEach((card) => {
        const a = card.querySelector('a[href]');
        if (a && a.href) out.add(a.href);
      });
    }
    document.querySelectorAll('a[href]').forEach((a) => {
      const href = a.href || '';
      const low = href.toLowerCase();
      if (low.includes('/property/') || low.includes('/asset/') || low.includes('/details/') || low.includes('/residential/')) {
        out.add(href);
      }
    });
    document.querySelectorAll('[data-href],[data-url],[data-link]').forEach((el) => {
      const href = el.getAttribute('data-href') || el.getAttribute('data-url') || el.getAttribute('data-link');
      if (href) {
        try { out.add(new URL(href, location.origin).href); } catch { out.add(href); }
      }
    });
    document.querySelectorAll('[onclick]').forEach((el) => {
      const onclick = el.getAttribute('onclick') || '';
      const m = onclick.match(/location\.href\s*=\s*['"]([^'"]+)['"]/i);
      if (m && m[1]) {
        try { out.add(new URL(m[1], location.origin).href); } catch { out.add(m[1]); }
      }
    });
    return Array.from(out);
  });

  console.log(`[COUNTY] Extracted ${rawLinks.length} candidate link(s) (pre-filter).`);

  // Filter to only /details links (case-insensitive)
  const detailsLinks = Array.from(new Set(rawLinks.filter(h => h && h.toLowerCase().includes('/details'))));

  console.log(`[COUNTY] Filtered to ${detailsLinks.length} /details link(s).`);

  // If H1 contains a numeric count (e.g., "17 Properties in ..."), limit to that count
  let expectedCount = 0;
  try {
    const m = h1Text.match(/(\d{1,3})\s+Properties?/i);
    if (m && m[1]) expectedCount = Number(m[1]);
  } catch (e) {
    expectedCount = 0;
  }

  if (expectedCount > 0) {
    // If we have more detail links than expected, keep the first expectedCount unique links
    if (detailsLinks.length > expectedCount) {
      console.log(`[COUNTY] H1 indicates ${expectedCount} properties; trimming ${detailsLinks.length} -> ${expectedCount}`);
      return detailsLinks.slice(0, expectedCount);
    }
  }

  // If no /details links found but anchors exist, fall back to anchors (rare)
  if (detailsLinks.length === 0 && rawLinks.length > 0) {
    console.log(`[COUNTY] No /details links found; returning ${rawLinks.length} raw anchor(s) as fallback.`);
    return Array.from(new Set(rawLinks));
  }

  return detailsLinks;
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
  const { existing, nextRow } = await getExistingAddressesAndNextRow(sheets);
  console.log(`Existing addresses in column E: ${existing.size}`);
  console.log(`Computed nextRow (before scraping): ${nextRow}`);

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

          // --- Begin robust state + county verification ---

          // Normalize for checks (single-line address)
          const singleLineAddr = (trimmedAddress || "").replace(/\r?\n/g, " ").replace(/\s+/g, " ").trim();
          const addrForCheck = singleLineAddr.toLowerCase();
          const urlLower = (detailUrl || "").toLowerCase();
          const snippetLower = (snippet || "").toLowerCase();

          // 1) Extract state as the two-letter token immediately before the 5-digit ZIP
          let stateFound = null;
          const stateMatch = singleLineAddr.match(/,\s*([A-Za-z]{2})\s+\d{5}(?:-\d{4})?$/);
          if (stateMatch && stateMatch[1]) {
            stateFound = stateMatch[1].toLowerCase();
            console.log(`[VERIFY] Detected state from address-before-zip: ${stateFound}`);
          }

          // 2) Fallbacks if direct state-before-zip extraction fails
          if (!stateFound) {
            // look for " KS " or " MO " tokens anywhere in the address (or full names)
            const tokenMatch = addrForCheck.match(/\b(ks|mo|kansas|missouri)\b/);
            if (tokenMatch && tokenMatch[1]) {
              const t = tokenMatch[1];
              if (t === "ks" || t === "kansas") stateFound = "ks";
              if (t === "mo" || t === "missouri") stateFound = "mo";
              console.log(`[VERIFY] Detected state from address token fallback: ${stateFound}`);
            }
          }
          if (!stateFound) {
            // check snippet or URL for state hints
            if (snippetLower.match(/\b(kansas|ks)\b/) || urlLower.includes("/ks/") || urlLower.includes("/kansas/")) {
              stateFound = "ks";
              console.log(`[VERIFY] Detected state from snippet/URL: ks`);
            }
            if (snippetLower.match(/\b(missouri|mo)\b/) || urlLower.includes("/mo/") || urlLower.includes("/missouri/")) {
              stateFound = "mo";
              console.log(`[VERIFY] Detected state from snippet/URL: mo`);
            }
          }

          // If state not in whitelist, skip
          if (!stateFound || !STATE_WHITELIST.has(stateFound)) {
            console.log(`[VERIFY] Skipping because state not in whitelist (found: ${stateFound || "none"}). URL: ${detailUrl}`);
            continue;
          }

          // 3) County verification (existing logic)
          let belongsToCounty = false;
          if (countySlugLower && (addrForCheck.includes(countySlugLower) || snippetLower.includes(countySlugLower) || urlLower.includes(countySlugLower))) {
            belongsToCounty = true;
          }
          if (!belongsToCounty && countyShortLower && (addrForCheck.includes(countyShortLower) || snippetLower.includes(countyShortLower) || urlLower.includes(countyShortLower))) {
            belongsToCounty = true;
          }

          // Enforce county match as additional guard; comment out the next block if you want to accept any KS/MO property
          if (!belongsToCounty) {
            console.log(`[VERIFY] Detail page does not appear to belong to ${countySlug || countyShort || "this county"}. Skipping: ${detailUrl}`);
            continue;
          }

          // --- End robust state + county verification ---

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

          const formattedDate = parseSaleDateToMDY(saleDate);

          const row = buildRowForEL({
            address: trimmedAddress,
            detailUrl,
            formattedDate
          });

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

  if (!rowsToAppend.length) {
    console.log("No new rows to append.");
    return;
  }

  // Recompute nextRow immediately before writing to minimize race window
  const { existing: existingAfter, nextRow: startRow } = await getExistingAddressesAndNextRow(sheets);
  console.log(`Starting write at row ${startRow} (recomputed before write).`);

  try {
    const result = await writeExactRangeWithRetries(sheets, startRow, rowsToAppend, 3);
    console.log(`[SHEETS] Write completed: ${JSON.stringify(result)}`);
  } catch (err) {
    console.error(`[SHEETS] Failed to write rows: ${err.message}`);
    try {
      const dumpPath = path.join(process.cwd(), `failed-write-payload-${Date.now()}.json`);
      fs.writeFileSync(dumpPath, JSON.stringify(rowsToAppend, null, 2), "utf8");
      console.error(`[SHEETS] Saved failed payload to ${dumpPath}`);
    } catch (saveErr) {
      console.error(`[SHEETS] Failed to save payload: ${saveErr.message}`);
    }
    throw err;
  }

  console.log("Done.");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});