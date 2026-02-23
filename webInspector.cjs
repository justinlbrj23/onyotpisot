const puppeteer = require('puppeteer');
const cheerio = require('cheerio');
const fs = require('fs');
const { google } = require('googleapis');

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = './service-account.json';
const SPREADSHEET_ID = '1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA';
const SHEET_NAME = 'web_tda';
const URL_RANGE = 'C2:C';

const OUTPUT_ELEMENTS_FILE = 'raw-elements.json';
const OUTPUT_ROWS_FILE = 'parsed-auctions.json';
const OUTPUT_ERRORS_FILE = 'errors.json';
const OUTPUT_SUMMARY_FILE = 'summary.json';

// Your ONLY surplus formula:
const MIN_SURPLUS = 25000;
const MAX_PAGES = 50;

// =========================
// Utilities
// =========================
function clean(text) {
  return text ? text.replace(/\s+/g, ' ').trim() : '';
}

function parseCurrency(str) {
  if (!str) return null;
  const n = parseFloat(str.replace(/[^0-9.-]/g, ''));
  return isNaN(n) ? null : n;
}

// Normalize ampersands
function normalizeAmpersands(u) {
  return u
    .replace(/&amp;amp;/gi, '&amp;')
    .replace(/&amp;/gi, '&')
    .replace(/%26amp%3B/gi, '&');
}

// =========================
// Google Sheets Auth
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
});
const sheets = google.sheets({ version: 'v4', auth });

// =========================
// Load URLs from sheet
// =========================
async function loadTargetUrls() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!${URL_RANGE}`,
  });

  return (res.data.values || [])
    .flat()
    .map(v => (v || '').trim())
    .filter(v => v.startsWith('http'))
    .map(normalizeAmpersands);
}

/* =========================
   PAGE SCOPES & PAGINATION
   ========================= */

async function getPageScopes(page) {
  // Ensure main auction container exists
  await page.waitForSelector('#BID_WINDOW_CONTAINER', { timeout: 60000 });
  await page.waitForSelector('#BID_WINDOW_CONTAINER div[aid]', { timeout: 60000 });

  return {
    // HTML snapshot of first auction block
    async firstRowHtml() {
      return await page.$eval('#BID_WINDOW_CONTAINER', (root) => {
        const first = root.querySelector('div[aid]');
        return first ? first.innerHTML : '__NONE__';
      });
    },

    // Detect if list changed (pagination worked)
    async waitForListChange(timeoutMs = 25000) {
      const start = Date.now();
      const prior = await this.firstRowHtml();
      while (Date.now() - start < timeoutMs) {
        const now = await this.firstRowHtml().catch(() => null);
        if (now && now !== prior) return true;
        await new Promise(r => setTimeout(r, 400));
      }
      return false;
    },

    // Locate pagination controls inside the BID_WINDOW_CONTAINER
    async getPagerPieces() {
      const bar = await page.$('#BID_WINDOW_CONTAINER .Head_C > div:nth-of-type(3)');
      if (!bar) return { bar: null, input: null, text: null, next: null };

      const input = await bar.$("input[type='text'], input[type='number'], input:not([type])");
      const text = await bar.$('span.PageText');

      let next = await bar.$('span.PageRight > img');
      if (!next) {
        next =
          await bar.$('.PageRight_HVR > img') ||
          await bar.$('.PageRight img') ||
          await bar.$('img[alt*="next" i], img[title*="next" i]');
      }

      return { bar, input, text, next };
    },

    // Extract page number indicator
    async readIndicator(pieces) {
      if (!pieces || !pieces.bar) {
        return { current: null, total: null, raw: '' };
      }

      const [current, total, raw] = await page.evaluate((bar) => {
        const inp = bar.querySelector('input');
        const txt = bar.querySelector('span.PageText');

        let cur = null;
        let tot = null;

        if (inp && /^\d+$/.test((inp.value || '').trim())) {
          cur = Number((inp.value || '').trim());
        }

        const rawText = txt ? (txt.innerText || txt.textContent || '') : '';
        const norm = rawText.replace(/\s+/g, ' ').trim();

        const m = norm.match(/\bof\s*([0-9]+)/i);
        if (m) tot = Number(m[1]);

        return [cur, tot, norm];
      }, pieces.bar);

      return { current, total, raw };
    },

    // Method 1: enter page number manually
    async setPageInputAndGo(pieces, nextIndex) {
      if (!pieces || (!pieces.input && !pieces.bar)) return false;

      let acted = false;

      if (pieces.input) {
        try {
          await pieces.input.focus();
          await page.evaluate((el, val) => {
            el.value = String(val);
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('change', { bubbles: true }));
          }, pieces.input, nextIndex);
          acted = true;
        } catch {}
      }

      if (acted) {
        // Attempt to click any element that looks like a "Go"
        const go = await pieces.bar.$('button, input[type="submit"], input[type="button"], a, span');
        if (go) {
          const looksGo = await page.evaluate(el => {
            const text = (el.innerText || el.textContent || el.value || '').toLowerCase();
            const cls = (el.getAttribute('class') || '').toLowerCase();
            return text === 'go' || text.includes('go') || cls.includes('go');
          }, go);

          if (looksGo) {
            try {
              await page.evaluate(el => {
                const clickable = el.closest('a, button, input[type="submit"], input[type="button"]');
                (clickable || el).click();
              }, go);
              return true;
            } catch {}
          }
        }
      }

      // Fallback: press Enter
      if (acted) {
        try {
          await page.keyboard.press('Enter');
          return true;
        } catch {}
      }

      return false;
    },

    // Method 2: use the Right Arrow
    async clickNextArrow(pieces) {
      if (!pieces || !pieces.next) return false;

      try {
        await page.evaluate(el => {
          el.scrollIntoView({ block: 'center', behavior: 'instant' });
        }, pieces.next);
      } catch {}

      try {
        await page.evaluate(el => {
          const clickable = el.closest('a, button, input[type="button"]');
          (clickable || el).click();
        }, pieces.next);
        return true;
      } catch {
        try {
          await pieces.next.click({ delay: 20 });
          return true;
        } catch {
          return false;
        }
      }
    },
  };
}

/* =========================
   PARSER — SOLD ONLY
   (Single Surplus Formula)
   ========================= */

function parseAuctionsFromHtml(html, pageUrl) {
  const $ = cheerio.load(html);
  const rows = [];
  const relevant = [];

  $('#BID_WINDOW_CONTAINER div[aid]').each((_, item) => {
    const $item = $(item);

    // record relevant block for debugging
    const blockText = clean($item.text());
    if (blockText) {
      relevant.push({
        sourceUrl: pageUrl,
        tag: 'div',
        attrs: $item.attr() || {},
        text: blockText.slice(0, 2000),
      });
    }

    // Extract fields
    const caseNumber = getByThLabel($, $item, 'Cause Number');
    const openingBid = getByThLabel($, $item, 'Est. Min. Bid');
    const parcelId = getByThLabel($, $item, 'Account Number');
    const streetAddress = getByThLabel($, $item, 'Property Address');

    const assessedValue = getByThLabel($, $item, 'Adjudged Value'); 
    // (kept for record only, no longer used in surplus)

    // City/State/Zip
    let cityStateZip =
      clean($item.find('tr:nth-of-type(8) td').first().text()) ||
      getByThLabel($, $item, 'City, State Zip') ||
      getByThLabel($, $item, 'City/State/Zip') ||
      '';

    const status = clean($item.find('div.ASTAT_MSGA').first().text());
    const soldAmount = clean($item.find('div.ASTAT_MSGD').first().text());

    // Determine sold status
    const looksSold =
      status.toLowerCase().includes('sold') ||
      (soldAmount && parseCurrency(soldAmount) !== null);

    if (!looksSold) return;

    // Convert to numbers
    const salePriceNum = parseCurrency(soldAmount);
    const openingBidNum = parseCurrency(openingBid);

    // Main row object
    const row = {
      sourceUrl: pageUrl,
      auctionStatus: 'Sold',
      auctionType: 'Tax Sale',
      caseNumber: clean(caseNumber),
      parcelId: clean(parcelId),
      propertyAddress: clean(streetAddress),
      openingBid: clean(openingBid),
      salePrice: clean(soldAmount),
      assessedValue: clean(assessedValue),
      auctionDate: '',
      cityStateZip: clean(cityStateZip),
      status: clean(status),
    };

    // Validate essential fields
    const valid =
      row.caseNumber &&
      row.parcelId &&
      row.openingBid &&
      row.salePrice;

    if (!valid) return;

    // ============================
    // NEW SINGLE SURPLUS FORMULA
    // Surplus = Sale Price – Opening Bid
    // ============================
    if (salePriceNum !== null && openingBidNum !== null) {
      row.surplus = salePriceNum - openingBidNum;
    } else {
      row.surplus = null;
    }

    // Flag if surplus meets threshold
    row.meetsMinimumSurplus =
      row.surplus !== null && row.surplus >= MIN_SURPLUS ? 'Yes' : 'No';

    rows.push(row);
  });

  return { rows, relevant };
}

/* =========================
   INSPECT + PARSE PAGE
   (Paginated SOLD-only scraping)
   ========================= */

async function inspectAndParse(browser, url) {
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  const allRelevantElements = [];
  const allParsedRows = [];
  const seen = new Set();

  try {
    // Anti-bot headers
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
      'AppleWebKit/537.36 (KHTML, like Gecko) ' +
      'Chrome/121.0.0.0 Safari/537.36'
    );
    await page.setExtraHTTPHeaders({
      'Accept-Language': 'en-US,en;q=0.9',
      Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    });
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, 'webdriver', { get: () => false });
    });

    const normalizedUrl = normalizeAmpersands(url);
    await page.goto(normalizedUrl, { waitUntil: 'networkidle2', timeout: 120000 });

    // Utilities scoped to BID_WINDOW_CONTAINER
    const scope = await getPageScopes(page);

    let pagesVisited = 0;

    while (pagesVisited < MAX_PAGES) {
      // Parse current page’s content
      const html = await page.content();
      const { rows, relevant } = parseAuctionsFromHtml(html, normalizedUrl);

      // Dedupe
      for (const row of rows) {
        const key = `${row.sourceUrl}|${row.caseNumber}|${row.parcelId}`;
        if (!seen.has(key)) {
          seen.add(key);
          allParsedRows.push(row);
        }
      }

      allRelevantElements.push(...relevant);
      console.log(`   ➜ SOLD rows so far: ${allParsedRows.length}`);

      // Read pagination controls
      const pieces = await scope.getPagerPieces();
      if (!pieces.bar) {
        console.log('🛑 Pager bar not found — stopping.');
        break;
      }

      const indicator = await scope.readIndicator(pieces);
      const current = indicator.current || (pagesVisited + 1);
      const total = indicator.total || null;

      if (total && current >= total) {
        console.log(`🛑 Reached last page (${current}/${total}).`);
        break;
      }

      const nextIndex = current + 1;

      // Method 1: set page number manually
      const acted = await scope.setPageInputAndGo(pieces, nextIndex);
      if (acted) {
        const changed = await scope.waitForListChange(30000);
        if (changed) {
          pagesVisited++;
          continue;
        }
      }

      // Method 2: click "next" arrow
      console.log('➡️ Clicking pager right arrow...');
      const clicked = await scope.clickNextArrow(pieces);
      if (clicked) {
        const changed = await scope.waitForListChange(30000);
        if (changed) {
          pagesVisited++;
          continue;
        }
      }

      console.log('🛑 No list change detected — stopping.');
      break;
    }

    return {
      relevantElements: allRelevantElements,
      parsedRows: allParsedRows,
    };

  } catch (err) {
    console.error(`❌ Error on ${url}:`, err.message);
    return {
      relevantElements: [],
      parsedRows: [],
      error: { url, message: err.message },
    };
  } finally {
    await page.close();
  }
}

/* =========================
   MAIN RUNNER
   ========================= */

(async () => {
  console.log('📥 Loading URLs from Google Sheets...');
  const urls = await loadTargetUrls();
  console.log(`🔗 Loaded ${urls.length} URL(s).`);

  const browser = await puppeteer.launch({
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-blink-features=AutomationControlled',
      '--ignore-certificate-errors',
      '--disable-gpu',
      '--single-process',
      '--no-zygote',
    ],
  });

  const allElements = [];
  const allRows = [];
  const errors = [];

  for (const url of urls) {
    try {
      console.log(`\n🌐 Processing → ${url}`);
      const { relevantElements, parsedRows, error } = await inspectAndParse(browser, url);

      allElements.push(...relevantElements);
      allRows.push(...parsedRows);

      if (error) errors.push(error);

    } catch (err) {
      console.error(`❌ Fatal error on ${url}:`, err.message);
      errors.push({ url, message: err.message });
    }
  }

  await browser.close();

  // ============================
  // GLOBAL DEDUPLICATION
  // ============================
  const unique = new Map();
  for (const row of allRows) {
    const key = `${row.sourceUrl}|${row.caseNumber}|${row.parcelId}`;
    if (!unique.has(key)) unique.set(key, row);
  }

  const finalRows = [...unique.values()];

  // ============================
  // SUMMARY FILE
  // ============================
  const summary = {
    totalUrls: urls.length,
    totalElements: allElements.length,
    totalRowsRaw: allRows.length,
    totalRowsFinal: finalRows.length,
    errorsCount: errors.length,
    surplusAboveThreshold: finalRows.filter(r => r.meetsMinimumSurplus === 'Yes').length,
    surplusBelowThreshold: finalRows.filter(r => r.meetsMinimumSurplus === 'No').length,
    blanks: {
      salePriceBlank: finalRows.filter(r => !r.salePrice).length,
      auctionDateBlank: finalRows.filter(r => !r.auctionDate).length,
    },
  };

  // ============================
  // WRITE OUTPUT ARTIFACTS
  // ============================
  fs.writeFileSync(OUTPUT_ELEMENTS_FILE, JSON.stringify(allElements, null, 2));
  fs.writeFileSync(OUTPUT_ROWS_FILE, JSON.stringify(finalRows, null, 2));
  fs.writeFileSync(OUTPUT_SUMMARY_FILE, JSON.stringify(summary, null, 2));

  if (errors.length) {
    fs.writeFileSync(OUTPUT_ERRORS_FILE, JSON.stringify(errors, null, 2));
    console.log(`⚠️ Saved ${errors.length} error(s) → ${OUTPUT_ERRORS_FILE}`);
  }

  console.log(`\n✅ Saved ${allElements.length} relevant elements → ${OUTPUT_ELEMENTS_FILE}`);
  console.log(`✅ Saved ${finalRows.length} SOLD auctions → ${OUTPUT_ROWS_FILE}`);
  console.log(`📊 Summary saved → ${OUTPUT_SUMMARY_FILE}`);
  console.log('🏁 Done.');
})();