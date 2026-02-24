// webInspector.cjs (page intelligence + auction parser for SOLD cards only)
// Requires:
// npm install puppeteer cheerio googleapis

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

const MIN_SURPLUS = 25000;
const MAX_PAGES = 50; // safety stop per URL

// =========================
// Sleep helper (cross-puppeteer)
// =========================
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// =========================
// GOOGLE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
});
const sheets = google.sheets({ version: 'v4', auth });

// =========================
// Load URLs
// =========================
function normalizeAmpersands(u) {
  // Normalize & encodings commonly seen coming from Sheets
  return u
    .replace(/&amp;amp;/gi, '&amp;')
    .replace(/&amp;/gi, '&')
    .replace(/%26amp%3B/gi, '&');
}

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

// =========================
// Utility helpers
// =========================
function clean(text) {
  if (!text) return '';
  return text.replace(/\s+/g, ' ').trim();
}

function parseCurrency(str) {
  if (!str) return null;
  const n = parseFloat(str.replace(/[^0-9.-]/g, ''));
  return isNaN(n) ? null : n;
}

/**
 * Extract the TD text that follows a TH containing a given label (case-insensitive).
 */
function getByThLabel($, $ctx, label) {
  let value = '';
  const target = clean(label).toLowerCase().replace(/:\s*$/, '');
  $ctx.find('th').each((_, el) => {
    const thText = clean($(el).text()).toLowerCase().replace(/:\s*$/, '');
    if (thText.includes(target)) {
      const td = $(el).next('td');
      if (td && td.length) value = clean(td.text());
      return false; // break
    }
  });
  return value;
}

/**
 * Wait for the list to exist and return an object with helpers scoped to the
 * RealForeclose body container (#BID_WINDOW_CONTAINER).
 */
async function getPageScopes(page) {
  // Ensure the main auction container exists
  await page.waitForSelector('#BID_WINDOW_CONTAINER', { timeout: 60000 });

  // Ensure at least one auction block exists
  await page.waitForSelector('#BID_WINDOW_CONTAINER div[aid]', { timeout: 60000 });

  // Return helpers that operate strictly inside BID_WINDOW_CONTAINER
  return {
    async firstRowHtml() {
      return await page.$eval('#BID_WINDOW_CONTAINER', (root) => {
        const first = root.querySelector('div[aid]');
        return first ? first.innerHTML : '__NONE__';
      });
    },
    async waitForListChange(timeoutMs = 25000) {
      const start = Date.now();
      const prior = await this.firstRowHtml();
      while (Date.now() - start < timeoutMs) {
        try {
          const now = await this.firstRowHtml();
          if (now !== prior) return true;
        } catch {}
        await new Promise(r => setTimeout(r, 400));
      }
      return false;
    },
    /**
     * Return handles to the pager bar and elements:
     * - bar:   #BID_WINDOW_CONTAINER .Head_C > div:nth-of-type(3)
     * - input: first text/number input inside the bar
     * - text:  span.PageText (for "of N")
     * - next:  span.PageRight > img  (your recording's target)
     */
    async getPagerPieces() {
      const bar = await page.$('#BID_WINDOW_CONTAINER .Head_C > div:nth-of-type(3)');
      if (!bar) return { bar: null, input: null, text: null, next: null };

      const input = await bar.$("input[type='text'], input[type='number'], input:not([type])");
      const text = await bar.$('span.PageText');
      // Prefer the exact selector you recorded
      let next = await bar.$('span.PageRight > img');
      if (!next) {
        // Fallbacks, just in case markup varies slightly
        next = await bar.$('.PageRight_HVR > img') || await bar.$('.PageRight img') || await bar.$('img[alt*="next" i], img[title*="next" i]');
      }

      return { bar, input, text, next };
    },
    /**
     * Parse current/total page from the pager:
     * - current: value from <input>
     * - total: from span.PageText (e.g., "of 5")
     */
    async readIndicator(pieces) {
      if (!pieces || !pieces.bar) return { current: null, total: null, raw: '' };
      const [current, total, raw] = await page.evaluate((bar) => {
        const inp = bar.querySelector('input');
        const txt = bar.querySelector('span.PageText');
        let cur = null;
        let tot = null;
        if (inp && /^\d+$/.test((inp.value || '').trim())) {
          cur = Number((inp.value || '').trim());
        }
        const rawText = (txt ? (txt.innerText || txt.textContent || '') : '').replace(/\s+/g, ' ').trim();
        // Expect formats like "page of 5" or just "of 5"
        const m = rawText.match(/\bof\s*([0-9]+)/i);
        if (m) tot = Number(m[1]);
        return [cur, tot, rawText];
      }, pieces.bar);
      return { current, total, raw };
    },
    async setPageInputAndGo(pieces, nextIndex) {
      if (!pieces || (!pieces.input && !pieces.bar)) return false;

      let acted = false;
      if (pieces.input) {
        try {
          await pieces.input.focus();
        } catch {}
        try {
          await page.evaluate((el, val) => {
            el.value = String(val);
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('change', { bubbles: true }));
          }, pieces.input, nextIndex);
          acted = true;
        } catch {}
      }

      // Try an explicit "Go" control if one exists in the bar
      if (acted) {
        const go = await pieces.bar.$('button, input[type="submit"], input[type="button"], a, span');
        if (go) {
          const looksGo = await page.evaluate((el) => {
            const t = ((el.innerText || el.textContent || el.value || el.getAttribute('title') || '') + '').toLowerCase().trim();
            const cls = (el.getAttribute('class') || '').toLowerCase();
            return t === 'go' || t.includes('go') || cls.includes('go');
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

      // Otherwise press Enter in the page (top-level keyboard events bubble)
      if (acted) {
        try { await page.keyboard.press('Enter'); return true; } catch {}
      }

      return false;
    },
    async clickNextArrow(pieces) {
      if (!pieces || !pieces.next) return false;
      try {
        await page.evaluate(el => {
          el.scrollIntoView({ block: 'center', behavior: 'instant' });
        }, pieces.next);
      } catch {}
      try {
        // Click the image or its nearest clickable ancestor
        await page.evaluate(el => {
          const clickable = el.closest('a, button, input[type="submit"], input[type="button"]');
          (clickable || el).click();
        }, pieces.next);
        return true;
      } catch {
        try { await pieces.next.click({ delay: 20 }); return true; } catch { return false; }
      }
    },
  };
}

// Extracts AUCTIONDATE=MM/DD/YYYY from the URL
function extractAuctionDateFromUrl(url) {
  try {
    const u = new URL(url);
    const d = u.searchParams.get("AUCTIONDATE");
    return d ? d.trim() : "";
  } catch {
    return "";
  }
}

// =========================
// Parser (SOLD-only) – sitemap-accurate mapping on div[aid]
// =========================
function parseAuctionsFromHtml(html, pageUrl) {
  const $ = cheerio.load(html);
  const rows = [];
  const relevant = [];

  $('#BID_WINDOW_CONTAINER div[aid]').each((_, item) => {
    const $item = $(item);

    // record relevant block for diagnostics
    const blockText = clean($item.text());
    if (blockText) {
      relevant.push({
        sourceUrl: pageUrl,
        tag: 'div',
        attrs: $item.attr() || {},
        text: blockText.slice(0, 2000),
      });
    }

    const caseNumber    = getByThLabel($, $item, 'Cause Number');
    const assessedValue = getByThLabel($, $item, 'Adjudged Value');
    const openingBid    = getByThLabel($, $item, 'Est. Min. Bid');
    const parcelId      = getByThLabel($, $item, 'Account Number');
    const streetAddress = getByThLabel($, $item, 'Property Address');

    // city/state/zip – template sometimes uses row order
    let cityStateZip = clean($item.find('tr:nth-of-type(8) td').first().text());
    if (!cityStateZip) {
      cityStateZip =
        getByThLabel($, $item, 'City, State Zip') ||
        getByThLabel($, $item, 'City/State/Zip') ||
        '';
    }

    const status     = clean($item.find('div.ASTAT_MSGA').first().text());
    const soldAmount = clean($item.find('div.ASTAT_MSGD').first().text());

    const looksSold =
      status.toLowerCase().includes('sold') ||
      (!!soldAmount && parseCurrency(soldAmount) !== null);
    if (!looksSold) return;

    const openingBidNum = parseCurrency(openingBid);
    const assessedNum   = parseCurrency(assessedValue);
    const salePriceNum  = parseCurrency(soldAmount);

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
      auctionDate: extractAuctionDateFromUrl(pageUrl),
      cityStateZip: clean(cityStateZip),
      status: clean(status),
    };

    const valid =
      row.caseNumber &&
      row.parcelId &&
      row.openingBid &&
      row.salePrice &&
      row.assessedValue;
    if (!valid) return;

    row.surplusAssessVsSale =
      assessedNum !== null && salePriceNum !== null ? assessedNum - salePriceNum : null;

    row.surplusSaleVsOpen =
      salePriceNum !== null && openingBidNum !== null ? salePriceNum - openingBidNum : null;

    row.meetsMinimumSurplus =
      row.surplusAssessVsSale !== null && row.surplusAssessVsSale >= MIN_SURPLUS ? 'Yes' : 'No';

    rows.push(row);
  });

  return { rows, relevant };
}

// =========================
// Inspect + Parse Page (SOLD only) – with pager clicking as per your recording
// =========================
async function inspectAndParse(browser, url) {
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  const allRelevantElements = [];
  const allParsedRows = [];
  const seen = new Set();

  try {
    // Anti-bot hardening
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
        'AppleWebKit/537.36 (KHTML, like Gecko) ' +
        'Chrome/121.0.0.0 Safari/537.36'
    );
    await page.setExtraHTTPHeaders({
      'Accept-Language': 'en-US,en;q=0.9',
      Accept:
        'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    });
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, 'webdriver', { get: () => false });
    });

    const normalizedUrl = normalizeAmpersands(url);
    await page.goto(normalizedUrl, { waitUntil: 'networkidle2', timeout: 120000 });

    // Get scoped helpers for the RealForeclose body container
    const scope = await getPageScopes(page);

    let pagesVisited = 0;
    while (pagesVisited < MAX_PAGES) {
      // Parse current page
      const html = await page.content(); // includes #BID_WINDOW_CONTAINER
      const { rows, relevant } = parseAuctionsFromHtml(html, normalizedUrl);

      for (const row of rows) {
        const key = `${row.sourceUrl}|${row.caseNumber}|${row.parcelId}`;
        if (!seen.has(key)) {
          seen.add(key);
          allParsedRows.push(row);
        }
      }
      allRelevantElements.push(...relevant);

      console.log(`   ➜ SOLD rows so far: ${allParsedRows.length}`);

      // Identify pager pieces inside BID_WINDOW_CONTAINER
      const pieces = await scope.getPagerPieces();
      if (!pieces.bar) {
        console.log('🛑 Pager bar not found (Head_C third div). Ending.');
        break;
      }

      const indicator = await scope.readIndicator(pieces);
      const current = indicator.current || (pagesVisited + 1);
      const total   = indicator.total || null;
      if (total && current >= total) {
        console.log(`🛑 Reached last page (${current}/${total}).`);
        break;
      }

      const nextIndex = current + 1;

      // Try input/set + Go/Enter (some tenants require this)
      const acted = await scope.setPageInputAndGo(pieces, nextIndex);
      if (acted) {
        const changed = await scope.waitForListChange(30000);
        if (changed) {
          pagesVisited += 1;
          continue;
        }
      }

      // Click the right arrow image (your recording's selector)
      if (pieces.next) {
        console.log('➡️ Clicking pager right arrow (span.PageRight > img)...');
        const clicked = await scope.clickNextArrow(pieces);
        if (clicked) {
          const changed = await scope.waitForListChange(30000);
          if (changed) {
            pagesVisited += 1;
            continue;
          }
        }
      }

      // If neither action changed the list, stop to avoid looping
      console.log('🛑 No list change after pager actions. Ending.');
      break;
    }

    return { relevantElements: allRelevantElements, parsedRows: allParsedRows };
  } catch (err) {
    console.error(`❌ Error on ${url}:`, err.message);
    return { relevantElements: [], parsedRows: [], error: { url, message: err.message } };
  } finally {
    await page.close();
  }
}

// =========================
// MAIN
// =========================
(async () => {
  console.log('📥 Loading URLs...');
  const urls = await loadTargetUrls();
  console.log(`🔗 Got ${urls.length} URL(s) to process.`);

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

  // Global dedupe
  const uniqueMap = new Map();
  for (const row of allRows) {
    const key = `${row.sourceUrl}|${row.caseNumber}|${row.parcelId}`;
    if (!uniqueMap.has(key)) uniqueMap.set(key, row);
  }
  const finalRows = [...uniqueMap.values()];

  // Summary artifact
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

  // Write artifacts
  fs.writeFileSync(OUTPUT_ELEMENTS_FILE, JSON.stringify(allElements, null, 2));
  fs.writeFileSync(OUTPUT_ROWS_FILE, JSON.stringify(finalRows, null, 2));
  fs.writeFileSync(OUTPUT_SUMMARY_FILE, JSON.stringify(summary, null, 2));

  if (errors.length) {
    fs.writeFileSync(OUTPUT_ERRORS_FILE, JSON.stringify(errors, null, 2));
    console.log(`⚠️ Saved ${errors.length} error(s) → ${OUTPUT_ERRORS_FILE}`);
  }

  console.log(`✅ Saved ${allElements.length} elements → ${OUTPUT_ELEMENTS_FILE}`);
  console.log(`✅ Saved ${finalRows.length} SOLD auctions → ${OUTPUT_ROWS_FILE}`);
  console.log(`📊 Saved summary → ${OUTPUT_SUMMARY_FILE}`);
  console.log('🏁 Done');
})();