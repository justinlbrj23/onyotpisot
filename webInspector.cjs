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
async function loadTargetUrls() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!${URL_RANGE}`,
  });

  const urls = (res.data.values || [])
    .flat()
    .map(v => (v || '').trim())
    .filter(v => v.startsWith('http'))
    // normalize HTML-encoded query separators (web sheets often store &amp;)
    .map(u => u.replace(/&amp;/g, '&'));

  return urls;
}

// =========================
// Utils / Helpers
// =========================
function clean(text) {
  if (!text) return '';
  return text.replace(/\s+/g, ' ').trim();
}

/** Currency parser: accepts $1,234 or $1,234.56 */
function parseCurrency(str) {
  if (!str) return null;
  const n = parseFloat(str.replace(/[^0-9.-]/g, ''));
  return isNaN(n) ? null : n;
}

/**
 * Extract the TD text that follows a TH containing a given label (case-insensitive).
 * This faithfully reproduces what `th:contains("X") + td` would capture, but is robust.
 */
function getByThLabel($, $ctx, label) {
  let value = '';
  const target = label.toLowerCase();
  $ctx.find('th').each((_, el) => {
    const thText = clean($(el).text()).toLowerCase();
    if (thText.includes(target)) {
      const td = $(el).next('td');
      if (td && td.length) {
        value = clean(td.text());
      }
      return false; // break
    }
  });
  return value;
}

/**
 * Best-effort detection of auction realm (top page or iframe).
 * Returns { dom } where `dom` is a Page or Frame exposing waitForSelector/content/evaluate APIs.
 */
async function resolveAuctionRealm(page) {
  // Quick try at top-level
  try {
    await page.waitForSelector('div[aid]', { timeout: 2500 });
    return { dom: page };
  } catch (_) {}

  // Wait for iframes, then search each frame
  try {
    await page.waitForSelector('iframe', { timeout: 60000 });
  } catch (_) {}

  let candidateFrame = null;
  for (let attempt = 0; attempt < 20 && !candidateFrame; attempt++) {
    const frames = page.frames();
    for (const f of frames) {
      try {
        await f.waitForSelector('div[aid], #BID_WINDOW_CONTAINER', { timeout: 1500 });
        candidateFrame = f;
        break;
      } catch (_) {}
    }
    if (!candidateFrame) {
      await page.waitForTimeout(500);
    }
  }

  if (candidateFrame) return { dom: candidateFrame };

  // Fallback to a known container at top-level
  try {
    await page.waitForSelector('#BID_WINDOW_CONTAINER', { timeout: 3000 });
    return { dom: page };
  } catch (_) {}

  throw new Error('Could not locate auction content in page or iframes.');
}

/**
 * Pager indicator: `.Head_C div:nth-of-type(3) span.PageText` => "Page X of Y"
 */
async function getPageIndicator(dom) {
  try {
    const text = await dom.$eval(
      '.Head_C div:nth-of-type(3) span.PageText',
      (el) => (el.innerText || el.textContent || '').trim()
    );
    const m = text.match(/page\s*:?\s*([0-9]+)\s+of\s+([0-9]+)/i);
    if (m) return { current: Number(m[1]), total: Number(m[2]) };
  } catch (_) {}
  return null;
}

/** Next button handle: `.PageRight_HVR img`  */
async function findNextHandle(dom) {
  try {
    const h = await dom.$('.PageRight_HVR img');
    if (!h) return null;
    const isClickable = await dom.evaluate((el) => {
      const style = window.getComputedStyle(el);
      const hidden =
        style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0';
      const p = el.closest('.PageRight_HVR');
      const cls = (p ? p.getAttribute('class') : el.getAttribute('class') || '').toLowerCase();
      const ariaDisabled = (p ? p.getAttribute('aria-disabled') : el.getAttribute('aria-disabled')) || '';
      return !hidden && !cls.includes('disabled') && ariaDisabled !== 'true';
    }, h);
    return isClickable ? h : null;
  } catch (_) {
    return null;
  }
}

/** Wait for the list (`div[aid]`) to change after a Next click */
async function waitForListChange(dom) {
  const priorFirstRowHtml = await dom.evaluate(() => {
    const first = document.querySelector('div[aid]');
    return first ? first.innerHTML : '';
  });

  await Promise.race([
    dom.waitForNavigation({ waitUntil: 'networkidle0', timeout: 15000 }).catch(() => {}),
    (async () => {
      await dom.waitForFunction(
        (prev) => {
          const first = document.querySelector('div[aid]');
          return first && first.innerHTML !== prev;
        },
        { timeout: 20000 },
        priorFirstRowHtml
      );
    })(),
  ]);

  const maybePage = typeof dom.page === 'function' ? dom.page() : dom;
  if (maybePage && typeof maybePage.waitForTimeout === 'function') {
    await maybePage.waitForTimeout(400);
  } else {
    await new Promise((r) => setTimeout(r, 400));
  }
}

// =========================
// Sitemap-accurate parser (SOLD only) for div[aid] cards
// =========================
function parseAuctionsFromHtml_SitemapAccurate(html, pageUrl) {
  const $ = cheerio.load(html);
  const rows = [];
  const relevant = [];

  $('div[aid]').each((_, item) => {
    const $item = $(item);

    // Capture for diagnostics: text + attrs if it looks relevant
    const blockText = clean($item.text());
    if (blockText) {
      relevant.push({
        sourceUrl: pageUrl,
        tag: 'div',
        attrs: $item.attr() || {},
        text: blockText.slice(0, 2000), // cap to keep files manageable
      });
    }

    // Map selectors from your sitemap to robust extractors
    const caseNumber = getByThLabel($, $item, 'Cause Number:');
    const assessedValue = getByThLabel($, $item, 'Adjudged Value:');
    const openingBid = getByThLabel($, $item, 'Est. Min. Bid:');
    const parcelId = getByThLabel($, $item, 'Account Number:');
    const streetAddress = getByThLabel($, $item, 'Property Address:');

    // The sitemap used a positional selector for city/state/zip
    let cityStateZip = clean($item.find('tr:nth-of-type(8) td').first().text());
    // fallback if empty (some templates differ)
    if (!cityStateZip) {
      cityStateZip =
        getByThLabel($, $item, 'City, State Zip:') ||
        getByThLabel($, $item, 'City/State/Zip:') ||
        '';
    }

    const status = clean($item.find('div.ASTAT_MSGA').first().text());
    const soldAmount = clean($item.find('div.ASTAT_MSGD').first().text());

    // SOLD-only filter
    const statusLower = status.toLowerCase();
    const looksSold =
      statusLower.includes('sold') ||
      statusLower.includes('sold amount') ||
      (!!soldAmount && parseCurrency(soldAmount) !== null);

    if (!looksSold) return; // skip non-sold cards

    // Build row consistent with your downstream usage
    const openingBidNum = parseCurrency(openingBid);
    const assessedNum = parseCurrency(assessedValue);
    const salePriceNum = parseCurrency(soldAmount);

    const row = {
      sourceUrl: pageUrl,
      auctionStatus: 'Sold',
      auctionType: 'Tax Sale',
      caseNumber: clean(caseNumber),
      parcelId: clean(parcelId),
      propertyAddress: clean(streetAddress),
      openingBid: clean(openingBid),
      salePrice: clean(soldAmount), // sale price is the sold amount in this template
      assessedValue: clean(assessedValue),
      auctionDate: '', // not present on these cards; left blank unless found elsewhere
      cityStateZip: clean(cityStateZip),
      status: clean(status),
    };

    // validation similar to your original
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
// Inspect + Parse Page (SOLD only) with iframe + pagination support
// =========================
async function inspectAndParse(browser, url) {
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  const allRelevantElements = [];
  const allParsedRows = [];
  const seen = new Set();

  try {
    // Anti-bot & headers
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

    // Normalize any &amp; in URL (in case)
    const normalizedUrl = url.replace(/&amp;/g, '&');

    await page.goto(normalizedUrl, { waitUntil: 'networkidle0', timeout: 120000 });

    // Resolve target DOM (page or iframe)
    const { dom } = await resolveAuctionRealm(page);

    // Wait for list
    await dom.waitForSelector('div[aid]', { timeout: 60000 });

    // Track pagination using indicator and Next button
    let indicator = await getPageIndicator(dom);
    let currentPage = indicator?.current || 1;
    const totalPagesKnown = indicator?.total || null;

    let pageCounter = 0;
    while (pageCounter < MAX_PAGES) {
      console.log(`📄 Parsing page ${currentPage}...`);

      const html = await dom.content();
      const { rows, relevant } = parseAuctionsFromHtml_SitemapAccurate(html, normalizedUrl);

      // Dedupe and collect
      for (const row of rows) {
        const key = `${row.sourceUrl}|${row.caseNumber}|${row.parcelId}`;
        if (!seen.has(key)) {
          seen.add(key);
          allParsedRows.push(row);
        }
      }
      allRelevantElements.push(...relevant);

      // Stop if known last page
      indicator = await getPageIndicator(dom);
      if (indicator && indicator.total && indicator.current >= indicator.total) {
        console.log(`🛑 Reached last page (${indicator.current} of ${indicator.total}).`);
        break;
      }

      // Find & click Next
      const nextHandle = await findNextHandle(dom);
      if (!nextHandle) {
        console.log('🛑 Next button not found (.PageRight_HVR img). End of pagination.');
        break;
      }

      console.log('➡️ Moving to next page...');
      try {
        await dom.evaluate((el) => el.scrollIntoView({ block: 'center', behavior: 'instant' }), nextHandle);
      } catch (_) {}

      await Promise.allSettled([
        dom.waitForNavigation({ waitUntil: 'networkidle0', timeout: 15000 }),
        nextHandle.click(),
      ]);

      try {
        await waitForListChange(dom);
      } catch (_) {
        // fallback: check if indicator progressed
        const after = await getPageIndicator(dom);
        if (!after || after.current === currentPage) {
          console.log('⚠️ Did not detect list change after Next. Stopping.');
          break;
        }
      }

      indicator = await getPageIndicator(dom);
      currentPage = indicator?.current || currentPage + 1;
      pageCounter += 1;
    }

    console.log(
      `📦 Parsed SOLD auctions so far: ${allParsedRows.length}`
    );

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
    headless: true, // set false locally to observe
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
