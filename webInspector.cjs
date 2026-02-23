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
const OUTPUT_PAGER_DEBUG = 'pager-sniff.json';

const MIN_SURPLUS = 25000;
const MAX_PAGES = 50; // safety stop per URL

// =========================
/** GOOGLE AUTH */
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
  // Sheets/copy often double-encodes & ... normalize to real '&'
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
 * Robust vs. :contains(); matches with or without trailing colon.
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
 * Find realm (page or iframe) that contains the auction list (div[aid]).
 * Returns { dom } exposing waitForSelector/content/evaluate APIs.
 */
async function resolveContentRealm(page) {
  try {
    await page.waitForSelector('div[aid]', { timeout: 2500 });
    return { dom: page };
  } catch (_) {}

  try {
    await page.waitForSelector('iframe', { timeout: 60000 });
  } catch (_) {}

  let candidateFrame = null;
  for (let attempt = 0; attempt < 20 && !candidateFrame; attempt++) {
    const frames = page.frames();
    for (const f of frames) {
      try {
        await f.waitForSelector('div[aid], #BID_WINDOW_CONTAINER', { timeout: 1500 });
        await f.waitForSelector('div[aid]', { timeout: 1500 });
        candidateFrame = f;
        break;
      } catch (_) {}
    }
    if (!candidateFrame) await page.waitForTimeout(500);
  }

  if (candidateFrame) return { dom: candidateFrame };

  try {
    await page.waitForSelector('#BID_WINDOW_CONTAINER', { timeout: 3000 });
    return { dom: page };
  } catch (_) {}

  throw new Error('Could not locate auction content (div[aid]) in page or iframes.');
}

/**
 * Discover "pager links" on the top page (controls realm).
 * Returns a list of absolute hrefs and a guessed pageParam name if any.
 */
async function sniffPagerLinks(page) {
  // Crawl all anchors on the *top-level page* for any href that:
  //  - contains numeric page-like params (page, pagenum, p, start),
  //  - or looks like "next" by text/title/alt/src.
  const anchors = await page.evaluate(() => {
    const aTags = Array.from(document.querySelectorAll('a[href]'));
    const rows = [];
    for (const a of aTags) {
      const href = a.href;
      const txt  = (a.innerText || a.textContent || '').trim();
      const title = (a.getAttribute('title') || '').trim();
      const alt = (a.getAttribute('alt') || '').trim();
      const img = a.querySelector('img');
      const imgSrc = img ? (img.getAttribute('src') || '').toLowerCase() : '';
      rows.push({
        href,
        txt: txt.toLowerCase(),
        title: title.toLowerCase(),
        alt: alt.toLowerCase(),
        imgSrc,
      });
    }
    return rows;
  });

  // Heuristic to guess param name seen in hrefs (page, PAGE, pagenum, p, start, startrow)
  const urlParamRegex = /\b(page|pagenum|p|pg|pageno|start|startrow|offset)=([0-9]+)/i;
  const paramCounts = {};
  const withParams = [];

  for (const a of anchors) {
    const m = a.href.match(urlParamRegex);
    if (m) {
      const key = m[1];
      const val = parseInt(m[2], 10);
      paramCounts[key.toLowerCase()] = (paramCounts[key.toLowerCase()] || 0) + 1;
      withParams.push({ ...a, key: key.toLowerCase(), val });
    }
  }

  // Decide most common page-like parameter
  let guessedParam = null;
  let maxCount = 0;
  for (const [k, c] of Object.entries(paramCounts)) {
    if (c > maxCount) { guessedParam = k; maxCount = c; }
  }

  // Also flag "next-ish" anchors (for scoring when choosing next link)
  const nextish = anchors.map(a => {
    const looksNext =
      a.txt.includes('next') ||
      a.title.includes('next') ||
      a.alt.includes('next') ||
      a.imgSrc.includes('right') || a.imgSrc.includes('arrow') || a.imgSrc.includes('next');
    return { ...a, looksNext };
  });

  return { anchors: nextish, withParams, guessedParam };
}

/**
 * Build a URL for the next page using a discovered page param and a current index.
 * If no current known, tries to increment based on links seen on the page.
 */
function buildNextUrlFromParam(currentUrl, paramName, nextIndex) {
  try {
    const url = new URL(currentUrl, 'https://dummy-base.invalid/');
    // If param absent, append; else replace.
    url.searchParams.set(paramName, String(nextIndex));
    // Drop dummy base in output if necessary
    const out = url.href.replace('https://dummy-base.invalid/', '');
    return out;
  } catch {
    // Fallback: naive append
    const sep = currentUrl.includes('?') ? '&' : '?';
    return `${currentUrl}${sep}${encodeURIComponent(paramName)}=${encodeURIComponent(nextIndex)}`;
  }
}

/**
 * Choose the next page URL using:
 *  1) Discovered page param (page/pagenum/...), increment index.
 *  2) Else pick a "next-ish" anchor with a higher page value than current.
 *  3) Else naive &page= index+1.
 */
function chooseNextUrl(currentUrl, pageIndex, pagerSniff) {
  // 1) Use discovered param if any
  if (pagerSniff.guessedParam) {
    return buildNextUrlFromParam(currentUrl, pagerSniff.guessedParam, pageIndex + 1);
  }

  // 2) If there are anchors with numeric params, pick the smallest href that advances
  const urlParamRegex = /\b(page|pagenum|p|pg|pageno|start|startrow|offset)=([0-9]+)/i;
  const forwards = [];
  for (const a of pagerSniff.anchors) {
    const m = a.href.match(urlParamRegex);
    if (!m) continue;
    const key = m[1].toLowerCase();
    const val = parseInt(m[2], 10);
    if (Number.isFinite(val) && val > pageIndex) {
      // prefer "next-ish"
      const score = a.looksNext ? 2 : 1;
      forwards.push({ href: a.href, val, score });
    }
  }
  if (forwards.length) {
    forwards.sort((a, b) => (b.score - a.score) || (a.val - b.val));
    return forwards[0].href;
  }

  // 3) Naive fallback: &page= index+1
  const sep = currentUrl.includes('?') ? '&' : '?';
  return `${currentUrl}${sep}page=${pageIndex + 1}`;
}

/** Wait for the list (`div[aid]`) to change after a navigation */
async function waitForListChange(contentDom, timeoutMs = 25000) {
  const start = Date.now();
  const priorFirstRowHtml = await contentDom.evaluate(() => {
    const first = document.querySelector('div[aid]');
    return first ? first.innerHTML : '__NONE__';
  });

  while (Date.now() - start < timeoutMs) {
    try {
      const current = await contentDom.evaluate(() => {
        const first = document.querySelector('div[aid]');
        return first ? first.innerHTML : '__NONE__';
      });
      if (current !== priorFirstRowHtml) return true;
    } catch { /* frame may reload */ }
    await new Promise(r => setTimeout(r, 400));
  }
  return false;
}

// =========================
/** Sitemap-accurate parser (SOLD only) for div[aid] cards */
// =========================
function parseAuctionsFromHtml_SitemapAccurate(html, pageUrl) {
  const $ = cheerio.load(html);
  const rows = [];
  const relevant = [];

  $('div[aid]').each((_, item) => {
    const $item = $(item);

    // Capture for diagnostics
    const blockText = clean($item.text());
    if (blockText) {
      relevant.push({
        sourceUrl: pageUrl,
        tag: 'div',
        attrs: $item.attr() || {},
        text: blockText.slice(0, 2000),
      });
    }

    // Map selectors from the sitemap to robust extractors
    const caseNumber    = getByThLabel($, $item, 'Cause Number');
    const assessedValue = getByThLabel($, $item, 'Adjudged Value');
    const openingBid    = getByThLabel($, $item, 'Est. Min. Bid');
    const parcelId      = getByThLabel($, $item, 'Account Number');
    const streetAddress = getByThLabel($, $item, 'Property Address');

    let cityStateZip = clean($item.find('tr:nth-of-type(8) td').first().text());
    if (!cityStateZip) {
      cityStateZip =
        getByThLabel($, $item, 'City, State Zip') ||
        getByThLabel($, $item, 'City/State/Zip') ||
        '';
    }

    const status     = clean($item.find('div.ASTAT_MSGA').first().text());
    theSold = clean($item.find('div.ASTAT_MSGD').first().text());
    const soldAmount = theSold;

    // SOLD-only filter
    const looksSold =
      status.toLowerCase().includes('sold') ||
      (!!soldAmount && parseCurrency(soldAmount) !== null);

    if (!looksSold) return;

    // Build row
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
      auctionDate: '',
      cityStateZip: clean(cityStateZip),
      status: clean(status),
    };

    // validation akin to your original
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
/** Inspect + Parse Page (SOLD only) — URL-driven pagination */
// =========================
async function inspectAndParse(browser, url) {
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  const allRelevantElements = [];
  const allParsedRows = [];
  const pagerDebug = [];
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

    let currentUrl = normalizeAmpersands(url);
    let pageIndex = 1;
    let pagesVisited = 0;

    while (pagesVisited < MAX_PAGES) {
      console.log(`🌐 Visiting ${currentUrl}`);
      await page.goto(currentUrl, { waitUntil: 'networkidle2', timeout: 120000 });
      await page.waitForTimeout(800); // small settle

      // Discover content realm (list may be in iframe)
      let { dom: contentDom } = await resolveContentRealm(page);
      await contentDom.waitForSelector('div[aid]', { timeout: 60000 });

      // Parse current page
      const html = await contentDom.content();

      // quick block check (top-level only)
      const topHtml = await page.content();
      if (
        topHtml.includes('403 Forbidden') ||
        topHtml.includes('Access Denied') ||
        topHtml.toLowerCase().includes('forbidden')
      ) {
        throw new Error('Blocked by target website (403)');
      }

      const { rows, relevant } = parseAuctionsFromHtml_SitemapAccurate(html, currentUrl);

      // Dedupe & collect
      for (const row of rows) {
        const key = `${row.sourceUrl}|${row.caseNumber}|${row.parcelId}`;
        if (!seen.has(key)) {
          seen.add(key);
          allParsedRows.push(row);
        }
      }
      allRelevantElements.push(...relevant);

      console.log(`   ➜ SOLD rows so far: ${allParsedRows.length}`);

      // Sniff pager links on the top page and decide next URL
      const sniff = await sniffPagerLinks(page);
      pagerDebug.push({ url: currentUrl, guessedParam: sniff.guessedParam });

      // Try to pick a next URL
      const nextUrl = chooseNextUrl(currentUrl, pageIndex, sniff);

      // If the next URL equals current or does not actually advance, stop
      if (!nextUrl || nextUrl === currentUrl) {
        console.log('🛑 No usable next URL discovered. End of pagination.');
        break;
      }

      // Naively detect repeated page (avoid loops): load the next URL headlessly to see if list changes
      console.log('➡️ Probing next URL...', nextUrl);

      // Navigate and ensure content changes; if not, we stop
      const prevFirstHtml = await contentDom.evaluate(() => {
        const first = document.querySelector('div[aid]');
        return first ? first.innerHTML : '__NONE__';
      });

      // Go to next page
      await page.goto(nextUrl, { waitUntil: 'networkidle2', timeout: 120000 });
      await page.waitForTimeout(600);

      // Re-resolve content realm (iframe may be rebuilt)
      try {
        ({ dom: contentDom } = await resolveContentRealm(page));
        await contentDom.waitForSelector('div[aid]', { timeout: 20000 });
      } catch {
        console.log('🛑 Could not find list on next page. Ending.');
        break;
      }

      const changed = await waitForListChange(contentDom, 8000);
      if (!changed) {
        // As another check, compare with cached prevFirstHtml
        const nowFirstHtml = await contentDom.evaluate(() => {
          const first = document.querySelector('div[aid]');
          return first ? first.innerHTML : '__NONE__';
        });
        if (nowFirstHtml === prevFirstHtml) {
          console.log('🛑 No list change after navigating to next URL. Ending.');
          break;
        }
      }

      // Advance counters
      currentUrl = nextUrl;
      pageIndex += 1;
      pagesVisited += 1;
    }

    // Dump pager debug to help future tuning
    try {
      fs.writeFileSync(OUTPUT_PAGER_DEBUG, JSON.stringify(pagerDebug, null, 2));
    } catch {}

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
