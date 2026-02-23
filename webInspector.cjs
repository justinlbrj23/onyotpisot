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
  // Decode common ampersand encodings coming from spreadsheets or copy/paste
  // &amp;amp;  -> &amp;  -> &
  return u
    .replace(/&amp;amp;/gi, '&amp;')
    .replace(/&amp;/gi, '&')
    .replace(/%26amp%3B/gi, '&'); // URL-encoded variant
}

async function loadTargetUrls() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!${URL_RANGE}`,
  });

  const urls = (res.data.values || [])
    .flat()
    .map(v => (v || '').trim())
    .filter(v => v.startsWith('http'))
    .map(normalizeAmpersands);

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
 * More robust than :contains(); matches with or without a trailing colon.
 */
function getByThLabel($, $ctx, label) {
  let value = '';
  const target = clean(label).toLowerCase().replace(/:\s*$/, ''); // remove trailing colon
  $ctx.find('th').each((_, el) => {
    const thText = clean($(el).text()).toLowerCase().replace(/:\s*$/, '');
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
 * Best-effort detection of auction content realm (top page or iframe).
 * Returns { dom } where `dom` is a Page or Frame exposing waitForSelector/content/evaluate APIs.
 */
async function resolveContentRealm(page) {
  // Try top-level page
  try {
    await page.waitForSelector('div[aid]', { timeout: 2500 });
    return { dom: page };
  } catch (_) {}

  // Try frames
  try {
    await page.waitForSelector('iframe', { timeout: 60000 });
  } catch (_) {}

  let candidateFrame = null;
  for (let attempt = 0; attempt < 20 && !candidateFrame; attempt++) {
    const frames = page.frames();
    for (const f of frames) {
      try {
        await f.waitForSelector('div[aid], #BID_WINDOW_CONTAINER', { timeout: 1500 });
        await f.waitForSelector('div[aid]', { timeout: 1500 }); // confirm rows exist
        candidateFrame = f;
        break;
      } catch (_) {}
    }
    if (!candidateFrame) {
      await page.waitForTimeout(500);
    }
  }

  if (candidateFrame) return { dom: candidateFrame };

  // Fallback to known container at top-level
  try {
    await page.waitForSelector('#BID_WINDOW_CONTAINER', { timeout: 3000 });
    return { dom: page };
  } catch (_) {}

  throw new Error('Could not locate auction content (div[aid]) in page or iframes.');
}

/**
 * Controls realm: pager container could be in the same realm, parent frame, top page, or another frame.
 * We scan for a visible element whose text matches /page <num> (of|/) <num>/.
 * Returns { dom, container } where container is the pager element handle (or null).
 */
async function resolveControlsRealm(page, contentDom) {
  async function locatePagerContainer(realm) {
    const handle = await realm.evaluateHandle(() => {
      const isVisible = (el) => {
        const s = getComputedStyle(el);
        const r = el.getBoundingClientRect();
        if (!el || !el.ownerDocument) return false;
        return s.display !== 'none' && s.visibility !== 'hidden' && r.width > 0 && r.height > 0;
      };
      const re = /\bpage\b\s*\d+\s*(?:of|\/)\s*\d+/i;
      const nodes = Array.from(document.querySelectorAll('*')).filter(isVisible);
      // Prefer smaller text blocks (avoid picking huge sections)
      let best = null;
      for (const el of nodes) {
        const t = (el.innerText || el.textContent || '').replace(/\s+/g, ' ').trim();
        if (!t) continue;
        if (re.test(t)) {
          if (!best || (el.innerText || '').length < (best.innerText || '').length) {
            best = el;
          }
        }
      }
      return best || null;
    });
    return handle.asElement();
  }

  // Try same realm as content
  let container = await locatePagerContainer(contentDom);
  if (container) return { dom: contentDom, container };

  // Try parent of content frame
  if (typeof contentDom.parentFrame === 'function') {
    const parent = contentDom.parentFrame();
    if (parent) {
      container = await locatePagerContainer(parent);
      if (container) return { dom: parent, container };
    }
  }

  // Try top-level page
  container = await locatePagerContainer(page);
  if (container) return { dom: page, container };

  // Try any other frame
  for (const f of page.frames()) {
    container = await locatePagerContainer(f);
    if (container) return { dom: f, container };
  }

  // Last resort: no container found (we'll gracefully stop pagination)
  return { dom: contentDom, container: null };
}

/** Read "Page X of Y" (or "page X / Y") from the given container */
async function getPageIndicatorFromContainer(dom, container) {
  if (!container) return null;
  try {
    const data = await dom.evaluate((el) => {
      const text = (el.innerText || el.textContent || '').replace(/\s+/g, ' ').trim();

      // Try to read current from a nearby input/select if present
      let current = null;
      const inputs = Array.from(el.querySelectorAll('input, select'))
        .filter(n => {
          const s = getComputedStyle(n);
          const r = n.getBoundingClientRect();
          return s.display !== 'none' && s.visibility !== 'hidden' && r.width > 0 && r.height > 0;
        });

      for (const n of inputs) {
        if (n.tagName === 'INPUT') {
          const v = (n.value || '').trim();
          if (/^\d+$/.test(v)) { current = Number(v); break; }
        } else if (n.tagName === 'SELECT') {
          let v = n.value;
          if (!v && n.selectedIndex >= 0) v = String(n.selectedIndex + 1);
          if (v && /^\d+$/.test(v)) { current = Number(v); break; }
        }
      }

      // Parse "page 1 of 5" or "page 1 / 5"
      const m = text.match(/\bpage\b\s*:?\s*([0-9]+)\s*(?:of|\/)\s*([0-9]+)/i);
      const total = m ? Number(m[2]) : null;
      if (current == null && m) current = Number(m[1]);

      return { raw: text, current, total };
    }, container);

    if (data && (data.current || data.total)) return data;
  } catch (_) {}
  return null;
}

/** Find Next button INSIDE the pager container */
async function findNextInContainer(dom, container) {
  if (!container) return { handle: null, disabled: true };

  const h = await dom.evaluateHandle((el) => {
    const isVisible = (n) => {
      const s = getComputedStyle(n);
      const r = n.getBoundingClientRect();
      return s.display !== 'none' && s.visibility !== 'hidden' && r.width > 0 && r.height > 0;
    };

    const candidates = [];
    const all = el.querySelectorAll('a, button, input, img, span');

    for (const node of all) {
      if (!isVisible(node)) continue;

      const txt =
        (node.innerText || node.textContent || '').trim().toLowerCase() ||
        (node.getAttribute('alt') || '').trim().toLowerCase() ||
        (node.getAttribute('title') || '').trim().toLowerCase() ||
        (node.getAttribute('value') || '').trim().toLowerCase();

      const src = (node.getAttribute('src') || '').toLowerCase();

      const looksNext =
        txt === '>' ||
        txt === '>>' ||
        txt.includes('next') ||
        txt.includes('›') ||
        txt.includes('»') ||
        src.includes('next') ||
        src.includes('right') ||
        src.includes('arrow');

      if (!looksNext) continue;

      // Skip obvious "fast-forward" >> unless no other choice
      const score =
        txt === '>' || txt.includes('next') || txt.includes('›') || txt.includes('»') ? 2 :
        txt === '>>' ? 1 :
        1;

      candidates.push({ node, score });
    }

    if (!candidates.length) return null;
    candidates.sort((a, b) => b.score - a.score);
    return candidates[0].node;
  }, container);

  const handle = h.asElement();
  if (!handle) return { handle: null, disabled: true };

  const disabled = await dom.evaluate((el) => {
    const style = getComputedStyle(el);
    const r = el.getBoundingClientRect();
    const hidden = style.display === 'none' || style.visibility === 'hidden' || r.width === 0 || r.height === 0;
    const p = el.closest('.disabled, .PageRight_DIS, [aria-disabled="true"]');
    const cls = (el.getAttribute('class') || '').toLowerCase();
    return hidden || !!p || cls.includes('disabled') || style.pointerEvents === 'none';
  }, handle);

  return { handle, disabled };
}

/** Click the nearest clickable ancestor (a/button/input) for a given handle */
async function clickNearestClickable(dom, handle) {
  try {
    await dom.evaluate((el) => {
      const clickable = el.closest('a, button, input[type="submit"], input[type="button"]');
      (clickable || el).click();
    }, handle);
    return true;
  } catch {
    try {
      await handle.click({ delay: 20 });
      return true;
    } catch {
      return false;
    }
  }
}

/** Wait for the list (`div[aid]`) to change after Next click */
async function waitForListChange(contentDom, timeoutMs = 25000) {
  const start = Date.now();
  const priorFirstRowHtml = await contentDom.evaluate(() => {
    const first = document.querySelector('div[aid]');
    return first ? first.innerHTML : '__NONE__';
  });

  // Poll for change (covers AJAX swaps and frame reloads)
  while (Date.now() - start < timeoutMs) {
    try {
      const current = await contentDom.evaluate(() => {
        const first = document.querySelector('div[aid]');
        return first ? first.innerHTML : '__NONE__';
      });
      if (current !== priorFirstRowHtml) return true;
    } catch {
      // frame may be reloading; brief pause then retry
    }
    await new Promise((r) => setTimeout(r, 400));
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
    const soldAmount = clean($item.find('div.ASTAT_MSGD').first().text());

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
/** Inspect + Parse Page (SOLD only) with iframe + robust pagination */
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

    const normalizedUrl = normalizeAmpersands(url);
    await page.goto(normalizedUrl, { waitUntil: 'networkidle0', timeout: 120000 });

    // Resolve realms
    let { dom: contentDom } = await resolveContentRealm(page);
    let { dom: controlsDom, container: pagerContainer } = await resolveControlsRealm(page, contentDom);

    // Wait for list
    await contentDom.waitForSelector('div[aid]', { timeout: 60000 });

    let pageCount = 0;

    while (pageCount < MAX_PAGES) {
      // Indicator (from pager container if found)
      let indicator = await getPageIndicatorFromContainer(controlsDom, pagerContainer);
      if (indicator) {
        console.log(`📄 Page ${indicator.current ?? '?'} of ${indicator.total ?? '?'} (raw: "${indicator.raw}")`);
      } else {
        console.log(`📄 Page (indicator not found)`);
      }

      // Parse current page (from content realm)
      const html = await contentDom.content();
      const { rows, relevant } = parseAuctionsFromHtml_SitemapAccurate(html, normalizedUrl);

      // Dedupe & collect rows
      for (const row of rows) {
        const key = `${row.sourceUrl}|${row.caseNumber}|${row.parcelId}`;
        if (!seen.has(key)) {
          seen.add(key);
          allParsedRows.push(row);
        }
      }
      allRelevantElements.push(...relevant);

      console.log(`   ➜ SOLD rows so far: ${allParsedRows.length}`);

      // Stop if this is the last page per indicator
      if (indicator && indicator.total && indicator.current && indicator.current >= indicator.total) {
        console.log(`🛑 Reached last page (${indicator.current}/${indicator.total}).`);
        break;
      }

      // Find Next in the pager container
      let { handle: nextHandle, disabled } = await findNextInContainer(controlsDom, pagerContainer);

      // If not found (or disabled), try to re-resolve the controls realm (some tenants swap containers after navigation)
      if (!nextHandle || disabled) {
        const re = await resolveControlsRealm(page, contentDom);
        controlsDom = re.dom;
        pagerContainer = re.container || pagerContainer;

        ({ handle: nextHandle, disabled } = await findNextInContainer(controlsDom, pagerContainer));
      }

      if (!nextHandle || disabled) {
        console.log('🛑 Next not available (missing or disabled). End of pagination.');
        break;
      }

      console.log('➡️ Moving to next page...');

      // Scroll and click nearest clickable ancestor
      try {
        await controlsDom.evaluate((el) => el.scrollIntoView({ block: 'center', behavior: 'instant' }), nextHandle);
      } catch (_) {}
      const clicked = await clickNearestClickable(controlsDom, nextHandle);
      if (!clicked) {
        console.log('⚠️ Click failed. Stopping to avoid loop.');
        break;
      }

      // Wait for content to actually change; if it doesn't, try to re-resolve content realm (frame could reload)
      const changed = await waitForListChange(contentDom, 30000);
      if (!changed) {
        console.log('⚠️ No list change detected. Re-resolving content realm...');
        try {
          const resolved = await resolveContentRealm(page);
          contentDom = resolved.dom;
          await contentDom.waitForSelector('div[aid]', { timeout: 20000 });
        } catch {
          console.log('🛑 Could not re-locate content after Next. Ending.');
          break;
        }
      }

      // Re-locate pager after navigation (some sites rebuild the toolbar)
      const re = await resolveControlsRealm(page, contentDom);
      controlsDom = re.dom;
      pagerContainer = re.container || pagerContainer;

      pageCount += 1;
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
    headless: true, // set to false locally to observe
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
``
