// Requires:
// npm install puppeteer cheerio googleapis

const fs = require('fs');
const puppeteer = require('puppeteer');
const cheerio = require('cheerio');
const { google } = require('googleapis');

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = './service-account.json';
const SPREADSHEET_ID = '1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA';
const SHEET_RANGE = 'Palm Beach - Taxdeed!A:I';

// IMPORTANT: Use '&' not '&amp;' in JS strings
const TARGET_URL =
  'https://dallas.texas.sheriffsaleauctions.com/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE=02/03/2026';

const MAX_PAGES = 50; // safety stop

// =========================
// GOOGLE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ['https://www.googleapis.com/auth/spreadsheets'],
});

const sheets = google.sheets({ version: 'v4', auth });

// =========================
/** HELPERS */
// =========================
function clean(text) {
  if (!text) return '';
  return text.replace(/\s+/g, ' ').trim();
}

/**
 * Extract the TD text that follows a TH containing a given label (case-insensitive).
 */
function getByLabel($, $ctx, label) {
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
 * Resolve the DOM "realm" where the auctions exist. Some sites render inside an iframe,
 * others are top-level. Return an object whose 'dom' can be used like a page/frame.
 */
async function resolveAuctionRealm(page) {
  // Try top-level quickly
  try {
    await page.waitForSelector('div[aid]', { timeout: 2500 });
    return { dom: page, realm: 'page' };
  } catch (_) {}

  // If not found, wait for possible iframes/containers
  try {
    await page.waitForSelector('iframe', { timeout: 60000 });
  } catch (_) {
    // continue - not all pages show iframes immediately
  }

  // Try to identify a frame that has rows or container
  let candidateFrame = null;
  for (let attempt = 0; attempt < 20 && !candidateFrame; attempt++) {
    const frames = page.frames();
    for (const f of frames) {
      try {
        await f.waitForSelector('div[aid], #BID_WINDOW_CONTAINER', { timeout: 1500 });
        candidateFrame = f;
        break;
      } catch (_) {
        // keep looping
      }
    }
    if (!candidateFrame) {
      await page.waitForTimeout(500);
    }
  }

  if (candidateFrame) {
    return { dom: candidateFrame, realm: 'frame' };
  }

  // Fallback to top-level container
  try {
    await page.waitForSelector('#BID_WINDOW_CONTAINER', { timeout: 3000 });
    return { dom: page, realm: 'page' };
  } catch (_) {}

  throw new Error('Could not locate auction content in page or iframes.');
}

/**
 * Parse "Page X of Y" from .Head_C div:nth-of-type(3) span.PageText
 * Returns { current, total } as numbers, or null if not found.
 */
async function getPageIndicator(dom) {
  try {
    const text = await dom.$eval(
      '.Head_C div:nth-of-type(3) span.PageText',
      (el) => (el.innerText || el.textContent || '').trim()
    );
    // Match patterns like "Page 1 of 7", "Page: 3 of 12", etc.
    const m = text.match(/page\s*[:]?[\s]*([0-9]+)\s+of\s+([0-9]+)/i);
    if (m) {
      return { current: Number(m[1]), total: Number(m[2]) };
    }
  } catch (_) {
    // ignore
  }
  return null;
}

/**
 * Find the "Next" button using your selector: .PageRight_HVR img
 * Returns an element handle or null.
 */
async function findNextHandle(dom) {
  try {
    // Ensure the container exists (best-effort)
    await dom.waitForSelector('.PageRight_HVR img', { timeout: 3000 }).catch(() => {});
    const h = await dom.$('.PageRight_HVR img');
    if (!h) return null;

    // Filter out hidden/disabled
    const isClickable = await dom.evaluate((el) => {
      const style = window.getComputedStyle(el);
      const hidden =
        style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0';
      // Some sites disable via parent class
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

/**
 * After clicking "Next", wait for either navigation or a DOM change in the list.
 */
async function waitForListChange(dom) {
  const priorFirstRowHtml = await dom.evaluate(() => {
    const first = document.querySelector('div[aid]');
    return first ? first.innerHTML : '';
  });

  // Wait for navigation OR in-place DOM change
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

  // Small settle
  const maybePage = typeof dom.page === 'function' ? dom.page() : dom;
  if (maybePage && typeof maybePage.waitForTimeout === 'function') {
    await maybePage.waitForTimeout(400);
  } else {
    await new Promise((r) => setTimeout(r, 400));
  }
}

// =========================
/** SCRAPER WITH PAGINATION */
// =========================
async function scrapeAllPages(url) {
  let browser;
  const allResults = [];

  try {
    browser = await puppeteer.launch({
      headless: true, // set false locally to watch
      executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--single-process',
        '--no-zygote',
      ],
    });

    const page = await browser.newPage();
    page.setDefaultNavigationTimeout(120000);

    // Friendlier UA can reduce gating
    await page.setUserAgent(
      'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
    );

    await page.goto(url, { waitUntil: 'networkidle0', timeout: 120000 });

    // Best-effort handle consent
    try {
      const agreeBtns = await page.$$(
        "button, input[type='button'], input[type='submit']"
      );
      for (const btn of agreeBtns) {
        const shouldClick = await page.evaluate((el) => {
          const t = (el.innerText || el.value || '').toLowerCase();
          return t.includes('i agree') || t.includes('accept') || t.includes('agree');
        }, btn);
        if (shouldClick) {
          await btn.click();
          await page.waitForTimeout(800);
          break;
        }
      }
    } catch (_) {}

    // Resolve where the auction rows live (page or iframe)
    const { dom } = await resolveAuctionRealm(page);

    // Wait for at least one row
    await dom.waitForSelector('div[aid]', { timeout: 60000 });

    // Determine starting page/total via indicator (if present)
    let indicator = await getPageIndicator(dom);
    let currentPage = indicator?.current || 1;
    const totalPages = indicator?.total || null;

    while (currentPage <= MAX_PAGES) {
      console.log(`📄 Scraping page ${currentPage}...`);

      // Pull DOM HTML from the realm and parse with Cheerio
      const html = await dom.content();
      const $ = cheerio.load(html);

      const pageResults = [];

      $('div[aid]').each((_, item) => {
        const $item = $(item);

        const record = {
          caseNumber: getByLabel($, $item, 'Cause Number'),
          assessedValue: getByLabel($, $item, 'Adjudged Value'),
          openingBid: getByLabel($, $item, 'Est. Min. Bid'),
          parcelId: getByLabel($, $item, 'Account Number'),
          streetAddress: getByLabel($, $item, 'Property Address'),
          cityStateZip:
            getByLabel($, $item, 'City, State Zip') ||
            getByLabel($, $item, 'City') ||
            clean($item.find('tr:nth-of-type(8) td').first().text()),
          status: clean($item.find('div.ASTAT_MSGA').first().text()),
          soldAmount: clean($item.find('div.ASTAT_MSGD').first().text()),
        };

        pageResults.push(record);
      });

      console.log(`   ➜ Found ${pageResults.length} auctions`);
      allResults.push(...pageResults);

      // If we know total pages and are at/over the end, stop
      indicator = await getPageIndicator(dom);
      if (indicator && indicator.total && indicator.current >= indicator.total) {
        console.log(`🛑 Reached last page (${indicator.current} of ${indicator.total}).`);
        break;
      }

      // Try to find and click "Next"
      const nextHandle = await findNextHandle(dom);
      if (!nextHandle) {
        console.log('🛑 Next button not found (.PageRight_HVR img). End of pagination.');
        break;
      }

      console.log('➡️ Moving to next page...');

      // Scroll into view (helps in headless)
      try {
        await dom.evaluate((el) => el.scrollIntoView({ block: 'center', behavior: 'instant' }), nextHandle);
      } catch (_) {}

      // Click and wait for list change
      await Promise.allSettled([
        dom.waitForNavigation({ waitUntil: 'networkidle0', timeout: 15000 }),
        nextHandle.click(),
      ]);

      try {
        await waitForListChange(dom);
      } catch (_) {
        // As a fallback, try to see if the indicator changed
        const afterIndicator = await getPageIndicator(dom);
        if (!afterIndicator || afterIndicator.current === currentPage) {
          console.log('⚠️ Did not detect list change after "Next". Stopping.');
          break;
        }
      }

      // Update current page from indicator if available; else increment
      indicator = await getPageIndicator(dom);
      if (indicator && indicator.current) {
        currentPage = indicator.current;
      } else {
        currentPage += 1;
      }

      // Safety stop if we somehow exceed a known total
      if (indicator && indicator.total && currentPage > indicator.total) {
        console.log(`🛑 Exceeded total pages (${indicator.total}). Stopping.`);
        break;
      }
    }

    return allResults;
  } catch (err) {
    console.error('❌ Scraping error:', err);

    // Diagnostics in CI: page screenshot + HTML dump
    try {
      if (browser) {
        const pages = await browser.pages();
        if (pages && pages[0]) {
          await pages[0].screenshot({ path: 'scrape_error.png', fullPage: true });
          const htmlDump = await pages[0].content();
          fs.writeFileSync('scrape_error.html', htmlDump);
          console.log('🖼️ Saved scrape_error.png and scrape_error.html for diagnosis.');
        }
      }
    } catch (e) {
      console.warn('Could not create diagnostics:', e.message);
    }

    return [];
  } finally {
    if (browser) {
      await browser.close();
    }
  }
}

// =========================
/** GOOGLE SHEETS APPEND */
// =========================
async function appendToSheet(data) {
  if (!data.length) {
    console.warn('⚠️ No auction data found.');
    return;
  }

  const timestamp = new Date().toISOString();

  const values = data.map((r) => [
    timestamp,
    r.caseNumber,
    r.assessedValue,
    r.openingBid,
    r.parcelId,
    r.streetAddress,
    r.cityStateZip,
    r.status,
    r.soldAmount,
  ]);

  try {
    const existing = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: SHEET_RANGE,
    });

    if (!existing.data.values || existing.data.values.length === 0) {
      values.unshift([
        'Timestamp',
        'Case Number',
        'Assessed Value',
        'Opening Bid',
        'Parcel ID',
        'Street Address',
        'City State Zip',
        'Status',
        'Sold Amount',
      ]);
    }

    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: SHEET_RANGE,
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values },
    });

    console.log(`✅ Appended ${values.length} rows.`);
  } catch (err) {
    console.error('❌ Google Sheets write error:', err);
  }
}

// =========================
/** MAIN */
// =========================
(async () => {
  console.log('🚀 Starting paginated scrape...');

  const auctions = await scrapeAllPages(TARGET_URL);

  console.log(`📦 Total auctions scraped: ${auctions.length}`);
  console.log('🧪 Sample:', auctions.slice(0, 2));

  console.log('📤 Writing to Google Sheets...');
  await appendToSheet(auctions);

  console.log('🏁 Finished.');
})();
