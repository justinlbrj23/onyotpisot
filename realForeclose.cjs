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
 * This avoids relying on jQuery-only CSS like :contains().
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
 * others are top-level. We return an object whose 'dom' can be used like a page/frame.
 */
async function resolveAuctionRealm(page) {
  // First try top-level quickly
  try {
    await page.waitForSelector('div[aid]', { timeout: 2500 });
    return { dom: page, realm: 'page' };
  } catch (_) {}

  // If not found, wait for iframes to appear
  try {
    await page.waitForSelector('iframe', { timeout: 60000 });
  } catch (_) {
    // No iframe and no rows at top-level; give one last try for top-level container
    // (some pages have a container id like BID_WINDOW_CONTAINER)
  }

  // Try to identify the frame that holds the auction rows or a known container
  let candidateFrame = null;
  for (let attempt = 0; attempt < 20 && !candidateFrame; attempt++) {
    const frames = page.frames();
    for (const f of frames) {
      try {
        await f.waitForSelector('div[aid], #BID_WINDOW_CONTAINER', { timeout: 1500 });
        candidateFrame = f;
        break;
      } catch (_) {
        // ignore and keep searching
      }
    }
    if (!candidateFrame) {
      await page.waitForTimeout(500);
    }
  }

  if (candidateFrame) {
    return { dom: candidateFrame, realm: 'frame' };
  }

  // As a fallback, try top-level container again
  try {
    await page.waitForSelector('#BID_WINDOW_CONTAINER', { timeout: 3000 });
    return { dom: page, realm: 'page' };
  } catch (_) {}

  throw new Error('Could not locate auction content in page or iframes.');
}

/**
 * Find a clickable "Next" element within the given dom (page or frame).
 * We prefer XPath for text contains, with CSS fallbacks. Filters out disabled.
 */
async function findNextHandle(dom) {
  // Primary: XPath by visible text
  const xpaths = [
    "//a[contains(normalize-space(.), 'Next')]",
    "//button[contains(normalize-space(.), 'Next')]",
    "//input[translate(@value,'NEXT','next')='next']",
    "//input[contains(translate(@value,'NEXT','next'), 'next')]",
  ];

  let handles = [];
  for (const xp of xpaths) {
    const found = await dom.$x(xp);
    if (found && found.length) handles.push(...found);
  }

  // CSS fallbacks by common patterns
  const cssCandidates = [
    'a[rel="next"]',
    '.pagination-next a',
    '.pagination .next a',
    'a.next',
    'button.next',
    'input[type="submit"][value*="Next"]',
  ];
  for (const sel of cssCandidates) {
    const found = await dom.$$(sel);
    if (found && found.length) handles.push(...found);
  }

  // Deduplicate by remote object id
  const unique = [];
  const ids = new Set();
  for (const h of handles) {
    const id = h._remoteObject?.objectId || Math.random().toString(36).slice(2);
    if (!ids.has(id)) {
      unique.push(h);
      ids.add(id);
    }
  }

  // Filter out disabled or obviously hidden items
  const usable = [];
  for (const h of unique) {
    const isDisabledOrHidden = await dom.evaluate((el) => {
      const style = window.getComputedStyle(el);
      const cls = (el.getAttribute('class') || '').toLowerCase();
      const ariaDisabled = el.getAttribute('aria-disabled');
      const disabledAttr = el.hasAttribute('disabled') && el.getAttribute('disabled') !== 'false';
      const hidden = style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0';
      // Consider class-based disabled markers as well
      return hidden || disabledAttr || ariaDisabled === 'true' || cls.includes('disabled');
    }, h);

    if (!isDisabledOrHidden) usable.push(h);
  }

  return usable[0] || null;
}

/**
 * After clicking "Next", wait for either navigation or DOM change in the list.
 */
async function waitForListChange(dom) {
  const priorFirstRowHtml = await dom.evaluate(() => {
    const first = document.querySelector('div[aid]');
    return first ? first.innerHTML : '';
  });

  // Wait for either navigation or an in-place DOM change
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

  // Small settle to ensure DOM is stable
  const page = dom.page ? dom.page() : null;
  if (page && page.waitForTimeout) {
    await page.waitForTimeout(400);
  } else {
    // fallback if 'dom' is a Page without page()
    await (dom.waitForTimeout ? dom.waitForTimeout(400) : new Promise(r => setTimeout(r, 400)));
  }
}

// =========================
// SCRAPER WITH PAGINATION
// =========================
async function scrapeAllPages(url) {
  let browser;
  const allResults = [];

  try {
    browser = await puppeteer.launch({
      headless: true, // keep true for CI; set false locally to watch it work
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

    // A friendlier UA can reduce gating
    await page.setUserAgent(
      'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
    );

    await page.goto(url, { waitUntil: 'networkidle0', timeout: 120000 });

    // Handle common disclaimers or cookie banners if present (best-effort)
    try {
      const agree = await page.$x("//button[contains(., 'I Agree')] | //input[@value='I Agree']");
      if (agree && agree[0]) {
        await agree[0].click();
        await page.waitForTimeout(800);
      }
    } catch (_) {}

    // Resolve where the auction rows live (page or iframe)
    const { dom, realm } = await resolveAuctionRealm(page);

    // Wait for at least one row
    await dom.waitForSelector('div[aid]', { timeout: 60000 });

    let currentPage = 1;

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
          // Try label, fallback to the original nth-of-type if label not found
          cityStateZip:
            getByLabel($, $item, 'City') ||
            getByLabel($, $item, 'City, State Zip') ||
            clean($item.find('tr:nth-of-type(8) td').first().text()),
          status: clean($item.find('div.ASTAT_MSGA').first().text()),
          soldAmount: clean($item.find('div.ASTAT_MSGD').first().text()),
        };

        pageResults.push(record);
      });

      console.log(`   ➜ Found ${pageResults.length} auctions`);
      allResults.push(...pageResults);

      // Try to find a usable "Next" control
      const nextHandle = await findNextHandle(dom);
      if (!nextHandle) {
        console.log('🛑 No "Next" found. End of pagination.');
        break;
      }

      // Before we click Next, double-check if it's actually the last page (some UIs leave a dead Next)
      const isDisabled = await dom.evaluate((el) => {
        const cls = (el.getAttribute('class') || '').toLowerCase();
        const disabledAttr = el.hasAttribute('disabled') && el.getAttribute('disabled') !== 'false';
        const ariaDisabled = el.getAttribute('aria-disabled');
        return cls.includes('disabled') || disabledAttr || ariaDisabled === 'true';
      }, nextHandle);

      if (isDisabled) {
        console.log('🛑 "Next" is disabled. End of pagination.');
        break;
      }

      console.log('➡️ Moving to next page...');
      await Promise.allSettled([
        dom.waitForNavigation({ waitUntil: 'networkidle0', timeout: 15000 }),
        nextHandle.click(),
      ]);

      // Wait for actual list change even if no navigation fired
      try {
        await waitForListChange(dom);
      } catch (_) {
        // If we couldn’t detect change, assume we’re done to avoid infinite loop
        console.log('⚠️ Did not detect list change after "Next". Stopping.');
        break;
      }

      currentPage++;
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
