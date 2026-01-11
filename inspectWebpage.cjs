// inspectWebpage.cjs
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

const MIN_SURPLUS = 25000;

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

  return (res.data.values || [])
    .flat()
    .map(v => v.trim())
    .filter(v => v.startsWith('http'));
}

// =========================
// Currency parser
// =========================
function parseCurrency(str) {
  if (!str) return null;
  const n = parseFloat(str.replace(/[^0-9.-]/g, ''));
  return isNaN(n) ? null : n;
}

// =========================
// Inspect + Parse Page
// =========================
async function inspectAndParse(browser, url) {
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  try {
    // -------------------------
    // Anti-bot hardening
    // -------------------------
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
        'AppleWebKit/537.36 (KHTML, like Gecko) ' +
        'Chrome/120.0.0.0 Safari/537.36'
    );

    await page.setExtraHTTPHeaders({
      'Accept-Language': 'en-US,en;q=0.9',
      Accept:
        'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    });

    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, 'webdriver', {
        get: () => false,
      });
    });

    console.log(`🌐 Visiting ${url}`);
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 120000 });
    await new Promise(r => setTimeout(r, 8000));

    const html = await page.content();

    if (
      html.includes('403 Forbidden') ||
      html.includes('Access Denied') ||
      html.toLowerCase().includes('forbidden')
    ) {
      throw new Error('Blocked by target website (403)');
    }

    const $ = cheerio.load(html);

    // ==================================================
    // (1) FILTER: auction-relevant elements ONLY
    // ==================================================
    const relevantElements = [];
    const auctionTextRegex =
      /(\$\d{1,3}(,\d{3})+)|(\bAPN\b)|(\bParcel\b)|(\bAuction\b)|(\bCase\b)/i;

    $('*').each((_, el) => {
      const tag = el.tagName;
      const text = $(el).text().replace(/\s+/g, ' ').trim();
      const attrs = el.attribs || {};

      if (text && auctionTextRegex.test(text)) {
        relevantElements.push({
          sourceUrl: url,
          tag,
          text,
          attrs,
        });
      }
    });

    // ==================================================
    // (5) CARD-BASED AUCTION PARSER (NO TABLES)
    // ==================================================
    const parsedRows = [];

    $('div').each((_, container) => {
      const blockText = $(container)
        .text()
        .replace(/\s+/g, ' ')
        .trim();

      // Heuristic: auction cards always contain Auction Type
      if (!blockText.includes('Auction Type')) return;

      const extract = label => {
        const regex = new RegExp(`${label}\\s*:?\\s*([^A-Z$]+)`, 'i');
        const m = blockText.match(regex);
        return m ? m[1].trim() : '';
      };

      const auctionStatus =
        blockText.includes('Redeemed')
          ? 'Redeemed'
          : blockText.includes('Auction Sold')
          ? 'Sold'
          : 'Active';

      const openingBidMatch = blockText.match(/\$[\d,]+\.\d{2}/);
      const openingBid = openingBidMatch ? openingBidMatch[0] : '';

      const assessedValueMatch =
        blockText.match(/Assessed Value:\s*\$[\d,]+\.\d{2}/i);
      const assessedValue = assessedValueMatch
        ? assessedValueMatch[0]
            .replace(/Assessed Value:/i, '')
            .trim()
        : '';

      const parcelLink = $(container).find('a').first();
      const parcelId = parcelLink.text().trim();

      if (!parcelId || !openingBid) return;

      const open = parseCurrency(openingBid);
      const assess = parseCurrency(assessedValue);
      const surplus =
        open !== null && assess !== null ? assess - open : null;

      parsedRows.push({
        sourceUrl: url,
        auctionStatus,
        auctionType: extract('Auction Type'),
        caseNumber: extract('Case #'),
        parcelId,
        propertyAddress: extract('Property Address'),
        openingBid,
        assessedValue,
        surplus,
        meetsMinimumSurplus:
          surplus !== null && surplus >= MIN_SURPLUS ? 'Yes' : 'No',
      });
    });

    console.log(
      `📦 Elements: ${relevantElements.length} | Auctions: ${parsedRows.length}`
    );

    return { relevantElements, parsedRows };
  } catch (err) {
    console.error(`❌ Error on ${url}:`, err.message);
    return { relevantElements: [], parsedRows: [] };
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

  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-blink-features=AutomationControlled',
    ],
  });

  const allElements = [];
  const allRows = [];

  for (const url of urls) {
    const { relevantElements, parsedRows } =
      await inspectAndParse(browser, url);

    allElements.push(...relevantElements);
    allRows.push(...parsedRows);
  }

  await browser.close();

  fs.writeFileSync(
    OUTPUT_ELEMENTS_FILE,
    JSON.stringify(allElements, null, 2)
  );
  fs.writeFileSync(
    OUTPUT_ROWS_FILE,
    JSON.stringify(allRows, null, 2)
  );

  console.log(
    `✅ Saved ${allElements.length} elements → ${OUTPUT_ELEMENTS_FILE}`
  );
  console.log(
    `✅ Saved ${allRows.length} auctions → ${OUTPUT_ROWS_FILE}`
  );
  console.log('🏁 Done');
})();