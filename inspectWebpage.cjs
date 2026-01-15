// inspectWebpage.cjs (Stage 2: evaluation + filtration with stealth hardening)
// Requires:
// npm install puppeteer-extra puppeteer-extra-plugin-stealth cheerio googleapis

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const cheerio = require('cheerio');
const fs = require('fs');
const { google } = require('googleapis');

puppeteer.use(StealthPlugin());

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
// Delay helper
// =========================
async function delay(page, ms) {
  if (typeof page.waitForTimeout === 'function') {
    await page.waitForTimeout(ms);
  } else {
    await new Promise(resolve => setTimeout(resolve, ms));
  }
}

// =========================
// Inspect + Parse Page
// =========================
async function inspectAndParse(page, url, seen) {
  try {
    console.log(`🌐 Visiting ${url}`);
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 120000 });
    await delay(page, 15000);

    const html = await page.content();
    if (
      html.includes('403 Forbidden') ||
      html.includes('Access Denied') ||
      html.toLowerCase().includes('forbidden')
    ) {
      throw new Error('Blocked by target website (403)');
    }

    const $ = cheerio.load(html);

    // (1) FILTER: auction-relevant elements ONLY
    const relevantElements = [];
    const auctionTextRegex =
      /(\$\d{1,3}(,\d{3})+)|(\bAPN\b)|(\bParcel\b)|(\bAuction\b)|(\bCase\b)/i;

    $('*').each((_, el) => {
      const tag = el.tagName;
      const text = $(el).text().replace(/\s+/g, ' ').trim();
      const attrs = el.attribs || {};
      if (text && auctionTextRegex.test(text)) {
        relevantElements.push({ sourceUrl: url, tag, text, attrs });
      }
    });

    // (2) CARD-BASED AUCTION PARSER
    const parsedRows = [];
    $('div').each((_, container) => {
      const blockText = $(container).text().replace(/\s+/g, ' ').trim();
      if (!blockText.includes('Auction Type')) return;

      const extract = label => {
        const regex = new RegExp(`${label}\\s*:?\\s*([^\\n$]+)`, 'i');
        const m = blockText.match(regex);
        return m ? m[1].trim() : '';
      };

      const auctionStatus =
        blockText.includes('Redeemed')
          ? 'Redeemed'
          : blockText.includes('Auction Sold')
          ? 'Sold'
          : 'Active';

      const openingBidMatch = blockText.match(/Opening Bid:\s*\$[\d,]+\.\d{2}/i);
      const openingBid = openingBidMatch
        ? openingBidMatch[0].replace(/Opening Bid:/i, '').trim()
        : '';

      const assessedValueMatch = blockText.match(/Assessed Value:\s*\$[\d,]+\.\d{2}/i);
      const assessedValue = assessedValueMatch
        ? assessedValueMatch[0].replace(/Assessed Value:/i, '').trim()
        : '';

      const auctionDateMatch = blockText.match(/Date\/Time:\s*([0-9]{2}\/[0-9]{2}\/[0-9]{4}(?:\s+[0-9]{1,2}:[0-9]{2}\s*(?:AM|PM)?\s*ET)?)/i);
      const auctionDate = auctionDateMatch ? auctionDateMatch[1].trim() : '';

      const salePriceMatch = blockText.match(/Amount:\s*\$[\d,]+\.\d{2}/i);
      const salePrice = salePriceMatch
        ? salePriceMatch[0].replace(/Amount:/i, '').trim()
        : '';

      const parcelLink = $(container).find('a').first();
      const parcelId = parcelLink.text().trim();

      const caseNumber = extract('Case #');

      if (!parcelId || !openingBid || !caseNumber) return;

      const open = parseCurrency(openingBid);
      const assess = parseCurrency(assessedValue);
      const surplus = assess !== null && open !== null ? assess - open : null;

      const baseUrl = url.split('&page=')[0];
      const dedupeKey = `${baseUrl}|${caseNumber}|${parcelId}`;
      if (seen.has(dedupeKey)) return;
      seen.add(dedupeKey);

      parsedRows.push({
        sourceUrl: baseUrl,
        auctionStatus,
        auctionType: extract('Auction Type'),
        caseNumber,
        parcelId,
        propertyAddress: extract('Property Address'),
        openingBid,
        assessedValue,
        auctionDate,
        salePrice,
        surplus,
        meetsMinimumSurplus: surplus !== null && surplus >= MIN_SURPLUS ? 'Yes' : 'No',
      });
    });

    console.log(`📦 Elements: ${relevantElements.length} | Auctions: ${parsedRows.length}`);
    return { relevantElements, parsedRows };
  } catch (err) {
    console.error(`❌ Error on ${url}:`, err.message);
    return { relevantElements: [], parsedRows: [], error: { url, message: err.message } };
  }
}

// =========================
// Scrape all pages (URL-based pagination)
// =========================
async function scrapeAllPages(browser, startUrl) {
  const page = await browser.newPage();
  page.setViewport({ width: 1280, height: 800 });
  await page.emulateTimezone('America/New_York');

  const allElements = [];
  const allRows = [];
  const errors = [];
  const seen = new Set();

  let pageIndex = 1;

  while (true) {
    const currentUrl = pageIndex === 1 ? startUrl : `${startUrl}&page=${pageIndex}`;
    console.log(`🌐 Visiting ${currentUrl}`);

    const { relevantElements, parsedRows, error } = await inspectAndParse(page, currentUrl, seen);
    allElements.push(...relevantElements);
    allRows.push(...parsedRows);
    if (error) errors.push(error);

    if (!relevantElements.length && !parsedRows.length) {
      console.log("⛔ No more pages");
      break;
    }

    pageIndex++;
    if (pageIndex > 50) {
      console.log("⚠️ Reached page limit, stopping.");
      break;
    }
    console.log(`➡️ Moving to page ${pageIndex}`);
  }

  await page.close();
  return { allElements, allRows, errors };
}

// =========================
// MAIN
// =========================
(async () => {
  console.log('📥 Loading URLs...');
  const urls = await loadTargetUrls();

  const browser = await puppeteer.launch({
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-blink-features=AutomationControlled',
      '--ignore-certificate-errors',
    ],
  });

  const allElements = [];
  const allRows = [];
  const errors = [];

  for (const url of urls) {
    const result = await scrapeAllPages(browser, url);
    allElements.push(...result.allElements);
    allRows.push(...result.allRows);
    errors.push(...result.errors);
  }

  await browser.close();

  fs.writeFileSync(OUTPUT_ELEMENTS_FILE, JSON.stringify(allElements, null, 2));
  fs.writeFileSync(OUTPUT_ROWS_FILE, JSON.stringify(allRows, null, 2));
  if (errors.length) {
    fs.writeFileSync(OUTPUT_ERRORS_FILE, JSON.stringify(errors, null, 2));
    console.log(`⚠️ Saved ${errors.length} errors → ${OUTPUT_ERRORS_FILE}`);
  }

  const summary = {
    totalUrls: urls.length,
    totalElements: allElements.length,
    totalRowsFinal: allRows.length,
    errorsCount: errors.length,
    surplusAboveThreshold: allRows.filter(r => r.meetsMinimumSurplus === 'Yes').length,
    surplusBelowThreshold: allRows.filter(r => r.meetsMinimumSurplus === 'No').length,
    blanks: {
      auctionDateBlank: allRows.filter(r => !r.auctionDate).length,
      salePriceBlank: allRows.filter(r => !r.salePrice).length,
    },
  };

  fs.writeFileSync(OUTPUT_SUMMARY_FILE, JSON.stringify(summary, null, 2));

  console.log(`✅ Saved ${allElements.length} elements → ${OUTPUT_ELEMENTS_FILE}`);
  console.log(`✅ Saved ${allRows.length} auctions → ${OUTPUT_ROWS_FILE}`);
  console.log(`📊 Saved summary → ${OUTPUT_SUMMARY_FILE}`);
  console.log('🏁 Done');
})();