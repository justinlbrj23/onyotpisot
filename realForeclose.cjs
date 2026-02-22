// Requires:
// npm install puppeteer cheerio googleapis

const puppeteer = require('puppeteer');
const cheerio = require('cheerio');
const { google } = require('googleapis');

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = './service-account.json';
const SPREADSHEET_ID = '1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA';
const SHEET_RANGE = 'Palm Beach - Taxdeed!A:I';

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
// HELPERS
// =========================
function clean(text) {
  if (!text) return '';
  return text.replace(/\s+/g, ' ').trim();
}

function extractText($root, selector) {
  const el = $root.find(selector).first();
  if (!el.length) return '';
  return clean(el.text());
}

// =========================
// SCRAPER WITH PAGINATION
// =========================
async function scrapeAllPages(url) {
  let browser;
  const allResults = [];

  try {
    browser = await puppeteer.launch({
      headless: true, // DO NOT use 'new' in CI
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

    await page.goto(url, {
      waitUntil: 'domcontentloaded',
      timeout: 120000,
    });

    let currentPage = 1;

    while (currentPage <= MAX_PAGES) {
      console.log(`📄 Scraping page ${currentPage}...`);

      await page.waitForSelector('div[aid]', { timeout: 60000 });

      const html = await page.content();
      const $ = cheerio.load(html);

      const pageResults = [];

      $('div[aid]').each((_, item) => {
        const $item = $(item);

        const record = {
          caseNumber: extractText($item, "th:contains('Cause Number:') + td"),
          assessedValue: extractText($item, "th:contains('Adjudged Value:') + td"),
          openingBid: extractText($item, "th:contains('Est. Min. Bid:') + td"),
          parcelId: extractText($item, "th:contains('Account Number:') + td"),
          streetAddress: extractText($item, "th:contains('Property Address:') + td"),
          cityStateZip: extractText($item, "tr:nth-of-type(8) td"),
          status: extractText($item, "div.ASTAT_MSGA"),
          soldAmount: extractText($item, "div.ASTAT_MSGD"),
        };

        pageResults.push(record);
      });

      console.log(`   ➜ Found ${pageResults.length} auctions`);

      allResults.push(...pageResults);

      // =========================
      // PAGINATION DETECTION
      // =========================
      const nextButton = await page.$(
        "a:contains('Next'), input[value='Next'], .pagination-next"
      );

      if (!nextButton) {
        console.log('🛑 No more pages detected.');
        break;
      }

      const isDisabled = await page.evaluate(el =>
        el.classList.contains('disabled') || el.disabled === true,
        nextButton
      );

      if (isDisabled) {
        console.log('🛑 Next button disabled. End of pagination.');
        break;
      }

      console.log('➡️ Moving to next page...');

      await Promise.all([
        page.waitForNavigation({ waitUntil: 'domcontentloaded' }),
        nextButton.click(),
      ]);

      currentPage++;
    }

    return allResults;
  } catch (err) {
    console.error('❌ Scraping error:', err);
    return [];
  } finally {
    if (browser) {
      await browser.close();
    }
  }
}

// =========================
// GOOGLE SHEETS APPEND
// =========================
async function appendToSheet(data) {
  if (!data.length) {
    console.warn('⚠️ No auction data found.');
    return;
  }

  const timestamp = new Date().toISOString();

  const values = data.map(r => [
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
// MAIN
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
