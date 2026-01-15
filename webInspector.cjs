// webInspector.cjs (Stage 1: DOM reconnaissance with pagination)
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
const OUTPUT_ERRORS_FILE = 'errors.json';

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
// Inspect + Parse Page (DOM scan only)
// =========================
async function inspectPage(page, url) {
  try {
    console.log(`🌐 Visiting ${url}`);
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 120000 });
    await new Promise(r => setTimeout(r, 5000));

    const html = await page.content();
    if (
      html.includes('403 Forbidden') ||
      html.includes('Access Denied') ||
      html.toLowerCase().includes('forbidden')
    ) {
      throw new Error('Blocked by target website (403)');
    }

    const $ = cheerio.load(html);
    const elements = [];

    $('*').each((_, el) => {
      const tag = el.tagName;
      const text = $(el).text().replace(/\s+/g, ' ').trim();
      const attrs = el.attribs || {};
      if (text || Object.keys(attrs).length) {
        elements.push({ sourceUrl: url, tag, text, attrs });
      }
    });

    console.log(`📦 Collected ${elements.length} DOM elements`);
    return { elements };
  } catch (err) {
    console.error(`❌ Error on ${url}:`, err.message);
    return { elements: [], error: { url, message: err.message } };
  }
}

// =========================
// Scrape all pages (pagination)
// =========================
async function scrapeAllPages(browser, startUrl) {
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  const allElements = [];
  const errors = [];

  let currentUrl = startUrl;
  let pageIndex = 1;

  while (true) {
    const { elements, error } = await inspectPage(page, currentUrl);
    allElements.push(...elements);
    if (error) errors.push(error);

    // Detect "Next Page" button
    const nextButton = await page.$('a[aria-label="Next"], a.pagination-next, button.next');
    if (!nextButton) {
      console.log("⛔ No more pages");
      break;
    }

    await Promise.all([
      nextButton.click(),
      page.waitForNavigation({ waitUntil: "networkidle2" })
    ]);

    currentUrl = page.url();
    pageIndex++;
    console.log(`➡️ Moving to page ${pageIndex}`);
  }

  await page.close();
  return { allElements, errors };
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
      '--ignore-certificate-errors',
    ],
  });

  const allElements = [];
  const errors = [];

  for (const url of urls) {
    const result = await scrapeAllPages(browser, url);
    allElements.push(...result.allElements);
    errors.push(...result.errors);
  }

  await browser.close();

  // Write artifacts
  fs.writeFileSync(OUTPUT_ELEMENTS_FILE, JSON.stringify(allElements, null, 2));
  if (errors.length) {
    fs.writeFileSync(OUTPUT_ERRORS_FILE, JSON.stringify(errors, null, 2));
    console.log(`⚠️ Saved ${errors.length} errors → ${OUTPUT_ERRORS_FILE}`);
  }

  console.log(`✅ Saved ${allElements.length} DOM elements → ${OUTPUT_ELEMENTS_FILE}`);
  console.log('🏁 Done');
})();