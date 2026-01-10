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

const OUTPUT_FILE = 'raw-scrape.json';

// =========================
// GOOGLE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
});

const sheets = google.sheets({ version: 'v4', auth });

// =========================
// Load URLs from Google Sheets
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
// FUNCTION: Inspect Web Page
// (SAME LOGIC AS REFERENCE SCRIPT)
// =========================
async function inspectPage(browser, url) {
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

    // -------------------------
    // Navigate + WAF wait
    // -------------------------
    console.log(`🌐 Visiting ${url}`);
    await page.goto(url, {
      waitUntil: 'networkidle2',
      timeout: 120000,
    });

    await new Promise(r => setTimeout(r, 8000));

    const html = await page.content();

    // -------------------------
    // Detect hard block early
    // -------------------------
    if (
      html.includes('403 Forbidden') ||
      html.includes('Access Denied') ||
      html.toLowerCase().includes('forbidden')
    ) {
      throw new Error('Blocked by target website (403)');
    }

    // -------------------------
    // HTML → Cheerio → Elements
    // -------------------------
    const $ = cheerio.load(html);
    const elements = [];

    $('*').each((_, el) => {
      const tag = el.tagName;
      const text = $(el).text().replace(/\s+/g, ' ').trim();
      const attrs = el.attribs || {};

      if (text) {
        elements.push({
          sourceUrl: url,
          tag,
          text,
          attrs,
        });
      }
    });

    console.log(`📦 Parsed ${elements.length} elements`);
    return elements;
  } catch (err) {
    console.error(`❌ Error inspecting ${url}:`, err.message);
    return [];
  } finally {
    await page.close();
  }
}

// =========================
// MAIN
// =========================
(async () => {
  console.log('📥 Loading URLs from Google Sheets...');
  const urls = await loadTargetUrls();

  if (!urls.length) {
    console.error('❌ No URLs found in sheet');
    process.exit(1);
  }

  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-blink-features=AutomationControlled',
    ],
  });

  const allResults = [];

  for (const url of urls) {
    const results = await inspectPage(browser, url);
    allResults.push(...results);
  }

  await browser.close();

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(allResults, null, 2));
  console.log(`✅ Saved ${allResults.length} elements to ${OUTPUT_FILE}`);
  console.log('🏁 Done');
})();