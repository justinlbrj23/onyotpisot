// inspectWebpage.cjs
// Requires:
// npm install puppeteer cheerio googleapis

const puppeteer = require('puppeteer');
const cheerio = require('cheerio');
const { google } = require('googleapis');

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = './service-account.json';
const SPREADSHEET_ID = '12qESHoxzkSXwUc5Pa1gAzt8-hIw7QyiExkIh6UeDCMM';
const SHEET_RANGE = 'Property Appraiser!A:D';
const TARGET_URL =
  'https://king.wa.realforeclose.com/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE=09/10/2025';

// =========================
// GOOGLE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ['https://www.googleapis.com/auth/spreadsheets'],
});

const sheets = google.sheets({ version: 'v4', auth });

// =========================
// FUNCTION: Inspect Web Page
// =========================
async function inspectPage(url) {
  let browser;

  try {
    browser = await puppeteer.launch({
      headless: 'new',
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-blink-features=AutomationControlled',
      ],
    });

    const page = await browser.newPage();
    page.setDefaultNavigationTimeout(120000);

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
    // Navigate + wait for WAF
    // -------------------------
    await page.goto(url, {
      waitUntil: 'networkidle2',
      timeout: 120000,
    });

    // Cloudflare / WAF delay
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

    const $ = cheerio.load(html);
    const elements = [];

    $('*').each((_, el) => {
      const tag = el.tagName;
      const text = $(el).text().replace(/\s+/g, ' ').trim();
      const attrs = el.attribs || {};

      if (text) {
        elements.push({ tag, text, attrs });
      }
    });

    return elements;
  } catch (err) {
    console.error('❌ Error during page inspection:', err.message);
    return [];
  } finally {
    if (browser) await browser.close();
  }
}

// =========================
// FUNCTION: Append to Google Sheets
// =========================
async function appendToSheet(results) {
  if (!results.length) {
    console.warn('⚠️ No valid data to write to Google Sheets.');
    return;
  }

  const timestamp = new Date().toISOString();

  const values = results.map(r => {
    const attrString = Object.entries(r.attrs)
      .map(([k, v]) => `${k}=${v}`)
      .join('; ');
    return [timestamp, r.tag, r.text, attrString];
  });

  try {
    const existing = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: SHEET_RANGE,
    });

    if (!existing.data.values || existing.data.values.length === 0) {
      values.unshift(['Timestamp', 'Tag', 'Text', 'Attributes']);
    }

    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: SHEET_RANGE,
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values },
    });

    console.log(`✅ Successfully appended ${values.length} rows.`);
  } catch (err) {
    console.error('❌ Error writing to Google Sheets:', err.message);
  }
}

// =========================
// MAIN
// =========================
(async () => {
  console.log('🔍 Inspecting webpage...');
  const results = await inspectPage(TARGET_URL);

  console.log(`📦 Total elements parsed: ${results.length}`);
  console.log('🧪 Sample output:', results.slice(0, 5));

  console.log('📤 Writing results to Google Sheets...');
  await appendToSheet(results);

  console.log('🏁 Done.');
})();