// Requires:
// npm install puppeteer cheerio googleapis

const puppeteer = require('puppeteer');
const cheerio = require('cheerio');
const { google } = require('googleapis');

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = './service-account.json';
const SPREADSHEET_ID = '1CAEdjXisPmgAHmv3qo3y1LBYktQftLKHk-LK04_oKes';
const SHEET_RANGE = 'Sheet1!A:D'; 
const TARGET_URL = 'https://www.nmlsconsumeraccess.org/';

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
      ],
    });

    const page = await browser.newPage();
    page.setDefaultNavigationTimeout(120000);

    await page.goto(url, {
      waitUntil: 'domcontentloaded',
      timeout: 120000,
    });

    await page.waitForSelector('body', { timeout: 60000 });

    const html = await page.content();
    const $ = cheerio.load(html);

    const elements = [];

    $('*').each((_, el) => {
      const tag = el.tagName;
      const text = $(el).text().replace(/\s+/g, ' ').trim();
      const attrs = el.attribs || {};

      if (text) {
        elements.push({
          tag,
          text,
          attrs,
        });
      }
    });

    return elements;
  } catch (err) {
    console.error('❌ Error during page inspection:', err);
    return [];
  } finally {
    if (browser) {
      await browser.close();
    }
  }
}

// =========================
// FUNCTION: Append to Google Sheets
// =========================
async function appendToSheet(results) {
  if (!results.length) {
    console.warn('⚠️ No data to write to Google Sheets.');
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
    // Check if the sheet already has data
    const existing = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: SHEET_RANGE,
    });

    // Add header row if sheet is empty
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
    console.error('❌ Error writing to Google Sheets:', err);
  }
}

// =========================
// MAIN EXECUTION
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