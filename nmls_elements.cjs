// Requires:
// npm install selenium-webdriver undetected-chromedriver cheerio googleapis

const { Builder, By } = require('selenium-webdriver');
const chrome = require('selenium-webdriver/chrome');
const uc = require('undetected-chromedriver');
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
  let driver;

  try {
    // Launch undetected Chrome
    const options = new chrome.Options();
    options.addArguments('--no-sandbox');
    options.addArguments('--disable-dev-shm-usage');
    options.addArguments('--disable-blink-features=AutomationControlled');
    options.addArguments('--disable-setuid-sandbox');
    options.addArguments('--window-size=1366,768');

    driver = await uc.Builder()
      .forBrowser('chrome')
      .setChromeOptions(options)
      .build();

    await driver.get(url);

    // Wait for body element
    await driver.findElement(By.css('body'));

    const html = await driver.getPageSource();
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
    console.error('❌ Error during page inspection:', err);
    return [];
  } finally {
    if (driver) {
      await driver.quit();
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