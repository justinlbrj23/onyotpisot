// Requires:
// npm install puppeteer-real-browser cheerio googleapis

const { connect } = require('puppeteer-real-browser');
const cheerio = require('cheerio');
const { google } = require('googleapis');

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = './service-account.json';
const SPREADSHEET_ID = '1CAEdjXisPmgAHmv3qo3y1LBYktQftLKHk-LK04_oKes';
const SHEET_RANGE = 'Sheet1!A:D';
const TARGET_URL = 'https://www.nmlsconsumeraccess.org/';
const SAMPLE_ZIP = '33122'; // Example ZIP code

// =========================
// GOOGLE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ['https://www.googleapis.com/auth/spreadsheets'],
});
const sheets = google.sheets({ version: 'v4', auth });

// =========================
// FUNCTION: Perform Search
// =========================
async function searchPage(url, zipcode) {
  let browser;
  try {
    const connection = await connect({
      headless: false,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
      ],
      customConfig: {},
      turnstile: true,
      connectOption: {},
      disableXvfb: false,
    });

    browser = connection.browser;
    const page = connection.page;

    console.log("🌐 Navigating...");
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });

    // Wait for the search input field
    await page.waitForSelector('#searchText', { timeout: 15000 });

    // Type ZIP code and press Enter
    await page.type('#searchText', zipcode);
    await page.keyboard.press('Enter');

    // Wait for results container to appear
    await page.waitForSelector('#searchResults', { timeout: 20000 });

    // Extract results HTML
    const html = await page.content();
    const $ = cheerio.load(html);

    const results = [];
    $('#searchResults .resultRow').each((_, el) => {
      const name = $(el).find('.resultName').text().trim();
      const details = $(el).find('.resultDetails').text().trim();
      results.push({ name, details });
    });

    console.log(`📦 Found ${results.length} results for ZIP ${zipcode}`);
    console.log('🧪 Sample:', results.slice(0, 5));

    return results;
  } catch (err) {
    console.error('❌ Error during search:', err);
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
    console.warn('⚠️ No data to write.');
    return;
  }

  const timestamp = new Date().toISOString();
  const values = results.map(r => [timestamp, r.name, r.details, '']);

  try {
    const existing = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: SHEET_RANGE,
    });

    if (!existing.data.values || existing.data.values.length === 0) {
      values.unshift(['Timestamp', 'Name', 'Details', 'Attributes']);
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
    console.error('❌ Sheets error:', err);
  }
}

// =========================
// MAIN
// =========================
(async () => {
  console.log('🔍 Performing search...');
  const results = await searchPage(TARGET_URL, SAMPLE_ZIP);

  console.log('📤 Writing to Sheets...');
  await appendToSheet(results);

  console.log('🏁 Done.');
})();