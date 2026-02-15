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


// =========================
// GOOGLE AUTH
// =========================

const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: ['https://www.googleapis.com/auth/spreadsheets'],
});

const sheets = google.sheets({
  version: 'v4',
  auth,
});


// =========================
// FUNCTION: Inspect Web Page
// =========================

async function inspectPage(url) {

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

    await page.goto(url, {

      waitUntil: 'networkidle2',
      timeout: 60000,

    });


    await page.waitForSelector('body');


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

  }

  catch (err) {

    console.error('❌ Error during page inspection:', err);

    return [];

  }

  finally {

    if (browser) {

      await browser.close();

    }

  }

}

// =========================
// FUNCTION: Perform Search (auto-detect input)
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

    // =========================
    // FETCH + ANALYZE STAGE
    // =========================
    console.log("📥 Fetching DOM to analyze inputs...");
    const html = await page.content();
    const $ = cheerio.load(html);

    // Find candidate input fields
    const inputs = [];
    $('input').each((_, el) => {
      const attrs = el.attribs || {};
      inputs.push(attrs);
    });

    console.log("🔎 Candidate inputs:", inputs);

    // Heuristic: pick the input with placeholder mentioning search terms
    let targetSelector = null;
    for (const attrs of inputs) {
      if (attrs.placeholder && attrs.placeholder.toLowerCase().includes('search')) {
        if (attrs.id) targetSelector = `#${attrs.id}`;
        else if (attrs.name) targetSelector = `[name="${attrs.name}"]`;
        break;
      }
    }

    if (!targetSelector) {
      throw new Error("Could not auto-detect search input field");
    }

    console.log(`🎯 Using selector: ${targetSelector}`);

    // =========================
    // INTERACTION STAGE
    // =========================
    await page.waitForSelector(targetSelector, { timeout: 15000 });

    // Clear any existing text
    await page.evaluate(sel => {
      const input = document.querySelector(sel);
      if (input) input.value = '';
    }, targetSelector);

    // Type ZIP code and press Enter
    await page.type(targetSelector, zipcode);
    await page.keyboard.press('Enter');

    // Wait for results container (adjust heuristics if needed)
    await page.waitForSelector('#searchResults, #searchResultsContainer, .searchResults', { timeout: 20000 });

    console.log("📥 Fetching updated page content...");
    const updatedHtml = await page.content();
    const $$ = cheerio.load(updatedHtml);

    const results = [];
    $$('#searchResults .resultRow, #searchResultsContainer .resultRow, .searchResults .resultRow').each((_, el) => {
      const name = $$(el).find('.resultName').text().trim();
      const details = $$(el).find('.resultDetails').text().trim();
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


  const values = results.map(r => {

    const attrString = Object.entries(r.attrs)

      .map(([k, v]) => `${k}=${v}`)

      .join('; ');


    return [

      timestamp,

      r.tag,

      r.text,

      attrString,

    ];

  });


  try {

    const existing = await sheets.spreadsheets.values.get({

      spreadsheetId: SPREADSHEET_ID,

      range: SHEET_RANGE,

    });


    if (!existing.data.values || existing.data.values.length === 0) {

      values.unshift([

        'Timestamp',

        'Tag',

        'Text',

        'Attributes',

      ]);

    }


    await sheets.spreadsheets.values.append({

      spreadsheetId: SPREADSHEET_ID,

      range: SHEET_RANGE,

      valueInputOption: 'RAW',

      insertDataOption: 'INSERT_ROWS',

      requestBody: {

        values,

      },

    });


    console.log(`✅ Appended ${values.length} rows.`);

  }

  catch (err) {

    console.error('❌ Sheets error:', err);

  }

}


// =========================
// MAIN
// =========================
(async () => {
  // Step 1: Inspect the page
  console.log('🔍 Inspecting webpage...');
  const inspected = await inspectPage(TARGET_URL);
  console.log(`📦 Parsed: ${inspected.length}`);
  console.log('🧪 Sample:', inspected.slice(0, 5));
  console.log('📤 Writing inspected data to Sheets...');
  await appendToSheet(inspected);

  // Step 2: Perform a search after inspection
  console.log('🔍 Performing search...');
  const searched = await searchPage(TARGET_URL, '33122'); // sample ZIP
  console.log(`📦 Found ${searched.length} search results`);
  console.log('🧪 Sample:', searched.slice(0, 5));
  console.log('📤 Writing search results to Sheets...');
  await appendToSheet(searched);

  console.log('🏁 Done.');
})();