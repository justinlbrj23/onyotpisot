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

  console.log('🔍 Inspecting webpage...');


  const results = await inspectPage(TARGET_URL);


  console.log(`📦 Parsed: ${results.length}`);


  console.log('🧪 Sample:', results.slice(0, 5));


  console.log('📤 Writing to Sheets...');


  await appendToSheet(results);


  console.log('🏁 Done.');

})();