// inspectWebpage.cjs
// Requires: npm install puppeteer cheerio googleapis readline

const puppeteer = require('puppeteer');
const cheerio = require('cheerio');
const { google } = require('googleapis');
const readline = require('readline');

// === GOOGLE OAUTH2 SETUP ===
const CLIENT_ID = process.env.CLIENT_ID || "YOUR_CLIENT_ID";
const CLIENT_SECRET = process.env.CLIENT_SECRET || "YOUR_CLIENT_SECRET";
const REDIRECT_URI = "http://localhost";

const oauth2Client = new google.auth.OAuth2(
  CLIENT_ID,
  CLIENT_SECRET,
  REDIRECT_URI
);

// Use refresh token from secrets
if (process.env.REFRESH_TOKEN) {
  oauth2Client.setCredentials({
    refresh_token: process.env.REFRESH_TOKEN
  });
}

// === FUNCTION: Get Refresh Token (local only) ===
async function getRefreshToken() {
  const SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets'
  ];
  const authUrl = oauth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: SCOPES,
  });
  console.log('Authorize this app by visiting this URL:\n', authUrl);

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  return new Promise((resolve, reject) => {
    rl.question('\nEnter the code from that page here: ', async (code) => {
      try {
        const { tokens } = await oauth2Client.getToken(code);
        console.log('\nAccess Token:', tokens.access_token);
        console.log('Refresh Token:', tokens.refresh_token);
        rl.close();
        resolve(tokens.refresh_token);
      } catch (err) {
        console.error('Error retrieving tokens:', err);
        rl.close();
        reject(err);
      }
    });
  });
}

// === FUNCTION: Inspect Page ===
async function inspectPage(url) {
  const browser = await puppeteer.launch({
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage'
    ]
  });
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  try {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 120000 });
    await page.waitForSelector('body', { timeout: 60000 });
    const html = await page.content();
    const $ = cheerio.load(html);

    const elements = [];
    $('*').each((i, el) => {
      const tag = el.tagName;
      const text = $(el).text().trim();
      const attrs = el.attribs;
      if (text) {
        elements.push({ tag, text, attrs });
      }
    });

    await browser.close();
    return elements;
  } catch (err) {
    console.error('Error during page inspection:', err);
    await browser.close();
    return [];
  }
}

// === FUNCTION: Append to Google Sheets ===
async function appendToSheet(results) {
  const sheets = google.sheets({ version: 'v4', auth: oauth2Client });

  const spreadsheetId = "12qESHoxzkSXwUc5Pa1gAzt8-hIw7QyiExkIh6UeDCMM";
  const range = "Property Appraiser!A:D"; // four columns: Timestamp, Tag, Text, Attributes

  // Flatten attributes into a string (e.g. class=..., id=...)
  const timestamp = new Date().toISOString();
  const values = results.map(r => {
    const attrString = r.attrs
      ? Object.entries(r.attrs).map(([k, v]) => `${k}=${v}`).join('; ')
      : '';
    return [timestamp, r.tag, r.text, attrString];
  });

  try {
    // First, check if sheet is empty
    const existing = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range
    });

    // If empty, add header row first
    if (!existing.data.values || existing.data.values.length === 0) {
      values.unshift(["Timestamp", "Tag", "Text", "Attributes"]);
    }

    await sheets.spreadsheets.values.append({
      spreadsheetId,
      range,
      valueInputOption: 'RAW',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values }
    });
    console.log(`✅ Appended ${values.length} rows to Google Sheet`);
  } catch (err) {
    console.error('Error writing to Google Sheets:', err);
  }
}

// === MAIN EXECUTION ===
(async () => {
  const url = 'https://www.miamidadepa.gov/pa/real-estate/property-search.page';
  const results = await inspectPage(url);

  console.log(`Total elements parsed: ${results.length}`);
  console.log('Sample output:', results.slice(0, 5));

  await appendToSheet(results);
})();