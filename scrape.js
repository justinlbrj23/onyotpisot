import { chromium } from 'playwright';
import fs from 'fs';
import { google } from 'googleapis';
import readline from 'readline';

// Google Sheets Config
const SCOPES = ['https://www.googleapis.com/auth/spreadsheets'];
const SHEET_ID = '1VUB2NdGSY0l3tuQAfkz8QV2XZpOj2khCB69r5zU1E5A';
const SHEET_NAME = 'CAPE CORAL FINAL';
const RANGE = 'R2:R';

// Authorize Google Sheets API
const authorizeGoogle = async () => {
  const credentials = JSON.parse(fs.readFileSync('credentials.json'));
  const { client_secret, client_id, redirect_uris } = credentials.installed;
  const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, redirect_uris[0]);

  if (fs.existsSync('token.json')) {
    oAuth2Client.setCredentials(JSON.parse(fs.readFileSync('token.json')));
    return oAuth2Client;
  }

  const authUrl = oAuth2Client.generateAuthUrl({ access_type: 'offline', scope: SCOPES });
  console.log('Authorize this app by visiting this URL:', authUrl);

  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  const code = await new Promise(resolve => rl.question('Enter the code from that page here: ', resolve));
  rl.close();

  const { tokens } = await oAuth2Client.getToken(code);
  oAuth2Client.setCredentials(tokens);
  fs.writeFileSync('token.json', JSON.stringify(tokens));
  return oAuth2Client;
};

// Fetch URLs + row index
const fetchUrlsFromSheet = async auth => {
  const sheets = google.sheets({ version: 'v4', auth });
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!${RANGE}`,
  });

  const rows = res.data.values;
  if (!rows || rows.length === 0) {
    console.log('No data found.');
    return [];
  }

  return rows.map((row, idx) => ({
    url: row[0],
    rowIndex: idx + 2, // Because R2 starts at row 2
  })).filter(entry => entry.url);
};

// Scrape name/link from a TruePeopleSearch URL
const scrapeUrl = async (page, url) => {
  console.log(`Scraping: ${url}`);
  try {
    await page.goto(url, { waitUntil: 'networkidle' });

    const captchaVisible = await page.locator('form#captchaForm').isVisible();
    if (captchaVisible) {
      console.log('🛑 CAPTCHA detected. Skipping...');
      return ['CAPTCHA', ''];
    }

    const result = await page.locator('div.card-summary').evaluateAll(cards =>
      cards.map(card => ({
        name: card.querySelector('div.h4')?.innerText || '',
        link: card.querySelector('a.btn-lg')?.href || ''
      }))
    );

    if (result.length > 0) {
      const { name, link } = result[0];
      return [name, link];
    } else {
      return ['NO DATA', ''];
    }
  } catch (err) {
    console.error(`❌ Error scraping ${url}:`, err.message);
    return ['ERROR', ''];
  }
};

// Write result horizontally to S:T on same row
const writeResultsToSheet = async (auth, rowIndex, values) => {
  const sheets = google.sheets({ version: 'v4', auth });
  const range = `${SHEET_NAME}!S${rowIndex}:T${rowIndex}`;

  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range,
    valueInputOption: 'RAW',
    requestBody: {
      values: [values],
    },
  });
};

// Main function
const main = async () => {
  const auth = await authorizeGoogle();
  const urls = await fetchUrlsFromSheet(auth);
  console.log(`Found ${urls.length} URLs to process.`);

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
      '(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  });
  const page = await context.newPage();

  for (const { url, rowIndex } of urls) {
    const result = await scrapeUrl(page, url);
    await writeResultsToSheet(auth, rowIndex, result);
    console.log(`✅ Row ${rowIndex} updated.`);
  }

  await browser.close();
  console.log('🎉 All done.');
};

main();
