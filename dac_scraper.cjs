// dac_scraper.cjs
// Dallas CAD Parcel Scraper
// Requires:
// npm install puppeteer puppeteer-extra puppeteer-extra-plugin-stealth googleapis

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const { google } = require('googleapis');

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = './service-account.json';
const SPREADSHEET_ID = '1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA';
const SHEET_NAME = 'web_tda';
const PARCEL_RANGE = 'F2:F';
const OWNER_OUTPUT_COL = 'N';

const DRIVE_PARENT_FOLDER = '11c9BxTj6ej-fJNvECJM_oBDz3WfsSkWl';

const TARGET_URL_1 = 'https://www.dallascad.org/AcctDetailRes.aspx?ID=';
const TARGET_URL_2 = 'https://www.dallascad.org/AcctHistory.aspx?ID=';

puppeteer.use(StealthPlugin());

// =========================
// GOOGLE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
  ]
});

const sheets = google.sheets({ version: 'v4', auth });
const drive = google.drive({ version: 'v3', auth });

// =========================
// Load Parcel IDs
// =========================
async function loadParcelIds() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!${PARCEL_RANGE}`,
  });

  return (res.data.values || [])
    .flat()
    .map(v => (v || '').trim())
    .filter(Boolean);
}

// =========================
// Drive Upload Helper
// =========================
async function uploadToDrive(parentFolderId, localPath, driveName) {
  await drive.files.create({
    requestBody: {
      name: driveName,
      parents: [parentFolderId],
    },
    media: {
      mimeType: 'image/jpeg',
      body: fs.createReadStream(localPath),
    },
    fields: 'id',
  });
}

// =========================
// Create Drive Subfolder
// =========================
async function createSubfolder(parcelId) {
  const folder = await drive.files.create({
    requestBody: {
      name: `${parcelId}`,
      mimeType: 'application/vnd.google-apps.folder',
      parents: [DRIVE_PARENT_FOLDER],
    },
    fields: 'id',
  });

  return folder.data.id;
}

// =========================
// Extract Owner Name
// =========================
async function extractOwnerName(page) {
  return await page.evaluate(() => {
    const cells = Array.from(document.querySelectorAll('table td'));
    if (cells.length < 2) return '';

    const raw = cells[1].innerText.trim();
    return raw.split('\n')[0].trim();
  });
}

// =========================
// MAIN
// =========================
(async () => {
  console.log('📥 Loading PARCEL IDs...');
  const parcels = await loadParcelIds();
  console.log(`🧾 Loaded ${parcels.length} Parcel IDs`);

  const browser = await puppeteer.launch({
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-blink-features=AutomationControlled',
      '--ignore-certificate-errors',
      '--disable-gpu',
      '--single-process',
      '--no-zygote',
    ],
  });

  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(120000);

  // Anti-bot
  await page.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
      'AppleWebKit/537.36 (KHTML, like Gecko) ' +
      'Chrome/121.0.0.0 Safari/537.36'
  );
  await page.setExtraHTTPHeaders({
    'Accept-Language': 'en-US,en;q=0.9',
    Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  });

  for (let i = 0; i < parcels.length; i++) {
    const parcel = parcels[i];
    console.log(`\n==============================`);
    console.log(`📌 Processing Parcel: ${parcel}`);
    console.log(`Row: ${i + 2}`);

    // Create Drive folder
    const folderId = await createSubfolder(parcel);
    console.log(`📁 Created Drive folder: ${folderId}`);

    // ============================
    // Visit URL 1 (Detail)
    // ============================
    const url1 = TARGET_URL_1 + parcel;
    console.log(`🌐 Navigating to: ${url1}`);
    await page.goto(url1, { waitUntil: 'networkidle2' });

    const shot1 = `detail_${parcel}.jpg`;
    await page.screenshot({ path: shot1, type: 'jpeg', fullPage: true });
    await uploadToDrive(folderId, shot1, `DETAIL_${parcel}.jpg`);
    fs.unlinkSync(shot1);
    console.log(`📤 Uploaded DETAIL screenshot`);

    // ============================
    // Visit URL 2 (History)
    // ============================
    const url2 = TARGET_URL_2 + parcel;
    console.log(`🌐 Navigating to: ${url2}`);
    await page.goto(url2, { waitUntil: 'networkidle2' });

    const shot2 = `history_${parcel}.jpg`;
    await page.screenshot({ path: shot2, type: 'jpeg', fullPage: true });
    await uploadToDrive(folderId, shot2, `HISTORY_${parcel}.jpg`);
    fs.unlinkSync(shot2);
    console.log(`📤 Uploaded HISTORY screenshot`);

    // ============================
    // Extract Owner Name
    // ============================
    const ownerName = await extractOwnerName(page);
    console.log(`👤 Owner: ${ownerName}`);

    // ============================
    // Write owner back to sheet
    // ============================
    const writeRange = `${SHEET_NAME}!${OWNER_OUTPUT_COL}${i + 2}`;
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: writeRange,
      valueInputOption: 'RAW',
      requestBody: { values: [[ownerName]] },
    });

    console.log(`📝 Updated sheet cell ${writeRange}`);
  }

  await browser.close();
  console.log('\n🏁 DONE — All parcels processed successfully!');
})();