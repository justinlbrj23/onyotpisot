// dac_scraper.cjs
// Dallas CAD Parcel Scraper (WebInspector-style, year-matched owner extraction)
// Requires:
// npm install puppeteer puppeteer-extra puppeteer-extra-plugin-stealth googleapis cheerio

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const cheerio = require('cheerio');
const { google } = require('googleapis');

// =========================
// CONFIG
// =========================
const SERVICE_ACCOUNT_FILE = './service-account.json';
const SPREADSHEET_ID = '1CsLXhlNp9pP9dAVBpGFvEnw1PpuUvLfypFg56RrgjxA';
const SHEET_NAME = 'raw_main';
const PARCEL_RANGE = 'F2:F';
const YEAR_RANGE = 'H2:H';
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
// Load Parcel IDs & Auction Years
// =========================
async function loadParcelData() {
  const parcelsRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!${PARCEL_RANGE}`,
  });
  const yearsRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${SHEET_NAME}!${YEAR_RANGE}`,
  });

  const parcels = (parcelsRes.data.values || []).flat().map(v => (v || '').trim());
  const years = (yearsRes.data.values || []).flat().map(v => (v || '').trim());

  // Pair up parcels and years by row index
  const parcelData = [];
  for (let i = 0; i < parcels.length; i++) {
    if (parcels[i]) {
      parcelData.push({
        parcelId: parcels[i],
        auctionYear: years[i] || '',
        rowNum: i + 2
      });
    }
  }
  return parcelData;
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
      name: parcelId,
      mimeType: 'application/vnd.google-apps.folder',
      parents: [DRIVE_PARENT_FOLDER],
    },
    fields: 'id',
  });
  return folder.data.id;
}

// =========================
// Extract Owner Name for Year (Cheerio, WebInspector-style)
// =========================
function extractOwnerNameForYear(html, auctionYear) {
  const $ = cheerio.load(html);
  let ownerName = '';

  // Find the "Owner / Legal Description" section
  const bodyText = $('body').text();

  // Regex to find the block for the correct year
  // Looks for: YEAR\nOWNER NAME\nADDRESS
  const regex = new RegExp(`${auctionYear}\\s*([A-Z\\s]+)\\s*\\d{1,4}`, 'm');
  const match = bodyText.match(regex);
  if (match) {
    ownerName = match[1].trim();
  } else {
    // Fallback: try to find the first uppercase line after the year
    const yearIdx = bodyText.indexOf(auctionYear);
    if (yearIdx !== -1) {
      const afterYear = bodyText.slice(yearIdx + auctionYear.length).split('\n').map(l => l.trim());
      for (const line of afterYear) {
        if (/^[A-Z\s]+$/.test(line) && line.length > 2) {
          ownerName = line;
          break;
        }
      }
    }
  }
  return ownerName;
}

// =========================
// MAIN
// =========================
(async () => {
  console.log('📥 Loading PARCEL IDs and Auction Years...');
  const parcelData = await loadParcelData();
  console.log(`🧾 Loaded ${parcelData.length} Parcel IDs`);

  const browser = await puppeteer.launch({
    headless: new,
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

  // Anti-bot improvements (matching your inspector style)
  await page.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
      'AppleWebKit/537.36 (KHTML, like Gecko) ' +
      'Chrome/121.0.0.0 Safari/537.36'
  );
  await page.setExtraHTTPHeaders({
    Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
  });
  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => false });
  });

  // ======================================
  // MAIN LOOP
  // ======================================
  for (const { parcelId, auctionYear, rowNum } of parcelData) {
    console.log('\n==============================');
    console.log(`📌 Processing Parcel: ${parcelId}`);
    console.log(`Row: ${rowNum}, Auction Year: ${auctionYear}`);

    // Create Drive folder
    const folderId = await createSubfolder(parcelId);
    console.log(`📁 Created Drive folder: ${folderId}`);

    // -------------------------------
    // TARGET URL 1 (DETAIL PAGE)
    // -------------------------------
    const url1 = TARGET_URL_1 + parcelId;
    console.log(`🌐 Navigating to: ${url1}`);

    let attempt1 = 0;
    let html1 = '';
    while (attempt1 < 3) {
      try {
        await page.goto(url1, { waitUntil: 'domcontentloaded', timeout: 120000 });
        await new Promise(r => setTimeout(r, 3000));
        html1 = await page.content();
        break;
      } catch (err) {
        attempt1++;
        console.log(`⚠️ Navigation failure (${attempt1}/3) for: ${url1}`);
        if (attempt1 >= 3) throw err;
        await new Promise(r => setTimeout(r, 4000));
      }
    }

    const shot1 = `detail_${parcelId}.jpg`;
    await page.screenshot({ path: shot1, type: 'jpeg', fullPage: true });
    await uploadToDrive(folderId, shot1, `DETAIL_${parcelId}.jpg`);
    fs.unlinkSync(shot1);
    console.log(`📤 Uploaded DETAIL screenshot`);

    // -------------------------------
    // TARGET URL 2 (HISTORY PAGE)
    // -------------------------------
    const url2 = TARGET_URL_2 + parcelId;
    console.log(`🌐 Navigating to: ${url2}`);

    let attempt2 = 0;
    let html2 = '';
    while (attempt2 < 3) {
      try {
        await page.goto(url2, { waitUntil: 'domcontentloaded', timeout: 120000 });
        await new Promise(r => setTimeout(r, 3000));
        html2 = await page.content();
        break;
      } catch (err) {
        attempt2++;
        console.log(`⚠️ Navigation failure (${attempt2}/3) for: ${url2}`);
        if (attempt2 >= 3) throw err;
        await new Promise(r => setTimeout(r, 4000));
      }
    }

    const shot2 = `history_${parcelId}.jpg`;
    await page.screenshot({ path: shot2, type: 'jpeg', fullPage: true });
    await uploadToDrive(folderId, shot2, `HISTORY_${parcelId}.jpg`);
    fs.unlinkSync(shot2);
    console.log(`📤 Uploaded HISTORY screenshot`);

    // -------------------------------
    // Extract Owner Name for Auction Year
    // -------------------------------
    const ownerName = extractOwnerNameForYear(html2, auctionYear);
    console.log(`👤 Owner for ${auctionYear}: ${ownerName}`);

    // -------------------------------
    // Write back to sheet
    // -------------------------------
    const writeRange = `${SHEET_NAME}!${OWNER_OUTPUT_COL}${rowNum}`;

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