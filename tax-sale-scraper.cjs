const { execFile } = require('child_process');
const { promisify } = require('util');
const sharp = require('sharp');
const { chromium } = require('playwright');
const { google } = require('googleapis');

const execFileAsync = promisify(execFile);

/**
 * Rects are in CSS px for a 640x480 screenshot.
 * Each rect targets ONLY the visible input textbox area (not the label),
 * which drastically improves OCR quality versus full-page OCR.
 */
const RECTS = {
  suitNo:         { x: 96,  y: 92,  width: 162, height: 25 },
  parcelNo:       { x: 366, y: 92,  width: 214, height: 25 },
  owner:          { x: 96,  y: 118, width: 364, height: 25 },

  propertyStreet: { x: 65,  y: 177, width: 295, height: 25 },
  propertyCity:   { x: 364, y: 177, width: 105, height: 25 },
  propertyState:  { x: 461, y: 177, width: 53,  height: 25 },

  dateSold:       { x: 111, y: 252, width: 248, height: 25 },
  purchasePrice:  { x: 111, y: 279, width: 149, height: 25 },
  judgment:       { x: 366, y: 279, width: 94,  height: 25 },
  excess:         { x: 528, y: 279, width: 60,  height: 25 },

  purchaser:      { x: 96,  y: 305, width: 364, height: 25 },

  // Optional: crop around the central form only for debugging
  formArea:       { x: 55,  y: 70,  width: 545, height: 290 }
};

function clean(value) {
  return String(value || '')
    .replace(/\u00A0/g, ' ')
    .replace(/[“”‘’]/g, "'")
    .replace(/[|]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function stripLeadingNoise(value) {
  return clean(value)
    .replace(/^[\]\[\(\)\{\}~`'".,:;_-]+/, '')
    .replace(/[\]\[\(\)\{\}~`'".,:;_-]+$/, '')
    .trim();
}

function cleanupName(value) {
  let v = stripLeadingNoise(value)
    .replace(/^[^A-Z0-9]+/i, '')
    .replace(/[^A-Z0-9&.,#/\- ]+/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  // Common OCR mistakes
  v = v.replace(/\bLLC\.$/i, 'LLC');
  return v;
}

function cleanupMoney(value) {
  let v = stripLeadingNoise(value)
    .replace(/[^0-9.,$]/g, '')
    .replace(/^\.+/, '')
    .trim();

  if (!v) return '';
  if (!v.startsWith('$')) v = `$${v}`;
  return v;
}

function cleanupDate(value) {
  return stripLeadingNoise(value)
    .replace(/[^0-9/.-]/g, '')
    .trim();
}

function cleanupAddressPart(value) {
  return stripLeadingNoise(value)
    .replace(/[^A-Z0-9#.,/\- ]+/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function makeSignature(record) {
  return [
    clean(record.suitNo).toUpperCase(),
    clean(record.parcelNo).toUpperCase(),
    clean(record.propertyAddress).toUpperCase(),
    clean(record.owner).toUpperCase(),
    clean(record.dateSold).toUpperCase(),
    clean(record.judgment).toUpperCase(),
    clean(record.purchasePrice).toUpperCase(),
    clean(record.excess).toUpperCase(),
    clean(record.purchaser).toUpperCase()
  ].join(' | ');
}

function makeSheetSignature(record) {
  return [
    clean(record.propertyAddress).toUpperCase(),
    clean(record.owner).toUpperCase(),
    clean(record.dateSold).toUpperCase(),
    clean(record.judgment).toUpperCase(),
    clean(record.purchasePrice).toUpperCase(),
    clean(record.excess).toUpperCase(),
    clean(record.purchaser).toUpperCase()
  ].join(' | ');
}

function isRecordUsable(record) {
  return Boolean(
    clean(record.propertyAddress) ||
    clean(record.owner) ||
    clean(record.dateSold) ||
    clean(record.judgment) ||
    clean(record.purchasePrice) ||
    clean(record.excess) ||
    clean(record.purchaser)
  );
}

async function ensureDir(dir) {
  await fs.promises.mkdir(dir, { recursive: true });
}

function scaledRect(rect, actualWidth, actualHeight) {
  const scaleX = actualWidth / BASE_SHOT.width;
  const scaleY = actualHeight / BASE_SHOT.height;

  return {
    left: Math.max(0, Math.round(rect.x * scaleX)),
    top: Math.max(0, Math.round(rect.y * scaleY)),
    width: Math.max(1, Math.round(rect.width * scaleX)),
    height: Math.max(1, Math.round(rect.height * scaleY))
  };
}

async function cropAndPreprocess(rawShotPath, outPath, rect, options = {}) {
  const {
    threshold = 185,
    enlarge = 3,
    grayscale = true,
    sharpen = true,
    normalize = true,
    pad = 2
  } = options;

  const meta = await sharp(rawShotPath).metadata();
  const actualWidth = meta.width || BASE_SHOT.width;
  const actualHeight = meta.height || BASE_SHOT.height;

  const r = scaledRect(rect, actualWidth, actualHeight);

  const safeLeft = Math.max(0, r.left - pad);
  const safeTop = Math.max(0, r.top - pad);
  const safeWidth = Math.min(actualWidth - safeLeft, r.width + pad * 2);
  const safeHeight = Math.min(actualHeight - safeTop, r.height + pad * 2);

  let img = sharp(rawShotPath).extract({
    left: safeLeft,
    top: safeTop,
    width: safeWidth,
    height: safeHeight
  });

  if (grayscale) img = img.grayscale();
  if (normalize) img = img.normalize();

  img = img.resize({
    width: safeWidth * enlarge,
    height: safeHeight * enlarge,
    fit: 'fill'
  });

  if (sharpen) img = img.sharpen();

  img = img.threshold(threshold).png();

  await img.toFile(outPath);
}

async function runTesseract(imagePath, opts = {}) {
  const {
    psm = 7,
    whitelist = ''
  } = opts;

  const args = [
    imagePath,
    'stdout',
    '--psm',
    String(psm),
    '-l',
    'eng',
    '-c',
    'preserve_interword_spaces=1'
  ];

  if (whitelist) {
    args.push('-c', `tessedit_char_whitelist=${whitelist}`);
  }

  const { stdout, stderr } = await execFileAsync('tesseract', args);

  if (stderr && stderr.trim()) {
    console.log('Tesseract stderr:', stderr.trim());
  }

  return clean(
    String(stdout || '')
      .replace(/\r/g, '\n')
      .replace(/\n+/g, ' ')
  );
}

async function ocrField(rawShotPath, fieldName, rect, ocrOpts, preprocessVariants = []) {
  const base = path.join(DEBUG_DIR, fieldName);

  const variants = preprocessVariants.length
    ? preprocessVariants
    : [
        { threshold: 170, enlarge: 3, pad: 2 },
        { threshold: 185, enlarge: 3, pad: 2 },
        { threshold: 200, enlarge: 4, pad: 2 }
      ];

  const results = [];

  for (let i = 0; i < variants.length; i++) {
    const outPath = `${base}-v${i + 1}.png`;
    await cropAndPreprocess(rawShotPath, outPath, rect, variants[i]);

    const text = await runTesseract(outPath, ocrOpts).catch(() => '');
    results.push(clean(text));
  }

  // Choose the "best" candidate: longest useful value after trimming noise.
  const best = results
    .map((v) => stripLeadingNoise(v))
    .sort((a, b) => b.length - a.length)[0] || '';

  return best;
}

async function saveDebugFormCrop(rawShotPath, index) {
  const outPath = path.join(DEBUG_DIR, `record-${String(index).padStart(4, '0')}-form.png`);
  await cropAndPreprocess(rawShotPath, outPath, RECTS.formArea, {
    threshold: 180,
    enlarge: 2,
    pad: 4
  });
}

async function capturePage(page, index) {
  await ensureDir(DEBUG_DIR);

  const rawShotPath = path.join(
    DEBUG_DIR,
    `record-${String(index).padStart(4, '0')}-raw.png`
  );

  await page.screenshot({
    path: rawShotPath,
    fullPage: false
  });

  await saveDebugFormCrop(rawShotPath, index);

  return rawShotPath;
}

async function extractRecordWithFieldOCR(rawShotPath, index) {
  // Per-field OCR configs
  const ALNUM = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  const MONEY = '0123456789$,.';
  const DATE = '0123456789/.';
  const ADDRESS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789#-.,/ ';
  const NAME = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789&.,#/- ';

  const suitNo = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-suitNo`,
    RECTS.suitNo,
    { psm: 7, whitelist: `${ALNUM}-` }
  );

  const parcelNo = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-parcelNo`,
    RECTS.parcelNo,
    { psm: 7, whitelist: `${ALNUM}-` }
  );

  const owner = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-owner`,
    RECTS.owner,
    { psm: 7, whitelist: NAME }
  );

  const propertyStreet = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-propertyStreet`,
    RECTS.propertyStreet,
    { psm: 7, whitelist: ADDRESS }
  );

  const propertyCity = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-propertyCity`,
    RECTS.propertyCity,
    { psm: 7, whitelist: ADDRESS }
  );

  const propertyState = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-propertyState`,
    RECTS.propertyState,
    { psm: 7, whitelist: 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' }
  );

  const dateSold = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-dateSold`,
    RECTS.dateSold,
    { psm: 7, whitelist: DATE }
  );

  const purchasePrice = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-purchasePrice`,
    RECTS.purchasePrice,
    { psm: 7, whitelist: MONEY }
  );

  const judgment = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-judgment`,
    RECTS.judgment,
    { psm: 7, whitelist: MONEY }
  );

  const excess = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-excess`,
    RECTS.excess,
    { psm: 7, whitelist: MONEY }
  );

  const purchaser = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-purchaser`,
    RECTS.purchaser,
    { psm: 7, whitelist: NAME }
  );

  const record = {
    suitNo: cleanupAddressPart(suitNo),
    parcelNo: cleanupAddressPart(parcelNo),
    owner: cleanupName(owner),
    propertyAddress: clean(
      [
        cleanupAddressPart(propertyStreet),
        cleanupAddressPart(propertyCity),
        cleanupAddressPart(propertyState)
      ].filter(Boolean).join(' ')
    ),
    dateSold: cleanupDate(dateSold),
    purchasePrice: cleanupMoney(purchasePrice),
    judgment: cleanupMoney(judgment),
    excess: cleanupMoney(excess),
    purchaser: cleanupName(purchaser)
  };

  // Save structured OCR output for debugging
  const txtPath = path.join(
    DEBUG_DIR,
    `record-${String(index).padStart(4, '0')}-parsed.json`
  );
  await fs.promises.writeFile(txtPath, JSON.stringify(record, null, 2), 'utf8');

  console.log(`Parsed record [${index}]:`, JSON.stringify(record, null, 2));

  return record;
}

async function getGoogleSheetsClient() {
  if (!fs.existsSync(SERVICE_ACCOUNT_FILE)) {
    throw new Error(`Missing service account file: ${SERVICE_ACCOUNT_FILE}`);
  }

  const auth = new google.auth.GoogleAuth({
    keyFile: SERVICE_ACCOUNT_FILE,
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  });

  return google.sheets({
    version: 'v4',
    auth
  });
}

async function getExistingSignatures(sheets) {
  const range = `${SHEET_NAME}!B2:I`;

  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range
  });

  const rows = resp.data.values || [];
  const signatures = new Set();

  for (const row of rows) {
    const record = {
      propertyAddress: row[0] || '',
      owner: row[1] || '',
      dateSold: row[3] || '',
      judgment: row[4] || '',
      purchasePrice: row[5] || '',
      excess: row[6] || '',
      purchaser: row[7] || ''
    };
    signatures.add(makeSheetSignature(record));
  }

  return signatures;
}

async function appendRowsToSheet(sheets, records) {
  if (!records.length) {
    console.log('No new rows to append.');
    return;
  }

  const values = records.map((r) => [
    clean(r.propertyAddress), // B
    clean(r.owner),           // C
    '',                       // D blank
    clean(r.dateSold),        // E
    clean(r.judgment),        // F
    clean(r.purchasePrice),   // G
    clean(r.excess),          // H
    clean(r.purchaser)        // I
  ]);

  await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!B2:I`,
    valueInputOption: 'USER_ENTERED',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values }
  });

  console.log(`Appended ${values.length} new row(s) to Google Sheets.`);
}

async function tryClickNext(page) {
  // Extraction is OCR-based (not DOM-based), but navigation can still use a safe click strategy.
  const candidates = [
    page.locator('input[value="Next"]').first(),
    page.locator('input[type="submit"][value="Next"]').first(),
    page.locator('input[type="button"][value="Next"]').first(),
    page.getByRole('button', { name: /^Next$/i }).first(),
    page.getByText(/^Next$/i).first()
  ];

  for (const locator of candidates) {
    try {
      if (await locator.count()) {
        await locator.click({ timeout: 5000 });
        return true;
      }
    } catch (_) {}
  }

  // Coordinate fallback based on the visible page layout in your screenshot
  try {
    await page.mouse.click(286, 79);
    return true;
  } catch (_) {}

  return false;
}

function imageHash(filePath) {
  const buf = fs.readFileSync(filePath);
  return crypto.createHash('md5').update(buf).digest('hex');
}

async function scrapeAllParcels() {
  await ensureDir(DEBUG_DIR);

  const browser = await chromium.launch({ headless: true });

  const page = await browser.newPage({
    viewport: VIEWPORT,
    deviceScaleFactor: 1
  });

  const records = [];
  const seenRecordSigs = new Set();
  const seenImageHashes = new Set();

  try {
    console.log('Starting scraper...');
    console.log('Opening:', TARGET_URL);

    await page.goto(TARGET_URL, {
      waitUntil: 'domcontentloaded',
      timeout: 60000
    });

    // Give the server-rendered form a moment to settle visually
    await page.waitForTimeout(3500);

    const MAX_PAGES = Number(process.env.MAX_PAGES || 5000);

    for (let i = 1; i <= MAX_PAGES; i++) {
      await page.waitForTimeout(1800);

      const rawShotPath = await capturePage(page, i);
      const shotHash = imageHash(rawShotPath);

      if (seenImageHashes.has(shotHash)) {
        console.log('Detected repeated page screenshot. Stopping.');
        break;
      }
      seenImageHashes.add(shotHash);

      const record = await extractRecordWithFieldOCR(rawShotPath, i);
      const sig = makeSignature(record);

      if (isRecordUsable(record)) {
        if (!seenRecordSigs.has(sig)) {
          seenRecordSigs.add(sig);
          records.push(record);

          console.log(
            `[${records.length}] ` +
            `Suit No="${record.suitNo}" | ` +
            `Parcel No="${record.parcelNo}" | ` +
            `Property Address="${record.propertyAddress}" | ` +
            `Owner="${record.owner}" | ` +
            `Date Sold="${record.dateSold}" | ` +
            `Judgment="${record.judgment}" | ` +
            `Purchase Price="${record.purchasePrice}" | ` +
            `Excess="${record.excess}" | ` +
            `Purchaser="${record.purchaser}"`
          );
        } else {
          console.log('Duplicate OCR record in current run; skipping.');
        }
      } else {
        console.log('OCR record had no usable data.');
      }

      const clicked = await tryClickNext(page);
      if (!clicked) {
        console.log('Next button not found/clickable. Stopping.');
        break;
      }

      await page.waitForTimeout(2500);
    }

    console.log(`Scraping complete. Collected ${records.length} unique record(s).`);
    return records;
  } finally {
    await page.close().catch(() => null);
    await browser.close().catch(() => null);
  }
}

async function main() {
  const records = await scrapeAllParcels();

  if (!records.length) {
    console.log('No records scraped. Exiting without Sheets update.');
    return;
  }

  const sheets = await getGoogleSheetsClient();
  const existingSignatures = await getExistingSignatures(sheets);

  const newRecords = records.filter((r) => !existingSignatures.has(makeSheetSignature(r)));

  console.log(`Existing sheet signatures: ${existingSignatures.size}`);
  console.log(`New records to append: ${newRecords.length}`);

  await appendRowsToSheet(sheets, newRecords);

  console.log('Done.');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
const { promisify } = require('util');
const sharp = require('sharp');
const { chromium } = require('playwright');
const { google } = require('googleapis');

const execFileAsync = promisify(execFile);

const TARGET_URL = 'https://www.16thcircuit.org/browse-all-parcels';
const SHEET_ID = process.env.SHEET_ID || '1fdj-Lk5RIjuo4ekGiAHUPoW7JKqTuiy35b_Q8w2xTyg';
const SHEET_NAME = process.env.SHEET_NAME || 'Tax Sale Tracker';
const SERVICE_ACCOUNT_FILE = path.join(process.cwd(), 'service-account.json');
const DEBUG_DIR = path.join(process.cwd(), 'debug_ocr');

/**
 * IMPORTANT:
 * These crop coordinates are based on the rendered layout you shared.
 * We lock the viewport to 640x480 so the OCR regions stay stable.
 * If the site layout shifts later, only the RECTS section should need tuning.
 */
const VIEWPORT = { width: 640, height: 480 };

// Base screenshot dimension used for crop coordinates below.
