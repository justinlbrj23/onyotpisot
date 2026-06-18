const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { execFile } = require('child_process');
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

const VIEWPORT = { width: 1280, height: 900 };

/**
 * Baseline label positions from the page layout.
 * We use OCR-detected label position(s) to shift field crop boxes dynamically.
 */
const LABEL_BASE = {
  suitNo: { x: 25, y: 126 },
  parcelNo: { x: 327, y: 126 },
  owner: { x: 22, y: 155 },
  propertyAddress: { x: 25, y: 215 },
  dateSold: { x: 25, y: 324 },
  purchasePrice: { x: 25, y: 352 },
  judgment: { x: 326, y: 352 },
  excess: { x: 499, y: 352 },
  purchaser: { x: 25, y: 379 }
};

/**
 * Baseline value-box crop rectangles.
 * These are adjusted dynamically based on detected labels.
 */
const VALUE_BASE = {
  suitNo:         { x: 103, y: 117, width: 163, height: 29 },
  parcelNo:       { x: 374, y: 117, width: 215, height: 29 },
  owner:          { x: 103, y: 145, width: 372, height: 29 },

  propertyStreet: { x: 66,  y: 206, width: 296, height: 29 },
  propertyCity:   { x: 364, y: 206, width: 105, height: 29 },
  propertyState:  { x: 461, y: 206, width: 53,  height: 29 },

  dateSold:       { x: 111, y: 315, width: 249, height: 29 },
  purchasePrice:  { x: 111, y: 342, width: 149, height: 29 },
  judgment:       { x: 366, y: 342, width: 95,  height: 29 },
  excess:         { x: 529, y: 342, width: 61,  height: 29 },

  purchaser:      { x: 96,  y: 368, width: 379, height: 29 },

  formArea:       { x: 0,   y: 70,  width: 650, height: 520 }
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
    .replace(/^[\]\[\(\)\{\}~`'".,:;_\-]+/, '')
    .replace(/[\]\[\(\)\{\}~`'".,:;_\-]+$/, '')
    .trim();
}

function normalizeWord(value) {
  return clean(value).toLowerCase().replace(/[^a-z0-9]/g, '');
}

function cleanupName(value) {
  let v = stripLeadingNoise(value)
    .replace(/^[^A-Z0-9]+/i, '')
    .replace(/[^A-Z0-9&.,#/\- ]+/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();

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

function looksLikeMoney(v) {
  return /^\$?\d[\d,]*\.?\d*$/.test(clean(v));
}

function looksLikeDate(v) {
  return /^\d{1,2}[\/.-]\d{1,2}[\/.-]\d{2,4}$/.test(clean(v));
}

function looksLikeOwner(v) {
  const s = clean(v);
  return s.length >= 6 && /[A-Za-z]/.test(s);
}

function looksLikeAddress(v) {
  const s = clean(v);
  return s.length >= 8 && /\d/.test(s) && /[A-Za-z]/.test(s);
}

function looksLikePurchaser(v) {
  const s = clean(v);
  return s.length >= 4 && /[A-Za-z]/.test(s);
}

function recordQuality(record) {
  let score = 0;

  if (looksLikeOwner(record.owner)) score += 2;
  if (looksLikeAddress(record.propertyAddress)) score += 3;
  if (looksLikeDate(record.dateSold)) score += 2;
  if (looksLikeMoney(record.purchasePrice)) score += 1;
  if (looksLikeMoney(record.judgment)) score += 1;
  if (looksLikeMoney(record.excess)) score += 1;
  if (looksLikePurchaser(record.purchaser)) score += 1;

  return score;
}

function isRecordUsable(record) {
  return recordQuality(record) >= 5;
}

async function ensureDir(dir) {
  await fs.promises.mkdir(dir, { recursive: true });
}

function imageHash(filePath) {
  const buf = fs.readFileSync(filePath);
  return crypto.createHash('md5').update(buf).digest('hex');
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

  return rawShotPath;
}

async function cropImage(inputPath, outputPath, rect, preprocess = {}) {
  const {
    pad = 4,
    threshold = 185,
    enlarge = 3,
    grayscale = true,
    normalize = true,
    sharpen = true
  } = preprocess;

  const meta = await sharp(inputPath).metadata();
  const imgW = meta.width;
  const imgH = meta.height;

  const left = Math.max(0, Math.round(rect.x - pad));
  const top = Math.max(0, Math.round(rect.y - pad));
  const width = Math.min(imgW - left, Math.round(rect.width + pad * 2));
  const height = Math.min(imgH - top, Math.round(rect.height + pad * 2));

  let img = sharp(inputPath).extract({ left, top, width, height });

  if (grayscale) img = img.grayscale();
  if (normalize) img = img.normalize();

  img = img.resize({
    width: width * enlarge,
    height: height * enlarge,
    fit: 'fill'
  });

  if (sharpen) img = img.sharpen();

  img = img.threshold(threshold).png();

  await img.toFile(outputPath);
}

async function runTesseractText(imagePath, opts = {}) {
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

  const { stdout } = await execFileAsync('tesseract', args);
  return clean(String(stdout || '').replace(/\r/g, '\n').replace(/\n+/g, ' '));
}

async function runTesseractTsv(imagePath, opts = {}) {
  const psm = opts.psm || 11;

  const args = [
    imagePath,
    'stdout',
    '--psm',
    String(psm),
    '-l',
    'eng',
    'tsv'
  ];

  const { stdout } = await execFileAsync('tesseract', args);
  return String(stdout || '');
}

function parseTsv(tsv) {
  const lines = String(tsv || '').split('\n').filter(Boolean);
  if (!lines.length) return [];

  const header = lines[0].split('\t');
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split('\t');
    if (cols.length < header.length) continue;

    const obj = {};
    for (let j = 0; j < header.length; j++) {
      obj[header[j]] = cols[j];
    }

    const text = clean(obj.text || '');
    const conf = Number(obj.conf || -1);

    if (!text) continue;

    rows.push({
      text,
      norm: normalizeWord(text),
      conf,
      left: Number(obj.left || 0),
      top: Number(obj.top || 0),
      width: Number(obj.width || 0),
      height: Number(obj.height || 0)
    });
  }

  return rows;
}

function findPhrase(words, phraseTokens) {
  const target = phraseTokens.map(normalizeWord);

  for (let i = 0; i <= words.length - target.length; i++) {
    let ok = true;
    for (let j = 0; j < target.length; j++) {
      if (words[i + j].norm !== target[j]) {
        ok = false;
        break;
      }
    }

    if (ok) {
      const group = words.slice(i, i + target.length);
      const left = Math.min(...group.map(w => w.left));
      const top = Math.min(...group.map(w => w.top));
      const right = Math.max(...group.map(w => w.left + w.width));
      const bottom = Math.max(...group.map(w => w.top + w.height));

      return {
        left,
        top,
        width: right - left,
        height: bottom - top
      };
    }
  }

  return null;
}

function computeDynamicRects(words) {
  const ownerLabel = findPhrase(words, ['Owner']);
  const propertyLabel = findPhrase(words, ['Property', 'Address']);
  const dateLabel = findPhrase(words, ['Date', 'Sold']);

  let dx = 0;
  let dy = 0;

  if (ownerLabel) {
    dx = ownerLabel.left - LABEL_BASE.owner.x;
    dy = ownerLabel.top - LABEL_BASE.owner.y;
  } else if (propertyLabel) {
    dx = propertyLabel.left - LABEL_BASE.propertyAddress.x;
    dy = propertyLabel.top - LABEL_BASE.propertyAddress.y;
  } else if (dateLabel) {
    dx = dateLabel.left - LABEL_BASE.dateSold.x;
    dy = dateLabel.top - LABEL_BASE.dateSold.y;
  }

  const rects = {};
  for (const [key, rect] of Object.entries(VALUE_BASE)) {
    rects[key] = {
      x: rect.x + dx,
      y: rect.y + dy,
      width: rect.width,
      height: rect.height
    };
  }

  return rects;
}

async function ocrField(rawShotPath, baseName, rect, ocrOpts, variants = []) {
  const list = variants.length
    ? variants
    : [
        { threshold: 170, enlarge: 3, pad: 4 },
        { threshold: 185, enlarge: 3, pad: 4 },
        { threshold: 200, enlarge: 4, pad: 4 }
      ];

  const results = [];

  for (let i = 0; i < list.length; i++) {
    const outPath = path.join(DEBUG_DIR, `${baseName}-v${i + 1}.png`);
    await cropImage(rawShotPath, outPath, rect, list[i]);

    const text = await runTesseractText(outPath, ocrOpts).catch(() => '');
    results.push(stripLeadingNoise(text));
  }

  const best = results.sort((a, b) => b.length - a.length)[0] || '';
  return best;
}

async function saveFormAreaDebug(rawShotPath, rects, index) {
  const formRect = rects.formArea || VALUE_BASE.formArea;
  const outPath = path.join(DEBUG_DIR, `record-${String(index).padStart(4, '0')}-form.png`);

  await cropImage(rawShotPath, outPath, formRect, {
    threshold: 180,
    enlarge: 2,
    pad: 6
  });
}

async function extractRecordWithDynamicOCR(rawShotPath, index) {
  const pageTsv = await runTesseractTsv(rawShotPath, { psm: 11 });
  const words = parseTsv(pageTsv);

  const tsvPath = path.join(DEBUG_DIR, `record-${String(index).padStart(4, '0')}-page.tsv`);
  await fs.promises.writeFile(tsvPath, pageTsv, 'utf8');

  const rects = computeDynamicRects(words);
  await saveFormAreaDebug(rawShotPath, rects, index);

  const ALNUM = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  const MONEY = '0123456789$,.';
  const DATE = '0123456789/.';
  const ADDRESS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789#-.,/ ';
  const NAME = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789&.,#/- ';

  const suitNo = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-suitNo`,
    rects.suitNo,
    { psm: 7, whitelist: `${ALNUM}-` }
  );

  const parcelNo = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-parcelNo`,
    rects.parcelNo,
    { psm: 7, whitelist: `${ALNUM}-` }
  );

  const owner = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-owner`,
    rects.owner,
    { psm: 7, whitelist: NAME }
  );

  const propertyStreet = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-propertyStreet`,
    rects.propertyStreet,
    { psm: 7, whitelist: ADDRESS }
  );

  const propertyCity = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-propertyCity`,
    rects.propertyCity,
    { psm: 7, whitelist: ADDRESS }
  );

  const propertyState = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-propertyState`,
    rects.propertyState,
    { psm: 7, whitelist: 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' }
  );

  const dateSold = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-dateSold`,
    rects.dateSold,
    { psm: 7, whitelist: DATE }
  );

  const purchasePrice = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-purchasePrice`,
    rects.purchasePrice,
    { psm: 7, whitelist: MONEY }
  );

  const judgment = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-judgment`,
    rects.judgment,
    { psm: 7, whitelist: MONEY }
  );

  const excess = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-excess`,
    rects.excess,
    { psm: 7, whitelist: MONEY }
  );

  const purchaser = await ocrField(
    rawShotPath,
    `record-${String(index).padStart(4, '0')}-purchaser`,
    rects.purchaser,
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

  const quality = recordQuality(record);

  const outJson = path.join(
    DEBUG_DIR,
    `record-${String(index).padStart(4, '0')}-parsed.json`
  );
  await fs.promises.writeFile(
    outJson,
    JSON.stringify({ quality, record }, null, 2),
    'utf8'
  );

  console.log(`Parsed record [${index}] quality=${quality}:`, JSON.stringify(record, null, 2));

  return { record, quality };
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
  const candidates = [
    page.locator('input[type="submit"][value="Next"]').first(),
    page.locator('input[type="button"][value="Next"]').first(),
    page.locator('input[value="Next"]').first(),
    page.getByRole('button', { name: /^Next$/i }).first(),
    page.getByText(/^Next$/i).first()
  ];

  for (const locator of candidates) {
    try {
      if (await locator.count()) {
        try {
          await locator.scrollIntoViewIfNeeded();
        } catch (_) {}
        await locator.click({ timeout: 5000 });
        return true;
      }
    } catch (_) {}
  }

  // coordinate fallback
  try {
    await page.mouse.click(291, 107);
    return true;
  } catch (_) {}

  return false;
}

async function scrapeAllParcels() {
  await ensureDir(DEBUG_DIR);

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({
    viewport: VIEWPORT,
    deviceScaleFactor: 1
  });

  const records = [];
  const seenImageHashes = new Set();
  const seenRecordSigs = new Set();

  try {
    console.log('Starting scraper...');
    console.log('Opening:', TARGET_URL);

    await page.goto(TARGET_URL, {
      waitUntil: 'domcontentloaded',
      timeout: 60000
    });

    await page.waitForTimeout(4000);

    const MAX_PAGES = Number(process.env.MAX_PAGES || 5000);

    for (let i = 1; i <= MAX_PAGES; i++) {
      const rawShotPath = await capturePage(page, i);
      const shotHash = imageHash(rawShotPath);

      if (seenImageHashes.has(shotHash)) {
        console.log('Detected repeated page screenshot. Stopping.');
        break;
      }
      seenImageHashes.add(shotHash);

      const { record, quality } = await extractRecordWithDynamicOCR(rawShotPath, i);

      if (isRecordUsable(record)) {
        const sig = makeSignature(record);

        if (!seenRecordSigs.has(sig)) {
          seenRecordSigs.add(sig);
          records.push(record);

          console.log(
            `[${records.length}] quality=${quality} | ` +
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
          console.log('Duplicate valid OCR record in current run; skipping.');
        }
      } else {
        console.log(
          `Rejected OCR record [${i}] due to low quality score (${quality}). Not adding to output.`
        );
      }

      const clicked = await tryClickNext(page);
      if (!clicked) {
        console.log('Next button not found/clickable. Stopping.');
        break;
      }

      await page.waitForTimeout(3000);
    }

    console.log(`Scraping complete. Collected ${records.length} valid unique record(s).`);
    return records;
  } finally {
    await page.close().catch(() => null);
    await browser.close().catch(() => null);
  }
}

async function main() {
  const records = await scrapeAllParcels();

  if (!records.length) {
    console.log('No valid records scraped. Exiting without Sheets update.');
    return;
  }

  const sheets = await getGoogleSheetsClient();
  const existingSignatures = await getExistingSignatures(sheets);

  const newRecords = records.filter((r) => !existingSignatures.has(makeSheetSignature(r)));

  console.log(`Existing sheet signatures: ${existingSignatures.size}`);
  console.log(`New valid records to append: ${newRecords.length}`);

  if (!newRecords.length) {
    console.log('No new valid records to append.');
    return;
  }

  await appendRowsToSheet(sheets, newRecords);

  console.log('Done.');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
