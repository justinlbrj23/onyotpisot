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
 * Updated crop based on the latest uploaded form-base image.
 * Changes vs previous version:
 * - slightly taller crop so Purchaser row is included
 * - slightly better vertical alignment for the actual field rows
 */
const FORM_RECT = {
  x: 182,
  y: 258,
  width: 668,
  height: 390
};

/**
 * OCR boxes relative to FORM_RECT.
 * Tuned from the latest "record-0001-form-base.png" result.
 *
 * Main fixes:
 * - owner moved DOWN
 * - property address moved DOWN
 * - date/money rows moved DOWN
 * - purchaser moved DOWN significantly
 */
const FORM_FIELD_RECTS = {
  owner:          { x: 96,  y: 67,  width: 405, height: 30 },

  propertyStreet: { x: 96,  y: 160, width: 298, height: 30 },
  propertyCity:   { x: 398, y: 160, width: 108, height: 30 },
  propertyState:  { x: 510, y: 160, width: 58,  height: 30 },

  dateSold:       { x: 96,  y: 236, width: 300, height: 30 },
  purchasePrice:  { x: 96,  y: 267, width: 185, height: 30 },
  judgment:       { x: 398, y: 267, width: 112, height: 30 },
  excess:         { x: 554, y: 267, width: 64,  height: 30 },

  purchaser:      { x: 96,  y: 298, width: 405, height: 30 }
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

function makeRunSignature(record) {
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

/**
 * Strict validation gate to prevent bad OCR rows.
 */
function isRecordUsable(record) {
  return (
    looksLikeOwner(record.owner) &&
    looksLikeAddress(record.propertyAddress) &&
    looksLikeDate(record.dateSold) &&
    looksLikeMoney(record.purchasePrice) &&
    looksLikeMoney(record.judgment) &&
    looksLikeMoney(record.excess) &&
    looksLikePurchaser(record.purchaser)
  );
}

function scoreRecord(record) {
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

async function ensureDir(dir) {
  await fs.promises.mkdir(dir, { recursive: true });
}

function imageHash(filePath) {
  const buf = fs.readFileSync(filePath);
  return crypto.createHash('md5').update(buf).digest('hex');
}

async function capturePage(page, index) {
  await ensureDir(DEBUG_DIR);

  const rawPath = path.join(
    DEBUG_DIR,
    `record-${String(index).padStart(4, '0')}-raw.png`
  );

  await page.screenshot({
    path: rawPath,
    fullPage: false
  });

  return rawPath;
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

  if (
    !rect ||
    !Number.isFinite(rect.x) ||
    !Number.isFinite(rect.y) ||
    !Number.isFinite(rect.width) ||
    !Number.isFinite(rect.height)
  ) {
    return false;
  }

  const meta = await sharp(inputPath).metadata();
  const imgW = meta.width || 0;
  const imgH = meta.height || 0;

  if (imgW <= 0 || imgH <= 0) {
    return false;
  }

  const rawLeft = Math.round(rect.x - pad);
  const rawTop = Math.round(rect.y - pad);
  const rawWidth = Math.round(rect.width + pad * 2);
  const rawHeight = Math.round(rect.height + pad * 2);

  if (rawWidth <= 0 || rawHeight <= 0) {
    return false;
  }

  if (rawLeft >= imgW || rawTop >= imgH) {
    return false;
  }

  const left = Math.max(0, rawLeft);
  const top = Math.max(0, rawTop);

  const maxWidth = imgW - left;
  const maxHeight = imgH - top;

  const width = Math.min(maxWidth, rawWidth - Math.max(0, left - rawLeft));
  const height = Math.min(maxHeight, rawHeight - Math.max(0, top - rawTop));

  if (width <= 1 || height <= 1) {
    return false;
  }

  let img = sharp(inputPath).extract({ left, top, width, height });

  if (grayscale) img = img.grayscale();
  if (normalize) img = img.normalize();

  img = img.resize({
    width: Math.max(2, width * enlarge),
    height: Math.max(2, height * enlarge),
    fit: 'fill'
  });

  if (sharpen) img = img.sharpen();

  if (Number.isFinite(threshold)) {
    img = img.threshold(threshold);
  }

  img = img.png();

  await img.toFile(outputPath);
  return true;
}

async function cropForm(rawPath, index) {
  const formPath = path.join(
    DEBUG_DIR,
    `record-${String(index).padStart(4, '0')}-form-base.png`
  );

  const ok = await cropImage(rawPath, formPath, FORM_RECT, {
    threshold: null,
    enlarge: 1,
    pad: 0,
    grayscale: false,
    normalize: false,
    sharpen: false
  });

  if (!ok) {
    throw new Error('Failed to create form crop.');
  }

  return formPath;
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

function chooseBestOCR(results, type) {
  const vals = results.map(v => clean(v)).filter(Boolean);
  if (!vals.length) return '';

  if (type === 'money') {
    const valid = vals.filter(looksLikeMoney);
    if (valid.length) return valid.sort((a, b) => b.length - a.length)[0];
    return vals.sort((a, b) => b.length - a.length)[0];
  }

  if (type === 'date') {
    const valid = vals.filter(looksLikeDate);
    if (valid.length) return valid[0];
    return vals.sort((a, b) => b.length - a.length)[0];
  }

  if (type === 'state') {
    const valid = vals
      .map(v => v.replace(/[^A-Za-z]/g, '').toUpperCase())
      .filter(v => /^[A-Z]{2}$/.test(v));
    if (valid.length) return valid[0];
    return vals[0];
  }

  return vals.sort((a, b) => b.length - a.length)[0];
}

async function ocrField(formPath, fieldName, rect, type, ocrOpts, variants = []) {
  const tries = variants.length
    ? variants
    : [
        { threshold: 165, enlarge: 3, pad: 4 },
        { threshold: 180, enlarge: 3, pad: 4 },
        { threshold: 195, enlarge: 4, pad: 4 }
      ];

  const outputs = [];

  for (let i = 0; i < tries.length; i++) {
    const outPath = path.join(DEBUG_DIR, `${fieldName}-v${i + 1}.png`);
    const ok = await cropImage(formPath, outPath, rect, tries[i]).catch(() => false);

    if (!ok) {
      outputs.push('');
      continue;
    }

    const text = await runTesseractText(outPath, ocrOpts).catch(() => '');
    outputs.push(stripLeadingNoise(text));
  }

  return chooseBestOCR(outputs, type);
}

async function extractRecordFromForm(formPath, index) {
  const NAME = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789&.,#/- ';
  const ADDRESS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789#-.,/ ';
  const MONEY = '0123456789$,.';
  const DATE = '0123456789/.';

  const owner = await ocrField(
    formPath,
    `record-${String(index).padStart(4, '0')}-owner`,
    FORM_FIELD_RECTS.owner,
    'text',
    { psm: 7, whitelist: NAME }
  );

  const propertyStreet = await ocrField(
    formPath,
    `record-${String(index).padStart(4, '0')}-propertyStreet`,
    FORM_FIELD_RECTS.propertyStreet,
    'text',
    { psm: 7, whitelist: ADDRESS }
  );

  const propertyCity = await ocrField(
    formPath,
    `record-${String(index).padStart(4, '0')}-propertyCity`,
    FORM_FIELD_RECTS.propertyCity,
    'text',
    { psm: 7, whitelist: ADDRESS }
  );

  const propertyState = await ocrField(
    formPath,
    `record-${String(index).padStart(4, '0')}-propertyState`,
    FORM_FIELD_RECTS.propertyState,
    'state',
    { psm: 7, whitelist: 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' }
  );

  const dateSold = await ocrField(
    formPath,
    `record-${String(index).padStart(4, '0')}-dateSold`,
    FORM_FIELD_RECTS.dateSold,
    'date',
    { psm: 7, whitelist: DATE }
  );

  const purchasePrice = await ocrField(
    formPath,
    `record-${String(index).padStart(4, '0')}-purchasePrice`,
    FORM_FIELD_RECTS.purchasePrice,
    'money',
    { psm: 7, whitelist: MONEY }
  );

  const judgment = await ocrField(
    formPath,
    `record-${String(index).padStart(4, '0')}-judgment`,
    FORM_FIELD_RECTS.judgment,
    'money',
    { psm: 7, whitelist: MONEY }
  );

  const excess = await ocrField(
    formPath,
    `record-${String(index).padStart(4, '0')}-excess`,
    FORM_FIELD_RECTS.excess,
    'money',
    { psm: 7, whitelist: MONEY }
  );

  const purchaser = await ocrField(
    formPath,
    `record-${String(index).padStart(4, '0')}-purchaser`,
    FORM_FIELD_RECTS.purchaser,
    'text',
    { psm: 7, whitelist: NAME }
  );

  const record = {
    owner: cleanupName(owner),
    propertyAddress: clean(
      [
        cleanupAddressPart(propertyStreet),
        cleanupAddressPart(propertyCity),
        cleanupAddressPart(propertyState)
      ].filter(Boolean).join(' ')
    ),
    dateSold: cleanupDate(dateSold),
    judgment: cleanupMoney(judgment),
    purchasePrice: cleanupMoney(purchasePrice),
    excess: cleanupMoney(excess),
    purchaser: cleanupName(purchaser)
  };

  const quality = scoreRecord(record);

  const parsedPath = path.join(
    DEBUG_DIR,
    `record-${String(index).padStart(4, '0')}-parsed.json`
  );
  await fs.promises.writeFile(
    parsedPath,
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

        await locator.click({ timeout: 5000 }).catch(() => null);
        await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => null);
        return true;
      }
    } catch (_) {}
  }

  // Fallback click point for the visible "Next" button at 1280x900
  try {
    await page.mouse.click(470, 305);
    await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => null);
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
  const seenPageHashes = new Set();
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
      const rawPath = await capturePage(page, i);
      const pageHash = imageHash(rawPath);

      if (seenPageHashes.has(pageHash)) {
        console.log('Detected repeated page screenshot. Stopping.');
        break;
      }
      seenPageHashes.add(pageHash);

      const formPath = await cropForm(rawPath, i);
      const { record, quality } = await extractRecordFromForm(formPath, i);

      if (isRecordUsable(record)) {
        const sig = makeRunSignature(record);

        if (!seenRecordSigs.has(sig)) {
          seenRecordSigs.add(sig);
          records.push(record);

          console.log(
            `[${records.length}] quality=${quality} | ` +
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
          `Rejected OCR record [${i}] due to failing strict validation (quality=${quality}).`
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
  if (!SHEET_ID) {
    throw new Error('Missing SHEET_ID environment variable.');
  }

  if (!SHEET_NAME) {
    throw new Error('Missing SHEET_NAME environment variable.');
  }

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
