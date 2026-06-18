const fs = require('fs');
const path = require('path');
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

function clean(value) {
  return String(value || '')
    .replace(/\u00A0/g, ' ')
    .replace(/[|]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function sanitizeMultiline(text) {
  return String(text || '')
    .replace(/\r/g, '')
    .replace(/\u00A0/g, ' ')
    .split('\n')
    .map(line => clean(line))
    .filter(Boolean)
    .join('\n');
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

function normalizeMoney(v) {
  const value = clean(v);
  if (!value) return '';
  if (/^\$/.test(value)) return value;
  if (/^\d[\d,]*\.?\d*$/.test(value)) return `$${value}`;
  return value;
}

function normalizeDate(v) {
  return clean(v);
}

async function ensureDir(dir) {
  await fs.promises.mkdir(dir, { recursive: true });
}

async function preprocessImage(inputPath, outputPath) {
  await sharp(inputPath)
    .grayscale()
    .normalize()
    .sharpen()
    .threshold(180)
    .png()
    .toFile(outputPath);
}

async function runTesseract(imagePath) {
  // stdout output mode:
  // tesseract image stdout --psm 6 -l eng
  const { stdout, stderr } = await execFileAsync('tesseract', [
    imagePath,
    'stdout',
    '--psm', '6',
    '-l', 'eng',
    '-c', 'preserve_interword_spaces=1'
  ]);

  if (stderr) {
    // Tesseract often emits diagnostic stderr even on success; log only
    console.log('Tesseract stderr:', stderr);
  }

  return sanitizeMultiline(stdout);
}

function getLineValue(lines, label) {
  const normalizedLabel = clean(label).toLowerCase();

  for (let i = 0; i < lines.length; i++) {
    const line = clean(lines[i]);
    const lower = line.toLowerCase();

    if (lower.startsWith(normalizedLabel)) {
      // Same-line value: "Owner HUDSON BILLY WAYNE"
      let sameLineValue = clean(line.slice(label.length));
      if (sameLineValue) return sameLineValue;

      // Next non-empty line value
      for (let j = i + 1; j < lines.length; j++) {
        const next = clean(lines[j]);
        if (!next) continue;
        return next;
      }
    }
  }

  return '';
}

function getPropertyAddress(lines) {
  // We expect:
  // Property Address
  // 8818 E ANDERSON AVE
  // INDEPENDENCE
  // MO
  const idx = lines.findIndex(line => clean(line).toLowerCase().startsWith('property address'));
  if (idx === -1) return '';

  const parts = [];
  for (let i = idx + 1; i < Math.min(lines.length, idx + 6); i++) {
    const v = clean(lines[i]);

    if (!v) continue;
    if (/^(date sold|purchase price|owner|co-owner|legal description|confirmation|purchaser)$/i.test(v)) break;

    parts.push(v);

    // usually street + city + state
    if (parts.length >= 3) break;
  }

  return clean(parts.join(' '));
}

function getPurchaser(lines) {
  const idx = lines.findIndex(line => clean(line).toLowerCase().startsWith('purchaser'));
  if (idx === -1) return '';

  // same-line
  const sameLine = clean(lines[idx].slice('Purchaser'.length));
  if (sameLine) return sameLine;

  for (let i = idx + 1; i < Math.min(lines.length, idx + 4); i++) {
    const v = clean(lines[i]);
    if (!v) continue;
    if (/^(address|city|state|zip|confirmation|excess proceeds)$/i.test(v)) break;
    return v;
  }

  return '';
}

function parseOcrText(ocrText) {
  const lines = sanitizeMultiline(ocrText).split('\n').map(clean).filter(Boolean);

  const record = {
    suitNo: getLineValue(lines, 'Suit No'),
    parcelNo: getLineValue(lines, 'Parcel No'),
    owner: getLineValue(lines, 'Owner'),
    propertyAddress: getPropertyAddress(lines),
    dateSold: getLineValue(lines, 'Date Sold'),
    purchasePrice: getLineValue(lines, 'Purchase Price'),
    judgment: getLineValue(lines, 'Judgment'),
    excess: getLineValue(lines, 'Excess'),
    purchaser: getPurchaser(lines)
  };

  // Cleanup typical OCR noise
  if (record.purchasePrice) record.purchasePrice = normalizeMoney(record.purchasePrice);
  if (record.judgment) record.judgment = normalizeMoney(record.judgment);
  if (record.excess) record.excess = normalizeMoney(record.excess);
  if (record.dateSold) record.dateSold = normalizeDate(record.dateSold);

  return record;
}

async function captureAndReadRecord(page, index) {
  await ensureDir(DEBUG_DIR);

  const rawPath = path.join(DEBUG_DIR, `record-${String(index).padStart(4, '0')}-raw.png`);
  const processedPath = path.join(DEBUG_DIR, `record-${String(index).padStart(4, '0')}-processed.png`);
  const textPath = path.join(DEBUG_DIR, `record-${String(index).padStart(4, '0')}.txt`);

  // Since extraction should not depend on DOM structure, capture main visible viewport.
  // We use a fixed clip that covers the rendered form region shown in your screenshot.
  await page.screenshot({
    path: rawPath,
    fullPage: false,
    clip: {
      x: 0,
      y: 0,
      width: 1280,
      height: 900
    }
  });

  await preprocessImage(rawPath, processedPath);
  const ocrText = await runTesseract(processedPath);
  await fs.promises.writeFile(textPath, ocrText, 'utf8');

  const record = parseOcrText(ocrText);

  console.log(`OCR text snapshot [${index}]:`);
  console.log(ocrText.slice(0, 1500));

  console.log(`Parsed record [${index}]:`, JSON.stringify(record, null, 2));

  return record;
}

async function findAndClickNext(page) {
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
        await locator.click({ timeout: 8000 });
        return true;
      }
    } catch (_) {}
  }

  // Coordinate fallback (based on visible layout position from your screenshot)
  try {
    await page.mouse.click(288, 85);
    return true;
  } catch (_) {}

  return false;
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
  const set = new Set();

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
    set.add([
      clean(record.propertyAddress).toUpperCase(),
      clean(record.owner).toUpperCase(),
      clean(record.dateSold).toUpperCase(),
      clean(record.judgment).toUpperCase(),
      clean(record.purchasePrice).toUpperCase(),
      clean(record.excess).toUpperCase(),
      clean(record.purchaser).toUpperCase()
    ].join(' | '));
  }

  return set;
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

async function appendRowsToSheet(sheets, records) {
  if (!records.length) {
    console.log('No new rows to append.');
    return;
  }

  const values = records.map(r => [
    clean(r.propertyAddress),       // B
    clean(r.owner),                 // C
    '',                             // D blank
    clean(r.dateSold),              // E
    clean(r.judgment),              // F
    clean(r.purchasePrice),         // G
    clean(r.excess),                // H
    clean(r.purchaser)              // I
  ]);

  await sheets.spreadsheets.values.append({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!B2:I`,
    valueInputOption: 'USER_ENTERED',
    insertDataOption: 'INSERT_ROWS',
    requestBody: {
      values
    }
  });

  console.log(`Appended ${values.length} new row(s) to Google Sheets.`);
}

async function scrapeAllParcels() {
  await ensureDir(DEBUG_DIR);

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({
    viewport: { width: 1280, height: 900 }
  });

  const records = [];
  const seenRecordSigs = new Set();
  const seenPageSigs = new Set();

  try {
    console.log('Opening:', TARGET_URL);

    await page.goto(TARGET_URL, {
      waitUntil: 'domcontentloaded',
      timeout: 60000
    });

    await page.waitForTimeout(5000);

    const MAX_PAGES = Number(process.env.MAX_PAGES || 5000);

    for (let i = 1; i <= MAX_PAGES; i++) {
      await page.waitForTimeout(2500);

      const record = await captureAndReadRecord(page, i);
      const sig = makeSignature(record);

      if (seenPageSigs.has(sig) && sig.replace(/\s+\|\s+/g, '') !== '') {
        console.log('Detected repeated OCR signature. Stopping.');
        break;
      }

      if (sig.replace(/\s+\|\s+/g, '') !== '') {
        seenPageSigs.add(sig);
      }

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

      const clicked = await findAndClickNext(page);
      if (!clicked) {
        console.log('Next button not found/clickable. Stopping.');
        break;
      }

      await page.waitForTimeout(3000);
    }

    console.log(`Scraping complete. Collected ${records.length} unique record(s).`);
    return records;
  } finally {
    await page.close().catch(() => null);
    await browser.close().catch(() => null);
  }
}

async function main() {
  console.log('Starting scraper...');

  const records = await scrapeAllParcels();

  if (!records.length) {
    console.log('No records scraped. Exiting without Sheets update.');
    return;
  }

  const sheets = await getGoogleSheetsClient();
  const existingSignatures = await getExistingSignatures(sheets);

  const newRecords = records.filter(r => !existingSignatures.has(makeSheetSignature(r)));

  console.log(`Existing sheet signatures: ${existingSignatures.size}`);
  console.log(`New records to append: ${newRecords.length}`);

  await appendRowsToSheet(sheets, newRecords);

  console.log('Done.');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
