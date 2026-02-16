// fillWorksheet.cjs
// CommonJS script: anchor-aware PDF filler using pdfjs-dist + pdf-lib + pdf-parse
// Install: npm install pdf-lib pdf-parse pdfjs-dist fs
//
// Inputs (repo root): Summary.pdf (optional), worksheet_template_tssf.pdf (required)
// Outputs: filled_worksheet.pdf, parsed-summary.txt, parsed-summary.json, anchors-debug.json

const fs = require('fs');
const path = require('path');
const { PDFDocument, rgb, StandardFonts } = require('pdf-lib');

// dynamic import helpers
async function loadPdfParse() {
  const mod = await import('pdf-parse');
  return mod.default || mod;
}
// resilient loader for pdfjs-dist (CommonJS script)
async function loadPdfJs() {
  const tryImport = async (p) => {
    try {
      const mod = await import(p);
      return mod.default || mod;
    } catch (e) {
      return null;
    }
  };

  // Try common ESM import paths (some versions expose different paths)
  const candidates = [
    'pdfjs-dist/legacy/build/pdf.js',
    'pdfjs-dist/build/pdf.js',
    'pdfjs-dist/legacy/build/pdf.node.js',
    'pdfjs-dist/build/pdf'
  ];

  for (const p of candidates) {
    const lib = await tryImport(p);
    if (lib) return lib;
  }

  // Try require (CJS) as a last resort (works in many CI/node setups)
  try {
    // eslint-disable-next-line global-require, import/no-dynamic-require
    const req = require('pdfjs-dist/legacy/build/pdf.js');
    return req;
  } catch (e) {
    // try alternate require path
    try {
      // eslint-disable-next-line global-require, import/no-dynamic-require
      const req2 = require('pdfjs-dist/build/pdf.js');
      return req2;
    } catch (err) {
      // final failure
      throw new Error('pdfjs-dist not found. Install pdfjs-dist and ensure node_modules is present on the runner.');
    }
  }
}

function safeTrim(s) {
  return s ? String(s).trim() : '';
}

function extractFromText(text) {
  const t = text || '';
  const find = (re) => {
    const m = t.match(re);
    return m ? safeTrim(m[1]) : '';
  };

  return {
    deedHolders: find(/DEED HOLDER.*?\n\s*([^\n]+)/i),
    deedBook: find(/OR Book\s*([0-9A-Za-z\-]+)/i),
    deedPage: find(/Page.*?\s*([0-9]+)/i),
    deedDate: find(/Recorded Date:\s*([0-9\/\-]+)/i),
    caseNo: find(/Case No[:.]?\s*([A-Z0-9\-]+)/i),
    estimatedSurplus: find(/Estimated Surplus[:\s]*\$?\s*([0-9,\.]+)/i),
    county: find(/County[:\s]*([A-Za-z\- ]+)/i),
    state: find(/State[:\s]*([A-Za-z\- ]+)/i),
    auctionDate: find(/Auction \/ Sale Date[:\s]*([A-Za-z0-9 ,]+)/i),
    salesPrice: find(/Sold Amount[:\s]*\$?\s*([0-9,\.]+)/i),
    openingBid: find(/Opening \/ Minimum Bid[\s\S]*?Amount[:\s]*\$?\s*([0-9,\.]+)/i),
    bidSource: find(/WHERE DID YOU GET YOUR OPENING BID LIST FROM.*?\n\s*([^\n]+)/i),
    foreclosingEntity: find(/FORECLOSING ENTITY.*?\n\s*([^\n]+)/i),
    propertyAddressLine1: find(/Site Address[:\s]*\n\s*([^\n]+)/i),
    propertyAddressLine2: find(/\n\s*(UNIT\s*[0-9A-Za-z\-]+)/i),
    propertyCityStateZip: find(/([A-Za-z ]+,\s*[A-Z]{2}\s*[0-9\-]+)/i),
    dateFileReviewed: find(/Submission Date[:\s]*([0-9\/\-]+)/i),
    researcher: find(/Researcher Name[:\s]*\n?\s*([^\n]+)/i)
  };
}

async function parseSummaryPdf() {
  const pdfParse = await loadPdfParse();
  if (!fs.existsSync('Summary.pdf')) {
    throw new Error('Summary.pdf not found');
  }
  const buf = fs.readFileSync('Summary.pdf');
  const parsed = await pdfParse(buf);
  const text = parsed.text || '';
  fs.writeFileSync('parsed-summary.txt', text, 'utf8');
  const extracted = extractFromText(text);
  fs.writeFileSync('parsed-summary.json', JSON.stringify(extracted, null, 2), 'utf8');
  return { extracted, rawText: text };
}

function fallback() {
  return {
    extracted: {
      deedHolders: 'SALOMON COHEN SALMUN and RUTH BLANCA SMEKE DE COHEN',
      deedBook: 'OR Book 25446',
      deedPage: '0422',
      deedDate: '2/01/2007',
      caseNo: '2025A00491',
      estimatedSurplus: '$306,151.89',
      county: 'Miami-Dade',
      state: 'Florida',
      auctionDate: 'November 13, 2025',
      salesPrice: '$338,200.00',
      openingBid: '$27,346.37',
      bidSource: 'Realforeclose.com',
      foreclosingEntity: 'Miami-Dade County',
      propertyAddressLine1: '19195 NE 36 CT',
      propertyAddressLine2: 'UNIT 1508',
      propertyCityStateZip: 'Aventura, FL 33180-4502',
      dateFileReviewed: '2/13/2026',
      researcher: 'Simon Roach'
    },
    rawText: ''
  };
}

/**
 * Use pdfjs-dist to extract text items with coordinates for all pages.
 * Returns array of { page, str, x, y, width, height, transform }.
 */
async function extractTextItemsWithCoords(pdfPath) {
  const pdfjs = await loadPdfJs();
  const data = new Uint8Array(fs.readFileSync(pdfPath));
  const loadingTask = pdfjs.getDocument({ data });
  const pdf = await loadingTask.promise;
  const items = [];

  for (let p = 1; p <= pdf.numPages; p++) {
    const page = await pdf.getPage(p);
    const textContent = await page.getTextContent();
    for (const item of textContent.items) {
      // transform: [a, b, c, d, tx, ty]
      const tx = item.transform[4];
      const ty = item.transform[5];
      // width/height are not always present; approximate using transform and fontSize
      const fontSize = Math.hypot(item.transform[0], item.transform[1]);
      const width = item.width || (String(item.str).length * fontSize * 0.5);
      const height = item.height || fontSize;
      items.push({
        page: p,
        str: item.str,
        x: tx,
        y: ty,
        width,
        height,
        transform: item.transform
      });
    }
  }
  return items;
}

/**
 * Find anchors: label followed by underscores or long whitespace in same text item or adjacent items.
 * Returns map: { labelNormalized: { page, x, y, placeholderWidth } }
 */
function findAnchorsFromItems(items) {
  const anchors = [];
  const labelRegex = /([A-Za-z0-9 &\-\(\)\/\.]{2,60}?:)\s*(_{3,}|\.{3,}|\s{4,})/;
  // Also detect patterns where label and placeholder are separate items on same line
  for (let i = 0; i < items.length; i++) {
    const it = items[i];
    const m = labelRegex.exec(it.str);
    if (m) {
      const label = m[1].replace(/:$/, '').trim();
      const placeholder = m[2];
      const placeholderWidth = (placeholder.length * 6) + 10;
      anchors.push({
        label,
        page: it.page,
        x: it.x + (it.width || 0),
        y: it.y,
        placeholderWidth
      });
      continue;
    }
    // check adjacent item on same line (y within tolerance)
    const next = items[i + 1];
    if (next && Math.abs(next.y - it.y) < 4) {
      // if current looks like label and next is underscores or long whitespace
      if (/[:\s]$/.test(it.str) && (/^_{3,}|^\.{3,}|\s{4,}/.test(next.str))) {
        const label = it.str.replace(/:$/, '').trim();
        const placeholderWidth = (next.str.length * 6) + 10;
        anchors.push({
          label,
          page: it.page,
          x: it.x + (it.width || 0),
          y: it.y,
          placeholderWidth
        });
      }
    }
  }
  // Normalize labels (simple normalization)
  const map = {};
  for (const a of anchors) {
    const key = a.label.replace(/\s+/g, ' ').trim().toLowerCase();
    map[key] = { page: a.page, x: a.x, y: a.y, placeholderWidth: a.placeholderWidth, label: a.label };
  }
  return map;
}

/**
 * Draw text with font scaling to fit placeholderWidth.
 * Returns used font size.
 */
function drawTextFit(page, font, text, startX, y, maxWidth, options = {}) {
  const baseSize = options.baseSize || 11;
  const minSize = options.minSize || 6;
  let size = baseSize;
  // measure using pdf-lib font metrics
  const measureWidth = (s, sz) => font.widthOfTextAtSize(s, sz);
  while (size >= minSize && measureWidth(text, size) > maxWidth) {
    size -= 0.5;
  }
  // if still too wide, truncate with ellipsis
  let finalText = text;
  if (measureWidth(finalText, size) > maxWidth) {
    while (finalText.length > 0 && measureWidth(finalText + '…', size) > maxWidth) {
      finalText = finalText.slice(0, -1);
    }
    finalText = finalText + '…';
  }
  page.drawText(finalText, { x: startX, y, size, font, color: rgb(0, 0, 0) });
  return size;
}

/**
 * Primary fill function:
 *  - extracts anchors from template using pdfjs-dist
 *  - maps parsed values to anchors by normalized label matching
 *  - falls back to coordinate map if anchor not found
 */
async function fillPdf(values) {
  const template = 'worksheet_template_tssf.pdf';
  if (!fs.existsSync(template)) {
    throw new Error('Template not found: ' + template);
  }

  // 1) extract text items with coords
  let items = [];
  try {
    items = await extractTextItemsWithCoords(template);
  } catch (err) {
    console.warn('pdfjs-dist extraction failed, will use coordinate fallback:', err.message);
  }

  // 2) find anchors
  const anchorsMap = items.length ? findAnchorsFromItems(items) : {};

  // 3) load pdf-lib doc
  const bytes = fs.readFileSync(template);
  const pdfDoc = await PDFDocument.load(bytes);
  const helvetica = await pdfDoc.embedFont(StandardFonts.Helvetica);

  // fallback coordinate map (kept from your baseline)
  const fallbackCoords = {
    deedHolders: { page: 1, x: 220, y: 740 },
    deedDate: { page: 1, x: 220, y: 720 },
    deedBook: { page: 1, x: 220, y: 700 },
    deedPage: { page: 1, x: 320, y: 700 },
    caseNo: { page: 1, x: 220, y: 670 },
    estimatedSurplus: { page: 1, x: 220, y: 650 },
    county: { page: 1, x: 220, y: 630 },
    state: { page: 1, x: 320, y: 630 },
    auctionDate: { page: 1, x: 220, y: 610 },
    salesPrice: { page: 1, x: 220, y: 590 },
    openingBid: { page: 1, x: 220, y: 570 },
    bidSource: { page: 1, x: 220, y: 550 },
    foreclosingEntity: { page: 1, x: 220, y: 530 },
    propertyAddressLine1: { page: 1, x: 220, y: 510 },
    propertyAddressLine2: { page: 1, x: 220, y: 495 },
    propertyCityStateZip: { page: 1, x: 220, y: 480 },
    dateFileReviewed: { page: 1, x: 220, y: 460 },
    researcher: { page: 1, x: 220, y: 440 }
  };

  // 4) mapping of keys to likely label text variants (normalization)
  const labelCandidates = {
    deedHolders: ['deed holder(s) at time of foreclosure', 'deed holders at time of foreclosure', 'deed holder'],
    deedBook: ['book and page', 'or book', 'book'],
    deedPage: ['page', 'page no', 'page no.'],
    deedDate: ['deed recordation date', 'recorded date', 'recordation date'],
    caseNo: ['case no', 'file or case number', 'case no.'],
    estimatedSurplus: ['estimated surplus amount', 'estimated surplus'],
    county: ['county'],
    state: ['state'],
    auctionDate: ['auction date', 'auction / sale date'],
    salesPrice: ['sold amount', 'sales price at auction', 'sold amount:'],
    openingBid: ['opening / minimum bid', 'opening bid', 'opening / minimum bid amount'],
    bidSource: ['where did you get your opening bid list from', 'opening bid list from'],
    foreclosingEntity: ['foreclosing entity'],
    propertyAddressLine1: ['foreclosed property address', 'site address'],
    propertyAddressLine2: ['unit', 'unit #'],
    propertyCityStateZip: ['city', 'state', 'zip', 'city, state zip'],
    dateFileReviewed: ['submission date', 'date file reviewed'],
    researcher: ['researcher name', 'researcher']
  };

  // helper to find anchor by candidates
  function findAnchorForKey(key) {
    const cands = labelCandidates[key] || [];
    for (const cand of cands) {
      const k = cand.replace(/\s+/g, ' ').trim().toLowerCase();
      if (anchorsMap[k]) return anchorsMap[k];
    }
    return null;
  }

  // 5) iterate keys and draw either at anchor or fallback coords
  const pages = pdfDoc.getPages();
  const debugAnchors = [];

  for (const key of Object.keys(labelCandidates)) {
    const value = values[key] || '';
    if (!value) continue;

    const anchor = findAnchorForKey(key);
    if (anchor) {
      const pageIndex = Math.max(0, anchor.page - 1);
      const page = pages[pageIndex];
      // pdfjs y origin is bottom; pdf-lib uses same origin; small y offset to align baseline
      const startX = anchor.x + 4;
      const startY = anchor.y - 2;
      const usedSize = drawTextFit(page, helvetica, value, startX, startY, anchor.placeholderWidth, { baseSize: 11, minSize: 6 });
      debugAnchors.push({ key, method: 'anchor', label: anchor.label, page: anchor.page, x: startX, y: startY, placeholderWidth: anchor.placeholderWidth, usedSize });
    } else if (fallbackCoords[key]) {
      const fc = fallbackCoords[key];
      const page = pages[fc.page - 1];
      drawTextFit(page, helvetica, value, fc.x, fc.y, 300, { baseSize: 11, minSize: 6 });
      debugAnchors.push({ key, method: 'fallback', page: fc.page, x: fc.x, y: fc.y });
    } else {
      debugAnchors.push({ key, method: 'skipped', reason: 'no anchor or fallback' });
    }
  }

  // 6) Save debug artifacts and output
  fs.writeFileSync('anchors-debug.json', JSON.stringify(debugAnchors, null, 2), 'utf8');

  const out = 'filled_worksheet.pdf';
  const pdfBytes = await pdfDoc.save();
  fs.writeFileSync(out, pdfBytes);
  return out;
}

(async function main() {
  try {
    let parsed;
    try {
      parsed = await parseSummaryPdf();
      const nonEmpty = Object.values(parsed.extracted).filter(Boolean).length;
      if (nonEmpty < 6) {
        console.warn('Parsed too few fields; using fallback values');
        parsed = fallback();
      }
    } catch (err) {
      console.warn('Parsing failed; using fallback values:', err.message);
      parsed = fallback();
    }

    const outPath = await fillPdf(parsed.extracted);
    console.log('✅ Worksheet filled successfully:', outPath);
  } catch (err) {
    console.error('❌ Fatal error:', err.message);
    process.exitCode = 1;
  }
})();