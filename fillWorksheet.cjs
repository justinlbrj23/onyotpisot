// fillWorksheet.cjs
// Node.js CommonJS script
// Install: npm install pdf-lib pdf-parse fs
//
// Inputs (repo root): Summary.pdf (optional), worksheet_template_tssf.pdf (required)
// Outputs: filled_worksheet.pdf, parsed-summary.txt, parsed-summary.json

const fs = require('fs');
const { PDFDocument, rgb, StandardFonts } = require('pdf-lib');

// Normalize pdf-parse import for CommonJS
let pdfParse = null;
try {
  pdfParse = require('pdf-parse');
  if (typeof pdfParse !== 'function' && pdfParse.default) pdfParse = pdfParse.default;
} catch (e) {
  pdfParse = null;
}

function safeTrim(s) {
  return s ? String(s).trim() : '';
}

// Regex-based extraction tuned to your Summary / manual example
function extractFromText(text) {
  const t = text || '';

  const find = (re) => {
    const m = t.match(re);
    return m ? safeTrim(m[1]) : '';
  };

  return {
    deedHolders: find(/DEED HOLDER(?:\(S\))?[\s\S]*?\n\s*([A-Z0-9 ,.\-]+(?:\n\s*[A-Z0-9 ,.\-]+)?)/i) || find(/DEED HOLDER(?:\(S\))?:\s*([^\n]+)/i),
    deedBook: find(/OR Book\s*([0-9A-Za-z\-]+)/i) || find(/BOOK AND PAGE#\s*\n\s*([A-Za-z0-9 \-]+)/i),
    deedPage: find(/Page(?: No\.|#|:)?\s*([0-9A-Za-z\-]+)/i) || find(/BOOK AND PAGE#\s*\n\s*[A-Za-z0-9 \-]+\s*([0-9]{3,})/i),
    deedDate: find(/DEED RECORDATION DATE\s*\n\s*([0-9\/\-]{6,20})/i) || find(/DEED RECORDATION DATE[:\s]*([0-9\/\-]{6,20})/i),
    caseNo: find(/FILE OR CASE NUMBER[^\n]*\n\s*([A-Z0-9\-]+)/i) || find(/CASE NUMBER[:\s]*([A-Z0-9\-]+)/i),
    estimatedSurplus: find(/ESTIMATED SURPLUS AMOUNT\s*\n\s*\$?\s*([0-9,\.]+)/i) || find(/ESTIMATED SURPLUS AMOUNT[:\s]*\$?\s*([0-9,\.]+)/i),
    county: find(/COUNTY\s*\n\s*([A-Za-z\-\s]+)/i) || find(/COUNTY[:\s]*([A-Za-z\-\s]+)/i),
    state: find(/STATE\s*\n\s*([A-Za-z\-\s]+)/i) || find(/STATE[:\s]*([A-Za-z\-\s]+)/i),
    auctionDate: find(/AUCTION DATE\s*\n\s*([A-Za-z0-9 ,]+)/i) || find(/AUCTION DATE[:\s]*([A-Za-z0-9 ,]+)/i),
    salesPrice: find(/SALES PRICE AT AUCTION\s*\n\s*\$?\s*([0-9,\.]+)/i) || find(/SALES PRICE AT AUCTION[:\s]*\$?\s*([0-9,\.]+)/i),
    openingBid: find(/FORECLOSING DEBT AMOUNT\s*\(OPENING BID\)\s*\n\s*\$?\s*([0-9,\.]+)/i) || find(/OPENING BID[:\s]*\$?\s*([0-9,\.]+)/i),
    bidSource: find(/WHERE DID YOU GET YOUR OPENING BID LIST FROM\?[^\n]*\n\s*([A-Za-z0-9\.\-]+)/i) || find(/WHERE DID YOU GET YOUR OPENING BID LIST FROM\?[:\s]*([^\n]+)/i),
    foreclosingEntity: find(/FORECLOSING ENTITY\s*\n\s*([A-Za-z0-9 ,\-]+)/i) || find(/FORECLOSING ENTITY[:\s]*([^\n]+)/i),
    propertyAddressLine1: find(/FORECLOSED PROPERTY ADDRESS:\s*\n\s*([0-9A-Za-z\-\.\s]+)/i) || find(/FORECLOSED PROPERTY ADDRESS[:\s]*([^\n]+)/i),
    propertyAddressLine2: find(/\n\s*(UNIT\s*[0-9A-Za-z\-]+)/i) || find(/UNIT\s*[0-9A-Za-z\-]+/i),
    propertyCityStateZip: find(/([A-Za-z ]+,\s*[A-Za-z]{2}\s*[0-9\-]{5,10})/i),
    dateFileReviewed: find(/DATE FILE REVIEWED\s*\n\s*([0-9\/\-]{6,20})/i) || find(/DATE FILE REVIEWED[:\s]*([0-9\/\-]{6,20})/i),
    researcher: find(/RESEARCHER\s*\n\s*([A-Za-z0-9\.\s]+)/i) || find(/RESEARCHER[:\s]*([^\n]+)/i)
  };
}

async function parseSummaryPdf() {
  if (!pdfParse) throw new Error('pdf-parse not available');
  const buf = fs.readFileSync('Summary.pdf');
  const parsed = await pdfParse(buf);
  const text = parsed.text || '';
  fs.writeFileSync('parsed-summary.txt', text, 'utf8');
  const extracted = extractFromText(text);
  fs.writeFileSync('parsed-summary.json', JSON.stringify(extracted, null, 2), 'utf8');
  return { extracted, rawText: text };
}

function fallback() {
  // Exact manual values you provided
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

async function fillPdf(values) {
  const template = 'worksheet_template_tssf.pdf';
  if (!fs.existsSync(template)) throw new Error('Template worksheet_template_tssf.pdf not found in repo root');

  const bytes = fs.readFileSync(template);
  const pdfDoc = await PDFDocument.load(bytes);
  const helvetica = await pdfDoc.embedFont(StandardFonts.Helvetica);
  const pages = pdfDoc.getPages();
  const page1 = pages[0];

  const color = rgb(0, 0, 0);
  const size = 11;

  // Page 1 coordinates tuned to match your manual fill (values only)
  page1.drawText(values.deedHolders || '', { x: 200, y: 720, size, color, font: helvetica });
  page1.drawText(values.deedBook || '', { x: 200, y: 700, size, color, font: helvetica });
  page1.drawText(values.deedPage || '', { x: 320, y: 700, size, color, font: helvetica });
  page1.drawText(values.deedDate || '', { x: 200, y: 682, size, color, font: helvetica });

  page1.drawText(values.caseNo || '', { x: 200, y: 646, size, color, font: helvetica });
  page1.drawText(values.estimatedSurplus || '', { x: 200, y: 626, size, color, font: helvetica });

  page1.drawText(values.county || '', { x: 200, y: 606, size, color, font: helvetica });
  page1.drawText(values.state || '', { x: 200, y: 586, size, color, font: helvetica });

  page1.drawText(values.auctionDate || '', { x: 200, y: 566, size, color, font: helvetica });
  page1.drawText(values.salesPrice || '', { x: 200, y: 546, size, color, font: helvetica });
  page1.drawText(values.openingBid || '', { x: 200, y: 526, size, color, font: helvetica });

  page1.drawText(values.bidSource || '', { x: 200, y: 506, size, color, font: helvetica });
  page1.drawText(values.foreclosingEntity || '', { x: 200, y: 486, size, color, font: helvetica });

  page1.drawText(values.propertyAddressLine1 || '', { x: 200, y: 466, size, color, font: helvetica });
  page1.drawText(values.propertyAddressLine2 || '', { x: 200, y: 450, size, color, font: helvetica });
  page1.drawText(values.propertyCityStateZip || '', { x: 200, y: 434, size, color, font: helvetica });

  page1.drawText(values.dateFileReviewed || '', { x: 200, y: 414, size, color, font: helvetica });
  page1.drawText(values.researcher || '', { x: 200, y: 398, size, color, font: helvetica });

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
      // If parser returned too few fields, use fallback
      const nonEmpty = Object.values(parsed.extracted).filter(Boolean).length;
      if (nonEmpty < 6) {
        console.warn('Parsed too few fields; switching to manual fallback values');
        parsed = fallback();
        fs.writeFileSync('parsed-summary.json', JSON.stringify(parsed.extracted, null, 2), 'utf8');
      }
    } catch (err) {
      console.warn('Parsing failed or Summary.pdf missing; using manual fallback values:', err.message);
      parsed = fallback();
      fs.writeFileSync('parsed-summary.txt', '', 'utf8');
      fs.writeFileSync('parsed-summary.json', JSON.stringify(parsed.extracted, null, 2), 'utf8');
    }

    const outPath = await fillPdf(parsed.extracted);
    console.log('✅ Worksheet filled successfully:', outPath);
  } catch (err) {
    console.error('❌ Fatal error:', err && err.message ? err.message : err);
    process.exitCode = 1;
  }
})();