// fillWorksheet.cjs
// Node.js CommonJS script (keeps CommonJS, uses dynamic import for pdf-parse)
// Install: npm install pdf-lib pdf-parse fs
//
// Inputs (repo root): Summary.pdf (optional), worksheet_template_tssf.pdf (required)
// Outputs: filled_worksheet.pdf, parsed-summary.txt, parsed-summary.json

const fs = require('fs');
const { PDFDocument, rgb, StandardFonts } = require('pdf-lib');

async function loadPdfParse() {
  const mod = await import('pdf-parse');
  return mod.default || mod;
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
    deedDate: find(/DEED RECORDATION DATE.*?\n\s*([0-9\/\-]+)/i),
    caseNo: find(/Case No.*?\n\s*([A-Z0-9\-]+)/i),
    estimatedSurplus: find(/ESTIMATED SURPLUS AMOUNT.*?\n\s*\$?([0-9,\.]+)/i),
    county: find(/COUNTY.*?\n\s*([A-Za-z\- ]+)/i),
    state: find(/STATE.*?\n\s*([A-Za-z\- ]+)/i),
    auctionDate: find(/AUCTION DATE.*?\n\s*([A-Za-z0-9 ,]+)/i),
    salesPrice: find(/SALES PRICE AT AUCTION.*?\n\s*\$?([0-9,\.]+)/i),
    openingBid: find(/OPENING BID.*?\n\s*\$?([0-9,\.]+)/i),
    bidSource: find(/WHERE DID YOU GET YOUR OPENING BID LIST FROM.*?\n\s*([^\n]+)/i),
    foreclosingEntity: find(/FORECLOSING ENTITY.*?\n\s*([^\n]+)/i),
    propertyAddressLine1: find(/FORECLOSED PROPERTY ADDRESS.*?\n\s*([^\n]+)/i),
    propertyAddressLine2: find(/\n\s*(UNIT\s*[0-9A-Za-z\-]+)/i),
    propertyCityStateZip: find(/([A-Za-z ]+,\s*[A-Z]{2}\s*[0-9\-]+)/i),
    dateFileReviewed: find(/DATE FILE REVIEWED.*?\n\s*([0-9\/\-]+)/i),
    researcher: find(/RESEARCHER.*?\n\s*([^\n]+)/i)
  };
}

async function parseSummaryPdf() {
  const pdfParse = await loadPdfParse();
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

async function fillPdf(values) {
  const template = 'worksheet_template_tssf.pdf';
  const bytes = fs.readFileSync(template);
  const pdfDoc = await PDFDocument.load(bytes);
  const helvetica = await pdfDoc.embedFont(StandardFonts.Helvetica);
  const page1 = pdfDoc.getPages()[0];

  const color = rgb(0, 0, 0);
  const size = 11;

  // Coordinate map for Page 1 (tuned to blanks)
  page1.drawText(values.deedHolders || '', { x: 220, y: 740, size, color, font: helvetica });
  page1.drawText(values.deedDate || '', { x: 220, y: 720, size, color, font: helvetica });
  page1.drawText(values.deedBook || '', { x: 220, y: 700, size, color, font: helvetica });
  page1.drawText(values.deedPage || '', { x: 320, y: 700, size, color, font: helvetica });

  page1.drawText(values.caseNo || '', { x: 220, y: 670, size, color, font: helvetica });
  page1.drawText(values.estimatedSurplus || '', { x: 220, y: 650, size, color, font: helvetica });

  page1.drawText(values.county || '', { x: 220, y: 630, size, color, font: helvetica });
  page1.drawText(values.state || '', { x: 320, y: 630, size, color, font: helvetica });

  page1.drawText(values.auctionDate || '', { x: 220, y: 610, size, color, font: helvetica });
  page1.drawText(values.salesPrice || '', { x: 220, y: 590, size, color, font: helvetica });
  page1.drawText(values.openingBid || '', { x: 220, y: 570, size, color, font: helvetica });

  page1.drawText(values.bidSource || '', { x: 220, y: 550, size, color, font: helvetica });
  page1.drawText(values.foreclosingEntity || '', { x: 220, y: 530, size, color, font: helvetica });

  page1.drawText(values.propertyAddressLine1 || '', { x: 220, y: 510, size, color, font: helvetica });
  page1.drawText(values.propertyAddressLine2 || '', { x: 220, y: 495, size, color, font: helvetica });
  page1.drawText(values.propertyCityStateZip || '', { x: 220, y: 480, size, color, font: helvetica });

  page1.drawText(values.dateFileReviewed || '', { x: 220, y: 460, size, color, font: helvetica });
  page1.drawText(values.researcher || '', { x: 220, y: 440, size, color, font: helvetica });

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