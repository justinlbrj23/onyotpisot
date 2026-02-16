// fillWorksheet.cjs
// Node.js CommonJS script
// Install: npm install pdf-lib fs
//
// Purpose: Draw values (based on the user's manual fill) onto the static worksheet template.
// Input files (repo root): worksheet_template_tssf.pdf
// Output file: filled_worksheet.pdf

const fs = require('fs');
const { PDFDocument, rgb, StandardFonts } = require('pdf-lib');

async function fillWorksheet() {
  try {
    // Values taken from your manual fill (downloadfile.pdf / filled example)
    const values = {
      deedHoldersLine1: 'SALOMON COHEN SALMUN and RUTH',
      deedHoldersLine2: 'BLANCA SMEKE DE COHEN',
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
    };

    // Load template
    const templatePath = 'worksheet_template_tssf.pdf';
    if (!fs.existsSync(templatePath)) {
      throw new Error(`Template not found at ${templatePath}`);
    }
    const templateBytes = fs.readFileSync(templatePath);
    const pdfDoc = await PDFDocument.load(templateBytes);

    // Embed a standard font for consistent rendering
    const helvetica = await pdfDoc.embedFont(StandardFonts.Helvetica);

    // Page 1: draw values into blanks (coordinates tuned to the template)
    const pages = pdfDoc.getPages();
    const page1 = pages[0];

    // Styling defaults
    const color = rgb(0, 0, 0);
    const size = 11;

    // Deed holder lines (top-left area)
    page1.drawText(values.deedHoldersLine1, { x: 200, y: 720, size, color, font: helvetica });
    page1.drawText(values.deedHoldersLine2, { x: 200, y: 704, size, color, font: helvetica });

    // Book / Page / Recordation date
    page1.drawText(values.deedBook, { x: 200, y: 684, size, color, font: helvetica });
    page1.drawText(values.deedPage, { x: 320, y: 684, size, color, font: helvetica });
    page1.drawText(values.deedDate, { x: 200, y: 666, size, color, font: helvetica });

    // Case info and surplus
    page1.drawText(values.caseNo, { x: 200, y: 636, size, color, font: helvetica });
    page1.drawText(values.estimatedSurplus, { x: 200, y: 616, size, color, font: helvetica });

    // County / State
    page1.drawText(values.county, { x: 200, y: 596, size, color, font: helvetica });
    page1.drawText(values.state, { x: 200, y: 576, size, color, font: helvetica });

    // Auction / Sales / Opening bid
    page1.drawText(values.auctionDate, { x: 200, y: 556, size, color, font: helvetica });
    page1.drawText(values.salesPrice, { x: 200, y: 536, size, color, font: helvetica });
    page1.drawText(values.openingBid, { x: 200, y: 516, size, color, font: helvetica });

    // Bid source and foreclosing entity
    page1.drawText(values.bidSource, { x: 200, y: 496, size, color, font: helvetica });
    page1.drawText(values.foreclosingEntity, { x: 200, y: 476, size, color, font: helvetica });

    // Property address (stacked lines)
    page1.drawText(values.propertyAddressLine1, { x: 200, y: 456, size, color, font: helvetica });
    page1.drawText(values.propertyAddressLine2, { x: 200, y: 440, size, color, font: helvetica });
    page1.drawText(values.propertyCityStateZip, { x: 200, y: 424, size, color, font: helvetica });

    // Date reviewed and researcher
    page1.drawText(values.dateFileReviewed, { x: 200, y: 404, size, color, font: helvetica });
    page1.drawText(values.researcher, { x: 200, y: 388, size, color, font: helvetica });

    // If you want to add values to other pages, use pages[1], pages[2], etc.
    // Example placeholder for Page 2 (left blank unless you provide mortgage data)
    // const page2 = pages[1];
    // page2.drawText('Lender Name', { x: 120, y: 700, size, color, font: helvetica });

    // Save output
    const outPath = 'filled_worksheet.pdf';
    const pdfBytes = await pdfDoc.save();
    fs.writeFileSync(outPath, pdfBytes);

    console.log('✅ Worksheet filled successfully:', outPath);
  } catch (err) {
    console.error('❌ Error filling worksheet:', err && err.message ? err.message : err);
    process.exitCode = 1;
  }
}

fillWorksheet();