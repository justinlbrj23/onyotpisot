// fillWorksheet.cjs
// Requires: npm install pdf-lib fs

const fs = require("fs");
const { PDFDocument, rgb } = require("pdf-lib");

async function fillWorksheet() {
  // Values based on your manual fill
  const summaryData = {
    deedHolders: "SALOMON COHEN SALMUN and RUTH BLANCA SMEKE DE COHEN",
    deedBook: "OR Book 25446",
    deedPage: "0422",
    deedDate: "2/01/2007",
    caseNo: "2025A00491",
    surplus: "$306,151.89",
    county: "Miami-Dade",
    state: "Florida",
    auctionDate: "November 13, 2025",
    salePrice: "$338,200.00",
    openingBid: "$27,346.37",
    bidSource: "Realforeclose.com",
    foreclosingEntity: "Miami-Dade County",
    propertyAddress: "19195 NE 36 CT UNIT 1508, Aventura, FL 33180-4502",
    fileReviewed: "2/13/2026",
    researcher: "Simon Roach"
  };

  const templateBytes = fs.readFileSync("worksheet_template_tssf.pdf");
  const pdfDoc = await PDFDocument.load(templateBytes);
  const pages = pdfDoc.getPages();
  const firstPage = pages[0];

  // Coordinates tuned for Page 1 blanks
  firstPage.drawText(summaryData.deedHolders, { x: 200, y: 720, size: 11, color: rgb(0,0,0) });
  firstPage.drawText(summaryData.deedBook, { x: 200, y: 700, size: 11 });
  firstPage.drawText(summaryData.deedDate, { x: 200, y: 680, size: 11 });
  firstPage.drawText(summaryData.deedPage, { x: 300, y: 680, size: 11 });

  firstPage.drawText(summaryData.caseNo, { x: 200, y: 640, size: 11 });
  firstPage.drawText(summaryData.surplus, { x: 200, y: 620, size: 11 });
  firstPage.drawText(summaryData.county, { x: 200, y: 600, size: 11 });
  firstPage.drawText(summaryData.state, { x: 200, y: 580, size: 11 });
  firstPage.drawText(summaryData.auctionDate, { x: 200, y: 560, size: 11 });
  firstPage.drawText(summaryData.salePrice, { x: 200, y: 540, size: 11 });
  firstPage.drawText(summaryData.openingBid, { x: 200, y: 520, size: 11 });
  firstPage.drawText(summaryData.bidSource, { x: 200, y: 500, size: 11 });
  firstPage.drawText(summaryData.foreclosingEntity, { x: 200, y: 480, size: 11 });

  firstPage.drawText(summaryData.propertyAddress, { x: 200, y: 460, size: 11 });
  firstPage.drawText(summaryData.fileReviewed, { x: 200, y: 440, size: 11 });
  firstPage.drawText(summaryData.researcher, { x: 200, y: 420, size: 11 });

  const pdfBytes = await pdfDoc.save();
  fs.writeFileSync("filled_worksheet.pdf", pdfBytes);

  console.log("✅ Worksheet filled successfully: filled_worksheet.pdf");
}

fillWorksheet().catch(err => console.error(err));