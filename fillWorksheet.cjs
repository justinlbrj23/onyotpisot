// fillWorksheet.cjs
// Requires: npm install pdf-lib fs

const fs = require("fs");
const { PDFDocument, rgb } = require("pdf-lib");

async function fillWorksheet() {
  // Fallback values from Summary.pdf
  const summaryData = {
    county: "Miami-Dade",
    state: "Florida",
    caseNo: "2025A00491",
    parcelNo: "28-2202-026-2180",
    siteAddress: "19195 NE 36 CT UNIT 1508, Aventura, FL 33180-4502",
    deedType: "Warranty Deed",
    recordedDate: "2/01/2007",
    deedBookPage: "OR Book 25446 Page 0422",
    lastOwner: "SALOMON COHEN SALMUN and RUTH BLANCA SMEKE DE COHEN",
    openingBid: "$27,346.37",
    auctionDate: "November 13, 2025",
    soldAmount: "$338,200.00",
    estimatedSurplus: "$306,151.89",
    researcher: "Justine John J. Sale, Sr."
  };

  const templateBytes = fs.readFileSync("worksheet_template_tssf.pdf");
  const pdfDoc = await PDFDocument.load(templateBytes);
  const pages = pdfDoc.getPages();
  const firstPage = pages[0];

  // Example coordinates for Page 1 fields
  firstPage.drawText(`Deed Holders: ${summaryData.lastOwner}`, { x: 50, y: 700, size: 12, color: rgb(0,0,0) });
  firstPage.drawText(`Deed Recordation Date: ${summaryData.recordedDate}`, { x: 50, y: 680, size: 12 });
  firstPage.drawText(`Book/Page: ${summaryData.deedBookPage}`, { x: 50, y: 660, size: 12 });

  firstPage.drawText(`Case Number: ${summaryData.caseNo}`, { x: 50, y: 630, size: 12 });
  firstPage.drawText(`Estimated Surplus: ${summaryData.estimatedSurplus}`, { x: 50, y: 610, size: 12 });
  firstPage.drawText(`County: ${summaryData.county}`, { x: 50, y: 590, size: 12 });
  firstPage.drawText(`State: ${summaryData.state}`, { x: 50, y: 570, size: 12 });
  firstPage.drawText(`Auction Date: ${summaryData.auctionDate}`, { x: 50, y: 550, size: 12 });
  firstPage.drawText(`Sales Price: ${summaryData.soldAmount}`, { x: 50, y: 530, size: 12 });
  firstPage.drawText(`Opening Bid: ${summaryData.openingBid}`, { x: 50, y: 510, size: 12 });
  firstPage.drawText(`Foreclosing Entity: County Tax Office`, { x: 50, y: 490, size: 12 });

  firstPage.drawText(`Property Address: ${summaryData.siteAddress}`, { x: 50, y: 460, size: 12 });
  firstPage.drawText(`Researcher: ${summaryData.researcher}`, { x: 50, y: 440, size: 12 });

  const pdfBytes = await pdfDoc.save();
  fs.writeFileSync("filled_worksheet.pdf", pdfBytes);

  console.log("✅ Worksheet filled successfully: filled_worksheet.pdf");
}

fillWorksheet().catch(err => console.error(err));