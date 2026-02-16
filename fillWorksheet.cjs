// fillWorksheet.cjs
// Requires: npm install pdf-lib pdf-parse fs

const fs = require("fs");
const { PDFDocument } = require("pdf-lib");
const pdfParse = require("pdf-parse");

async function extractSummaryData() {
  const dataBuffer = fs.readFileSync("Summary.pdf");
  const parsed = await pdfParse(dataBuffer);
  const text = parsed.text;

  // Helper to extract values by label
  const getValue = (label) => {
    const regex = new RegExp(`${label}:\\s*(.+)`);
    const match = text.match(regex);
    return match ? match[1].trim() : "";
  };

  return {
    county: getValue("County"),
    state: getValue("State"),
    caseNo: getValue("Case No ."),
    parcelNo: getValue("Parcel No ."),
    siteAddress: getValue("Site Address"),
    deedType: getValue("Deed Type"),
    recordedDate: getValue("Recorded Date"),
    deedBookPage: getValue("OR Book 25446 Page No\\. 0422"), // adjust regex if needed
    lastOwner: getValue("Last Owner Name"),
    openingBid: getValue("Opening / Minimum Bid Amount"),
    auctionDate: getValue("Auction / Sale Date"),
    soldAmount: getValue("Sold Amount"),
    estimatedSurplus: getValue("Estimated Surplus"),
    researcher: getValue("Researcher Name")
  };
}

async function fillWorksheet() {
  const summaryData = await extractSummaryData();

  const templateBytes = fs.readFileSync("worksheet_template_tssf.pdf");
  const pdfDoc = await PDFDocument.load(templateBytes);

  // If the PDF has AcroForm fields
  const form = pdfDoc.getForm();

  // Fill fields (names must match actual AcroForm field names in template)
  form.getTextField("County").setText(summaryData.county);
  form.getTextField("State").setText(summaryData.state);
  form.getTextField("Case Number").setText(summaryData.caseNo);
  form.getTextField("Parcel Number").setText(summaryData.parcelNo);
  form.getTextField("Property Address").setText(summaryData.siteAddress);
  form.getTextField("Deed Type").setText(summaryData.deedType);
  form.getTextField("Recorded Date").setText(summaryData.recordedDate);
  form.getTextField("Book and Page").setText(summaryData.deedBookPage);
  form.getTextField("Deed Holders").setText(summaryData.lastOwner);
  form.getTextField("Opening Bid").setText(summaryData.openingBid);
  form.getTextField("Auction Date").setText(summaryData.auctionDate);
  form.getTextField("Sales Price").setText(summaryData.soldAmount);
  form.getTextField("Estimated Surplus").setText(summaryData.estimatedSurplus);
  form.getTextField("Researcher").setText(summaryData.researcher);

  const pdfBytes = await pdfDoc.save();
  fs.writeFileSync("filled_worksheet.pdf", pdfBytes);

  // Also save debug outputs
  fs.writeFileSync("parsed-summary.txt", parsed.text);
  fs.writeFileSync("parsed-summary.json", JSON.stringify(summaryData, null, 2));

  console.log("✅ Worksheet filled successfully: filled_worksheet.pdf");
}

fillWorksheet().catch(err => console.error(err));