// bridge.js
// Converts parsed-auctions.json → raw-scrape.json (legacy schema)

const fs = require('fs');

const INPUT = 'parsed-auctions.json';
const OUTPUT = 'raw-scrape.json';

function safe(v) {
  return v === undefined || v === null ? '' : v;
}

function transformRow(row, index) {
  return {
    id: String(index + 1),                     // synthetic ID
    apn: safe(row.parcelId),                   // parcelId → apn
    saleDate: safe(row.auctionStatus),         // best available mapping
    openingBid: safe(row.openingBid),
    winningBid: '',                            // not available in new schema
    notes: [
      row.auctionType ? `Type: ${row.auctionType}` : '',
      row.caseNumber ? `Case: ${row.caseNumber}` : '',
      row.propertyAddress ? `Address: ${row.propertyAddress}` : '',
      row.assessedValue ? `Assessed: ${row.assessedValue}` : '',
      row.surplus !== null ? `Surplus: ${row.surplus}` : '',
      row.meetsMinimumSurplus ? `MeetsMin: ${row.meetsMinimumSurplus}` : ''
    ].filter(Boolean).join(' | ')
  };
}

function main() {
  if (!fs.existsSync(INPUT)) {
    console.error(`❌ Missing ${INPUT}`);
    process.exit(1);
  }

  const raw = JSON.parse(fs.readFileSync(INPUT, 'utf8'));

  if (!Array.isArray(raw)) {
    console.error(`❌ ${INPUT} must be an array`);
    process.exit(1);
  }

  const out = raw.map(transformRow);

  fs.writeFileSync(OUTPUT, JSON.stringify(out, null, 2));
  console.log(`✅ Wrote ${OUTPUT} (${out.length} rows)`);
}

main();