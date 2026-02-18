// map_sample.js
// Run: node map_sample.js parsed-auctions.json
const fs = require('fs');
const INPUT = process.argv[2] || 'parsed-auctions.json';
const MIN_SURPLUS = parseFloat(process.env.MIN_SURPLUS || '25000');
function parseCurrency(str) { if (!str) return null; const n = parseFloat(String(str).replace(/[^0-9.-]/g, '')); return isNaN(n) ? null : n; }
function yn(val) { if (val === true || val === 'Yes') return 'Yes'; if (val === false || val === 'No') return 'No'; if (typeof val === 'string') { const v = val.trim().toLowerCase(); if (v === 'yes') return 'Yes'; if (v === 'no') return 'No'; } return ''; }
function loadParsedInput(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8').trim();
  if (!raw) return [];
  if (raw[0] === '[') { try { return JSON.parse(raw); } catch (e) {} }
  const lines = raw.split(/\r?\n/).filter(Boolean);
  const out = [];
  for (const line of lines) { try { out.push(JSON.parse(line)); } catch (e) {} }
  return out;
}
function mapRow(raw) {
  if (!raw) return null;
  const mapped = {};
  mapped['Property Address'] = raw.propertyAddress || '';
  mapped['Parcel / APN Number'] = raw.parcelId || '';
  mapped['Case Number'] = raw.caseNumber || '';
  mapped['Auction Date'] = raw.auctionDate || raw.date || '';
  mapped['Sale Finalized (Yes/No)'] = raw.auctionStatus === 'Sold' ? 'Yes' : 'No';
  mapped['Sale Price'] = raw.salePrice || raw.amount || '';
  mapped['Opening / Minimum Bid'] = raw.openingBid || '';
  const sale = parseCurrency(mapped['Sale Price']);
  const open = parseCurrency(mapped['Opening / Minimum Bid']);
  const estimated = (sale !== null && open !== null) ? (open - sale) : null;
  mapped['Estimated Surplus'] = estimated !== null ? String(estimated) : '';
  mapped['Meets Minimum Surplus? (Yes/No)'] = estimated !== null && estimated >= MIN_SURPLUS ? 'Yes' : 'No';
  mapped['Deal Viable? (Yes/No)'] = mapped['Meets Minimum Surplus? (Yes/No)'] === 'Yes' ? 'Yes' : 'No';
  return mapped;
}
try {
  if (!fs.existsSync(INPUT)) { console.error('Input file not found:', INPUT); process.exit(1); }
  const parsed = loadParsedInput(INPUT);
  console.log(`Loaded ${parsed.length} parsed rows (showing up to 10 mapped rows):\n`);
  const sample = parsed.slice(0, 10).map(r => mapRow(r)).filter(Boolean);
  console.log(JSON.stringify(sample, null, 2));
} catch (err) { console.error('Error:', err && err.message ? err.message : err); process.exit(1); }