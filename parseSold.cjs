// lib/parseSold.cjs
// Pure parsing utilities for SOLD auction containers
// No puppeteer or cheerio dependencies

const MIN_SURPLUS = parseFloat(process.env.MIN_SURPLUS || '25000');

function normalizeText(s) {
  if (!s) return '';
  return String(s).replace(/\s+/g, ' ').replace(/\u00A0/g, ' ').trim();
}

function parseCurrency(str) {
  if (!str) return null;
  const s = String(str).trim();
  const million = /([\d,.]+)\s*[mM]\b/.exec(s);
  if (million) return parseFloat(million[1].replace(/,/g, '')) * 1e6;
  const n = parseFloat(s.replace(/[^0-9.-]/g, ''));
  return isNaN(n) ? null : n;
}

function extractBetween(text, startLabel, stopLabels = []) {
  const t = normalizeText(text);
  const idx = t.toLowerCase().indexOf(startLabel.toLowerCase());
  if (idx === -1) return '';
  let substr = t.slice(idx + startLabel.length).trim();
  let stopIndex = substr.length;
  for (const stop of stopLabels) {
    const i = substr.toLowerCase().indexOf(stop.toLowerCase());
    if (i !== -1 && i < stopIndex) stopIndex = i;
  }
  return substr.slice(0, stopIndex).trim();
}

function extractDateFlexible(text) {
  const t = normalizeText(text);
  const patterns = [
    /Date\s*:?\s*([0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4}(?:\s+[0-9]{1,2}:[0-9]{2}\s*(?:AM|PM)?)?)/i,
    /Auction Date\s*:?\s*([0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4}(?:\s+[0-9]{1,2}:[0-9]{2}\s*(?:AM|PM)?)?)/i,
    /Sale Date\s*:?\s*([0-9]{4}-[0-9]{2}-[0-9]{2}(?:\s+[0-9]{2}:[0-9]{2})?)/i,
  ];
  for (const re of patterns) {
    const m = t.match(re);
    if (m && m[1]) return m[1].trim();
  }
  return '';
}

function extractAmountAfter(text, label) {
  const t = normalizeText(text);
  const regex = new RegExp(label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\s*:?' + '\\s*\\$[\\d,]+(?:\\.\\d{2})?', 'i');
  const m = t.match(regex);
  if (!m) return '';
  const moneyMatch = m[0].match(/\$[\d,]+(?:\.\d{2})?/);
  return moneyMatch ? moneyMatch[0] : '';
}

function extractCurrencyNearLabels(text, labels, window = 80) {
  const t = normalizeText(text);
  const lower = t.toLowerCase();
  for (const label of labels) {
    const idx = lower.indexOf(label.toLowerCase());
    if (idx !== -1) {
      const slice = t.slice(Math.max(0, idx - 10), Math.min(t.length, idx + window));
      const m = slice.match(/\$[\d,]+(?:\.\d{2})?/);
      if (m) return m[0];
    }
  }
  return '';
}

function extractSalePrice(text) {
  const t = normalizeText(text);
  const labels = [
    'Amount','Sale Price','Sold Price','Sold Amount','Winning Bid','Final Bid','Sold For','Final Sale Price','Winning Amount','Winning Offer'
  ];
  for (const label of labels) {
    const v = extractAmountAfter(t, label);
    if (v) return v;
  }
  const near = extractCurrencyNearLabels(t, labels, 120);
  if (near) return near;
  const allMoney = [...t.matchAll(/\$[\d,]+(?:\.\d{2})?/g)].map(m => m[0]);
  if (allMoney.length) {
    const sorted = allMoney
      .map(s => ({ s, n: parseCurrency(s) }))
      .filter(x => x.n !== null)
      .sort((a, b) => b.n - a.n);
    return sorted.length ? sorted[0].s : '';
  }
  return '';
}

function buildLabelValueMapFromHtmlText(htmlLikeText) {
  const t = normalizeText(htmlLikeText);
  const parts = t.split(/[\r\n|]+/).map(p => p.trim()).filter(Boolean);
  const map = {};
  for (const p of parts) {
    const kv = p.split(/[:|-]\s*/);
    if (kv.length >= 2) {
      const label = kv[0].trim().toLowerCase();
      const value = kv.slice(1).join(':').trim();
      if (label && value) map[label] = value;
    }
  }
  return { map, text: parts.join(' | ') };
}

function validateRow(row) {
  return row && row.parcelId && row.openingBid && row.salePrice;
}

function parseSoldContainer(blockText = '') {
  const text = normalizeText(blockText);
  if (!text) return null;
  const looksSold = /sold|auction sold|sale finalized|finalized/i.test(text);
  if (!looksSold) return null;

  const kv = buildLabelValueMapFromHtmlText(text).map || {};

  const openingBid = extractAmountAfter(text, 'Opening Bid') || (kv['opening bid'] && kv['opening bid'].match(/\$[\d,]+(?:\.\d{2})?/)?.[0]) || '';
  const assessedValue = extractBetween(text, 'Assessed Value:', []) || kv['assessed value'] || '';
  const assessedMoney = (assessedValue.match(/\$[\d,]+(?:\.\d{2})?/) || [])[0] || '';
  const salePrice = extractSalePrice(text) || kv['sale price'] || kv['sold price'] || '';
  const apn = extractBetween(text, 'APN', ['Property Address', 'Assessed Value']) || extractBetween(text, 'Parcel ID', ['Property Address', 'Assessed Value']) || kv['parcel id'] || kv['apn'] || '';
  const parcelId = (apn || '').split(/\s|\|/)[0].trim();
  const auctionDate = extractDateFlexible(text) || kv['auction date'] || kv['date sold'] || '';

  const row = {
    auctionStatus: 'Sold',
    parcelId,
    openingBid,
    assessedValue: assessedMoney,
    salePrice,
    auctionDate,
    rawText: text
  };

  if (!validateRow(row)) return null;

  const openN = parseCurrency(row.openingBid);
  const assessN = parseCurrency(row.assessedValue);
  const saleN = parseCurrency(row.salePrice);

  row.surplusAssessVsSale = assessN !== null && saleN !== null ? assessN - saleN : null;
  row.surplusSaleVsOpen = saleN !== null && openN !== null ? saleN - openN : null;
  row.meetsMinimumSurplus = row.surplusAssessVsSale !== null && row.surplusAssessVsSale >= MIN_SURPLUS ? 'Yes' : 'No';

  return row;
}

module.exports = {
  parseSoldContainer,
  parseCurrency,
  extractSalePrice,
  extractDateFlexible,
  buildLabelValueMapFromHtmlText,
  validateRow
};
