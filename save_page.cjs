// save_page.js
// Saves page HTML for a given URL. Usage: node save_page.js "https://example.com"
const fs = require('fs');
const puppeteer = require('puppeteer');
(async () => {
  const url = process.argv[2] || 'https://dallas.texas.sheriffsaleauctions.com/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE=10/07/2025';
  const b = await puppeteer.launch({ headless: true, args: ['--no-sandbox'] });
  const p = await b.newPage();
  await p.goto(url, { waitUntil: 'networkidle2', timeout: 120000 });
  const html = await p.content();
  const fname = 'debug_page_snapshot.html';
  fs.writeFileSync(fname, html);
  await b.close();
  console.log('Saved', fname);
})();