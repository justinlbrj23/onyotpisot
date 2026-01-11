// inspectWebpage.cjs
// Requires:
// npm install puppeteer fs googleapis

import puppeteer from 'puppeteer';
import fs from 'fs';
import { google } from 'googleapis';

// =========================
// CONFIG
// =========================
const SPREADSHEET_ID = 'YOUR_SPREADSHEET_ID';
const URL = 'https://king.wa.realforeclose.com/index.cfm?zaction=AUCTION&Zmethod=PREVIEW&AUCTIONDATE=09/10/2025';
const ELEMENT_SELECTOR = '.auction-item'; // <-- Update to match actual auction item class
const WAIT_MS = 5000; // Wait 5 seconds for dynamic content to load

// =========================
// Helper Functions
// =========================
async function scrapeAuctions() {
  const browser = await puppeteer.launch({ headless: true });
  const page = await browser.newPage();
  
  console.log('🌐 Visiting', URL);
  await page.goto(URL, { waitUntil: 'networkidle2' });

  // Wait for auction items to appear
  await page.waitForTimeout(WAIT_MS);

  // Grab all auction elements
  const elements = await page.$$eval(ELEMENT_SELECTOR, nodes =>
    nodes.map(node => node.innerText.trim())
  );

  console.log(`📦 Elements: ${elements.length}`);

  // Save raw elements
  fs.writeFileSync('raw-elements.json', JSON.stringify(elements, null, 2));
  
  // Parse auctions (example: filtering non-empty items)
  const auctions = elements.filter(el => el && el.length > 0);
  fs.writeFileSync('parsed-auctions.json', JSON.stringify(auctions, null, 2));

  console.log(`✅ Saved ${elements.length} elements → raw-elements.json`);
  console.log(`✅ Saved ${auctions.length} auctions → parsed-auctions.json`);

  await browser.close();
  console.log('🏁 Done');
}

// =========================
// Run
// =========================
scrapeAuctions().catch(err => console.error('❌ Error:', err));