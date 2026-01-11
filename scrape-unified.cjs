// scrape-unified.cjs
const fs = require('fs');
const puppeteer = require('puppeteer');

(async () => {
  // ----------------------------------------------------
  // CI‑SAFE PUPPETEER LAUNCH (required for GitHub Actions)
  // ----------------------------------------------------
  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage' // optional but improves CI stability
    ]
  });

  const page = await browser.newPage();

  const START_URL =
    'https://www.realforeclose.com/index.cfm?zaction=USER&zmethod=AUCTIONLIST&countycode=KING';

  await page.goto(START_URL);

  const allCards = [];

  // -----------------------------
  // 1. PAGINATION LOOP
  // -----------------------------
  for (let pageNum = 1; pageNum <= 50; pageNum++) {
    console.log(`🔎 Scraping page ${pageNum}`);

    await page.waitForSelector('.auctionCard', { timeout: 15000 });

    // -----------------------------
    // 2. PARSE MODERN SCHEMA
    // -----------------------------
    const cards = await page.evaluate(() => {
      const clean = (txt) =>
        txt?.replace(/\s+/g, ' ').trim() || '';

      const money = (txt) => {
        const n = parseFloat(txt?.replace(/[^0-9.]/g, ''));
        return isNaN(n) ? null : n;
      };

      return Array.from(document.querySelectorAll('.auctionCard')).map((card) => {
        const get = (sel) => clean(card.querySelector(sel)?.textContent || '');

        const status = get('.auctionStatus');
        const saleDate = get('.saleDate');
        const saleAmount = money(get('.saleAmount'));
        const buyerType = get('.buyerType');

        return {
          auctionStatus: status,
          auctionType: get('.auctionType'),
          caseNumber: get('.caseNumber'),
          openingBid: money(get('.openingBid')),
          parcelId: get('.parcelId'),
          propertyAddress: get('.propertyAddress'),
          assessedValue: money(get('.assessedValue')),
          saleResult:
            status === 'Auction Sold'
              ? {
                  date: saleDate,
                  amount: saleAmount,
                  buyerType: buyerType,
                }
              : null,
        };
      });
    });

    allCards.push(...cards);

    // -----------------------------
    // NEXT PAGE HANDLING
    // -----------------------------
    const nextBtn = await page.$('a.next');
    if (!nextBtn) {
      console.log('⛔ No more pages detected');
      break;
    }

    await Promise.all([page.waitForNavigation(), nextBtn.click()]);
  }

  // -----------------------------
  // 3. WRITE MODERN SCHEMA
  // -----------------------------
  fs.writeFileSync(
    'parsed-auctions.json',
    JSON.stringify(allCards, null, 2)
  );
  console.log(`📦 Saved modern schema: ${allCards.length} items`);

  // -----------------------------
  // 4. TRANSFORM → LEGACY SCHEMA
  // -----------------------------
  const legacy = allCards.map((card) => {
    return {
      id: card.caseNumber,
      apn: card.parcelId,
      saleDate: card.saleResult?.date || '',
      openingBid: card.openingBid || '',
      winningBid: card.saleResult?.amount || '',
      notes: [
        card.auctionStatus,
        card.saleResult?.buyerType,
        card.assessedValue
          ? `Assessed: $${card.assessedValue.toLocaleString()}`
          : null,
      ]
        .filter(Boolean)
        .join(' | '),
    };
  });

  fs.writeFileSync('raw-scrape.json', JSON.stringify(legacy, null, 2));
  console.log(`📦 Saved legacy schema: ${legacy.length} items`);

  await browser.close();
  console.log('🎉 Unified scrape + transform complete');
})();