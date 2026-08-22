const fs = require('fs');
const { chromium } = require('playwright');

const START_URL =
  process.env.START_URL ||
  'https://www.publicnoticeoregon.com/(S(b13eg0ew25gdpkwpknkphdvy))/Search.aspx';

const OUTPUT_JSON = 'notices.json';
const OUTPUT_TEXT = 'notices.txt';
const DEBUG_HTML = 'page.html';
const DEBUG_SCREENSHOT = 'page.png';
const FINAL_SCREENSHOT = 'final-page.png';

function cleanText(value = '') {
  return String(value)
    .replace(/\u00a0/g, ' ')
    .replace(/\r/g, '')
    .replace(/[ \t]+/g, ' ')
    .replace(/ *\n */g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function normalizeIdentifier(value = '') {
  return cleanText(value)
    .replace(/^[#:\-\s]+|[#:\-\s]+$/g, '')
    .toUpperCase();
}

function extractNoticeIdentifier(text = '') {
  const normalized = cleanText(text);
  const patterns = [
    /\bCase\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
    /\bT\.?S\.?\s*(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
    /\bTrustee(?:'s)?\s+Sale\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
    /\bSale\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
    /\bFile\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i,
    /\bLoan\s+(?:No\.?|Number)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]*)/i
  ];

  for (const pattern of patterns) {
    const match = normalized.match(pattern);
    if (match?.[1]) return normalizeIdentifier(match[1]);
  }

  return '';
}

function extractTrustor(text = '') {
  const normalized = cleanText(text);
  const patterns = [
    /\b(?:original\s+)?trustor(?:s)?\s*[:,]\s*(.+?)(?=\s*,?\s*(?:whose|who|as|beneficiary|trustee|recorded|the))/i,
    /\bTrustor(?:s)?\s*[:,]\s*([^.;]+)/i
  ];

  for (const pattern of patterns) {
    const match = normalized.match(pattern);
    if (match?.[1]) {
      return cleanText(match[1]).replace(/^[,;:\-\s]+|[,;:\-\s]+$/g, '');
    }
  }

  return '';
}

function fallbackIdentifier(notice) {
  const source = [
    notice.publication,
    notice.publishedDate,
    notice.text
  ].join('|');

  let hash = 2166136261;
  for (let index = 0; index < source.length; index += 1) {
    hash ^= source.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }

  return `NOTICE-${(hash >>> 0).toString(16).toUpperCase().padStart(8, '0')}`;
}

(async () => {
  let browser;

  try {
    browser = await chromium.launch({ headless: true });

    const context = await browser.newContext({
      viewport: { width: 1440, height: 1000 },
      userAgent:
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ' +
        '(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
    });

    const page = await context.newPage();
    page.setDefaultTimeout(120000);
    page.setDefaultNavigationTimeout(120000);

    console.log(`Opening: ${START_URL}`);

    await page.goto(START_URL, {
      waitUntil: 'domcontentloaded',
      timeout: 120000
    });

    await waitForResults(page);

    fs.writeFileSync(DEBUG_HTML, await page.content(), 'utf8');
    await page.screenshot({ path: DEBUG_SCREENSHOT, fullPage: true });

    const notices = [];
    const seen = new Set();
    let loopGuard = 0;

    while (true) {
      loopGuard += 1;
      if (loopGuard > 250) {
        throw new Error('Pagination safety limit reached (250 pages).');
      }

      await waitForResults(page);

      const pageInfo = await getPageInfo(page);
      const currentPage = pageInfo?.current || loopGuard;
      const totalPages = pageInfo?.total || currentPage;

      console.log(`Extracting page ${currentPage} of ${totalPages}...`);

      const pageNotices = await extractNotices(page);
      console.log(`Found ${pageNotices.length} notice(s) on this page.`);

      if (pageNotices.length === 0) {
        fs.writeFileSync(`failed-page-${currentPage}.html`, await page.content(), 'utf8');
        await page.screenshot({
          path: `failed-page-${currentPage}.png`,
          fullPage: true
        });
        throw new Error(`No sale notices were extracted from page ${currentPage}.`);
      }

      for (const notice of pageNotices) {
        const caseNumber =
          extractNoticeIdentifier(notice.text) || fallbackIdentifier(notice);
        const key = caseNumber || notice.text;

        if (seen.has(key)) continue;
        seen.add(key);

        notices.push({
          page: currentPage,
          publication: notice.publication,
          publishedDate: notice.publishedDate,
          caseNumber,
          noticeId: caseNumber,
          trustor: extractTrustor(notice.text),
          text: notice.text
        });
      }

      if (currentPage >= totalPages) break;

      const previousMarker =
        pageNotices[0]?.text.slice(0, 160) || `page-${currentPage}`;

      const moved = await goToNextPage(
        page,
        currentPage,
        currentPage + 1,
        previousMarker
      );

      if (!moved) {
        throw new Error(
          `Could not move from page ${currentPage} to page ${currentPage + 1}.`
        );
      }
    }

    writeOutput(notices);

    console.log(`Finished. Extracted ${notices.length} unique sale notice(s).`);
    console.log(`JSON output: ${OUTPUT_JSON}`);
    console.log(`Text output: ${OUTPUT_TEXT}`);

    await page.screenshot({ path: FINAL_SCREENSHOT, fullPage: true });
  } finally {
    if (browser) await browser.close();
  }
})().catch(error => {
  console.error('Scraper failed:', error);
  process.exitCode = 1;
});

async function waitForResults(page) {
  await page.waitForFunction(() => {
    const text = document.body?.innerText || '';
    return (
      /Page\s+\d+\s+of\s+\d+\s+Pages?/i.test(text) ||
      /(?:TRUSTEE(?:'S)?\s+)?NOTICE OF SALE/i.test(text) ||
      /click\s+['"]?view['"]?\s+to\s+open/i.test(text)
    );
  });

  await page.waitForTimeout(750);
}

async function getPageInfo(page) {
  const bodyText = await page.locator('body').innerText();
  const match = bodyText.match(/Page\s+(\d+)\s+of\s+(\d+)\s+Pages?/i);

  return match
    ? { current: Number(match[1]), total: Number(match[2]) }
    : null;
}

async function extractNotices(page) {
  const rawNotices = await page.evaluate(() => {
    const normalize = (value = '') =>
      String(value)
        .replace(/\u00a0/g, ' ')
        .replace(/\r/g, '')
        .replace(/[ \t]+/g, ' ')
        .replace(/ *\n */g, '\n')
        .replace(/\n{3,}/g, '\n\n')
        .trim();

    const salePattern = /(?:TRUSTEE(?:'S)?\s+)?NOTICE OF SALE/i;
    const summaryPattern = /click\s+['"]?view['"]?\s+to\s+open/i;
    const datePattern = /\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b/i;

    const looksLikeResult = text =>
      salePattern.test(text) &&
      (summaryPattern.test(text) || text.length >= 100);

    const countSaleNotices = text =>
      (text.match(/(?:TRUSTEE(?:'S)?\s+)?NOTICE OF SALE/gi) || []).length;

    const selectors = [
      '[id*="SearchResults"] article',
      '[id*="SearchResults"] li',
      '[id*="SearchResults"] tr',
      '[id*="SearchResults"] div',
      '[class*="search-result"]',
      '[class*="SearchResult"]',
      '[class*="result-item"]',
      'article',
      'li',
      'tr',
      'td',
      'section',
      'div'
    ].join(',');

    const candidates = Array.from(document.querySelectorAll(selectors))
      .filter(element => {
        const text = normalize(element.innerText);
        if (!looksLikeResult(text)) return false;

        const childHasResult = Array.from(
          element.querySelectorAll('article, li, tr, td, section, div')
        ).some(child => child !== element && looksLikeResult(normalize(child.innerText)));

        return !childHasResult;
      });

    return candidates.map(leaf => {
      const leafText = normalize(leaf.innerText);
      let recordElement = leaf;
      let parent = leaf.parentElement;

      while (parent && parent !== document.body) {
        const parentText = normalize(parent.innerText);
        const saleCount = countSaleNotices(parentText);

        if (saleCount > 1) break;
        if (!salePattern.test(parentText)) break;

        recordElement = parent;
        if (datePattern.test(parentText)) break;
        parent = parent.parentElement;
      }

      const recordText = normalize(recordElement.innerText);
      const dateMatch = recordText.match(datePattern);
      const publishedDate = dateMatch?.[0] || '';
      let publication = '';

      if (dateMatch) {
        const precedingLines = recordText
          .slice(0, dateMatch.index)
          .split('\n')
          .map(line => line.trim())
          .filter(Boolean)
          .filter(line => !/^(view|details?)$/i.test(line));

        publication = precedingLines.at(-1) || '';
      }

      let text = leafText;
      if (!salePattern.test(text)) text = recordText;

      return { publication, publishedDate, text };
    });
  });

  const unique = new Map();

  for (const notice of rawNotices) {
    const cleaned = {
      publication: cleanText(notice.publication),
      publishedDate: cleanText(notice.publishedDate),
      text: cleanText(notice.text)
    };

    if (!cleaned.text || !/(?:TRUSTEE(?:'S)?\s+)?NOTICE OF SALE/i.test(cleaned.text)) {
      continue;
    }

    const key = extractNoticeIdentifier(cleaned.text) || cleaned.text;
    if (!unique.has(key)) unique.set(key, cleaned);
  }

  return [...unique.values()];
}

async function goToNextPage(page, currentPage, nextPageNumber, previousMarker) {
  const selectors = [
    `a[href*="Page%24${nextPageNumber}"]`,
    `a[href*="Page$${nextPageNumber}"]`,
    `a[onclick*="Page$${nextPageNumber}"]`,
    `input[onclick*="Page$${nextPageNumber}"]`
  ].join(',');

  const aspNetPager = page.locator(selectors);
  if (await aspNetPager.count()) {
    return clickAndWaitForPageChange(
      page,
      aspNetPager.first(),
      currentPage,
      nextPageNumber,
      previousMarker
    );
  }

  const exactLinks = page.getByRole('link', {
    name: String(nextPageNumber),
    exact: true
  });

  for (let index = 0; index < await exactLinks.count(); index += 1) {
    const link = exactLinks.nth(index);
    const evidence = await link.evaluate(element => [
      element.getAttribute('href') || '',
      element.getAttribute('onclick') || '',
      element.getAttribute('title') || '',
      element.parentElement?.innerText || ''
    ].join(' '));

    if (/page|pager|next|__doPostBack/i.test(evidence)) {
      return clickAndWaitForPageChange(
        page,
        link,
        currentPage,
        nextPageNumber,
        previousMarker
      );
    }
  }

  const nextControl = page
    .getByRole('link', { name: /^(next|>|›|»|next page)$/i })
    .or(page.getByRole('button', { name: /^(next|>|›|»|next page)$/i }))
    .first();

  if (await nextControl.count()) {
    return clickAndWaitForPageChange(
      page,
      nextControl,
      currentPage,
      nextPageNumber,
      previousMarker
    );
  }

  return false;
}

async function clickAndWaitForPageChange(
  page,
  locator,
  currentPage,
  expectedPage,
  previousMarker
) {
  const navigationPromise = page
    .waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 30000 })
    .catch(() => null);

  await locator.scrollIntoViewIfNeeded();
  await locator.click();
  await navigationPromise;

  await page.waitForFunction(
    ({ expected, marker }) => {
      const text = document.body?.innerText || '';
      const pageChanged = new RegExp(
        `Page\\s+${expected}\\s+of\\s+\\d+\\s+Pages?`,
        'i'
      ).test(text);
      const resultChanged = Boolean(marker) && !text.includes(marker);
      return pageChanged || resultChanged;
    },
    { expected: expectedPage, marker: previousMarker },
    { timeout: 120000 }
  );

  await page.waitForTimeout(750);

  const info = await getPageInfo(page);
  return !info || info.current !== currentPage;
}

function writeOutput(notices) {
  fs.writeFileSync(OUTPUT_JSON, JSON.stringify(notices, null, 2), 'utf8');

  const textOutput = notices
    .map((notice, index) => [
      `NOTICE ${index + 1}`,
      `Page: ${notice.page}`,
      notice.publication ? `Publication: ${notice.publication}` : null,
      notice.publishedDate ? `Published: ${notice.publishedDate}` : null,
      `Notice ID: ${notice.noticeId}`,
      notice.trustor ? `Trustor: ${notice.trustor}` : null,
      notice.text
    ].filter(Boolean).join('\n'))
    .join('\n\n----------------------------------------\n\n');

  fs.writeFileSync(OUTPUT_TEXT, textOutput, 'utf8');
}
